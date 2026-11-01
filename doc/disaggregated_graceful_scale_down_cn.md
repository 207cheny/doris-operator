# Apache Doris Operator：存算分离 ComputeGroup 优雅缩容（3→2）源码分析

> **仓库**：<https://github.com/apache/doris-operator>  
> **基线**：tag `26.0.1` = commit `f705d058c128c052820e588d6971df251d7afc78`

---

## 0 前提

**范围**：只分析存算分离 `DorisDisaggregatedCluster` 下 ComputeGroup 的**优雅缩容 3→2**。decommission、升级、删除 ComputeGroup 均不在范围内。

| 维度   | 前提                                              |
| ---- | ----------------------------------------------- |
| 部署模式 | 存算分离，ComputeGroup `replicas` 3 → 2              |
| 分支前提 | `spec.enableDecommission = false`（默认入口，进入优雅状态机） |
| 镜像前提 | BE 镜像含哨兵检测入口脚本与 `stop_be.sh --grace`            |
| 验证方式 | 仓库自带 Go 测试设施 + fake client；无真实集群                |

镜像侧的两个契约文件不在 operator 仓库，而在 apache/doris 主仓：

| 文件                               | 作用                                             | 链接                                                                                                                                                                   |
| -------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `be_disaggregated_entrypoint.sh` | 启动前检查终止哨兵，命中则直接退出，不启动 BE                       | [docker/runtime/be/resource/be\_disaggregated\_entrypoint.sh](https://github.com/apache/doris/blob/master/docker/runtime/be/resource/be_disaggregated_entrypoint.sh) |
| `stop_be.sh`                     | 停 BE 进程；`--grace` 以 SIGTERM 替代 SIGKILL 并等待进程退出 | [bin/stop\_be.sh](https://github.com/apache/doris/blob/master/bin/stop_be.sh)                                                                                        |

---

## 1 业务诉求

客户把 `spec.computeGroups[].replicas` 从 3 改成 2，语义是**把 3 个 BE 平稳变成 2 个**。该操作需要同时满足四项约束：

1. **流量不中断**——被移除的 BE 上仍有进行中的查询与写入，必须先行排空；
2. **成员关系不残留**——FE 成员表中不能留下已删除的 BE；
3. **Pod 不被重建**——副本数若不同步调整，StatefulSet 会立即重建被删除的 Pod；
4. **过程可中断、可恢复**——Operator 进程可能随时被终止，重启后必须能继续推进。

因此缩容的实质是一个**有序、可中断、可恢复的编排问题**，而非一次删除操作。

---

## 2 三个疑问（第5节做专属论证）

**Q1　缩容核心主流程是什么？**

> A：一次只处理一个 BE，按"优雅停止并排空 → 从 FE 摘除 → 删除 Pod → 收缩副本数"四步串行执行，顺序不可调换。

**Q2　缩容期间出现异常，靠什么接着往下走？**

> A：每一步都设计为可重复执行，并配以超时上限与外部进度记录；Operator 重启后从记录点继续，而非从头重来。

**Q3　缩容期间改主意（replicas 改回 3）能不能撤回？**

> A：撤回仅在第一个 Pod 被处理之前有效，此后不可逆。

---

## 3 代码链路

### 3.1 入口：链路的注册与装配

`main` 维护一份 `controller.Controllers` 列表逐个 `Init`。存算分离**不是默认开启**的：只有环境变量 `START_DISAGGREGATED_OPERATOR=="true"` 时才追加 `DisaggregatedClusterReconciler`（[main.go:101-104](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/cmd/operator/main.go#L101-L104)）。

该 Reconciler 的 `Init` 注册三个子控制器——metaservice、disaggregated_fe、computegroups（[disaggregated\_cluster\_controller.go:68-77](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L68-L77)），三者共用同一接口，顶层只依赖接口（[disaggregated\_subcontroller.go:67-79](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_subcontroller.go#L67-L79)）：

```go
type DisaggregatedSubController interface {
    Sync(ctx context.Context, obj client.Object) error
    ClearResources(ctx context.Context, obj client.Object) (bool, error)
    GetControllerName() string
    UpdateComponentStatus(obj client.Object) error
}
```

### 3.2 顶层 Reconcile 四层

每轮 `Reconcile`（[disaggregated\_cluster\_controller.go:196](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L196)）固定经过四个调用：

| 层    | 调用点                                                                                                                                                                           | 作用                                                        |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| ① 同步 | `reconcileSub` [:288](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L288)         | 遍历子控制器执行 `Sync`，推进各组件状态；缩容状态机在此层推进                        |
| ② 清理 | `clearUnusedResources` [:254](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L254) | 回收 spec 不再引用的资源：Doris 元数据、无主 Service / StatefulSet、高位 PVC |
| ③ 汇总 | `reorganizeStatus` [:262](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L262)     | 汇总各组件状态、计算集群健康状态；未收敛则返回 `RequeueAfter 10s`                |
| ④ 写回 | `updateObjectORStatus` [:309](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L309) | 将计算结果写回 CR status                                         |

重新入队依赖第 ③ 层的返回值：`reorganizeStatus` 遍历 CG 状态，`shouldRequeueComputeGroupPhase` 判定未收敛即返回 `RequeueAfter 10s`（[:333-346](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L333-L346)），`Reconcile` 原样返回该 Result（[:244-251](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/disaggregated_cluster_controller.go#L244-L251)）。状态机每轮只推进一个状态即返回，因此一次缩容由多轮 Reconcile 完成，而不是单次长调用。

### 3.3 核心调用链

```
Reconcile                                        disaggregated_cluster_controller.go:196
│
├─ ① reconcileSub(:288) → sub.Sync(:292)
│   └─ computeGroupSync                          computegroups/controller.go:148
│       └─ reconcileStatefulset                  computegroups/controller.go:219
│           ├─[分支①] !enableDecommission        controller.go:256-259
│           └─ reconcileGracefulStatefulSet      controller.go:308
│               └─ gracefulRolloutReconcile      graceful_rollout.go:86
│                   ├─ detectGracefulAction      :222   判定动作类型：缩容 > 滚动更新 > 恢复
│                   ├─ runGracefulStateMachine   :264   按状态分派 :273-283
│                   └─ prepare + setGracefulAction → return true
│                                                        （apply 仍在 :281 执行）
│
├─ ② clearUnusedResources(:254) → sub.ClearResources(computegroups/controller.go:467)
│   ├─ clearCGInDorisMeta(:504)                  清已删 ComputeGroup 的 Doris 元数据
│   ├─ clearSvcs(:507) / clearStatefulsets(:510) 清无主 Service / StatefulSet
│   └─ ClearStatefulsetUnusedPVCs(:516, :527)
│       └─ 删除 ordinal ≥ STS.spec.replicas 的 PVC
│
├─ ③ reorganizeStatus(:262)                      汇总状态；未收敛则返回 RequeueAfter 10s
├─ ④ updateObjectORStatus(:309)                  写回 CR status
│
└─ 返回 ③ 的 Result → 10s 后回到 ①，推进状态机的下一个状态
```

第 ①、② 层与缩容直接相关：第 ① 层推进状态机，第 ② 层回收已释放的资源；第 ③ 层负责把下一轮排上。

---

## 4 核心状态机

### 4.1 载体：进度为何存放在 STS 注解

一次缩容的全部进度封装为 `GracefulAction`（[types.go:441](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/api/disaggregated/v1/types.go#L441)），经 JSON 序列化写入 STS 注解 `doris.disaggregated.cluster/graceful-action`（[graceful\_rollout.go:1172](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L1172)）。

选择注解而非 CR status，源码注释给出的理由明确：

> *"CR status alone is not safe here because old CRDs prune unknown status fields."*（[graceful\_rollout.go:59-61](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L59-L61)）

即：集群若仍部署旧版 CRD，新增的 status 字段会被裁剪，进度随之丢失。注解为键值对，不受 CRD schema 约束。

字段按用途分四组：

| 组     | 字段                                                                                         | 用途            |
| ----- | ------------------------------------------------------------------------------------------ | ------------- |
| 目标    | `Type`、`DesiredReplicas`、`TargetRevision`                                                  | 本次动作的类型与目标值   |
| 进度    | `Phase`、`CurrentPod`、`CurrentOrdinal`                                                      | 当前状态与处理对象     |
| 基线    | `InitialPodUID`、`InitialContainerID`、`InitialRestartCount`、`InitialBackendStartTime/Epoch` | 停机前的原始值，供事后比对 |
| 超时与诊断 | `DeadlineAt`、`DrainTriggered`、`SentinelWritten`、`LastMessage`                              | 超时判据与可读状态说明   |

### 4.2 状态与出口

**ScaleDown 只经过三个状态**：

```
TriggerDrain ──► WaitDrain ──► DeletePod ──┬──► TriggerDrain   （仍有待处理 Pod）
                                           └──► Done            （desired ≥ current）
```

**注意：**`WaitPodReady` 与 `WaitBEAlive` **只服务滚动更新**（等待替换 Pod 就绪、等待 FE 观察到新 BE 世代），缩容不经过这两个状态。

| 状态             | 进入时执行                                                                                   | 出口条件                                                              | 出口去向                                                                         |
| -------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `TriggerDrain` | 选 Pod；记录 UID/容器/restartCount/backend 世代；计算 `DeadlineAt`；写终止哨兵；exec `stop_be.sh --grace` | ① Pod 不存在<br />② BE 已不在成员表<br />③ 正常执行完毕（**exec 失败也计入**）          | ①→`advanceToNextPod`<br />②→`DeletePod`（跳过排空，直接进入删除pod阶段）<br />③→`WaitDrain` |
| `WaitDrain`    | 轮询容器状态                                                                                  | ① 容器 `Terminated`<br />② `restartCount` 增加<br />③ `DeadlineAt` 超时 | 三者均→`DeletePod`                                                              |
| `DeletePod`    | DROP BE → 删 Pod → `replicas = ordinal`                                                  | 执行完毕                                                              | `desired < current`→`TriggerDrain`<br />否则→`Done`                            |
| `Done`         | `shouldFinalizeGracefulAction` 复核                                                       | 无遗留 → `finalize`                                                  | 清理注解 → 常规 apply → `Ready`                                                    |


出口②是一条短路：

> 当 BE 已不在成员表时（`errBackendNotFound`），跳过停机直接进入 `DeletePod`（[graceful\_rollout.go:359-363](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L359-L363)）。这条短路给出了一个语义层面的结论：停机之所以是必要步骤，前提是该 BE 仍在承接流量；成员关系一旦解除，停机便不再是前置条件。

`WaitDrain` 的三个出口覆盖容器生命周期的三种终态：

> 1. 容器已自己退出；
> 2. kubelet 已重启容器（说明上一代 BE 进程已结束）；
> 3. 等待超过 `DeadlineAt`。三者均收敛至 `DeletePod`，因此该状态具有确定的时间上界，不存在无限等待的路径。

### 4.3 五条不变量

| # | 不变量                     | 保证机制                             |
| - | ----------------------- | -------------------------------- |
| 1 | 任何时刻至多一个 Pod 处于"已停机未删除" | 一次只选一个 Pod，且取最高 ordinal          |
| 2 | 停机一定早于摘除                | 状态顺序；出口②的短路不违反该序                 |
| 3 | 摘除一定早于删 Pod             | `DeletePod` 内部先 DROP 后 delete    |
| 4 | 副本数不会提前收缩               | `st.replicas` 被改回 `est.replicas` |
| 5 | 副本数每次只减一                | `newReplicas = CurrentOrdinal`   |

第 4 条的依据在于每轮末尾的 apply 无条件执行：`ApplyStatefulSet` 位于 `if !gracefulInProgress` 块之外（[controller.go:281](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/controller.go#L281)）。若不把 `st.replicas` 改回当前值，第一轮 apply 就会把 3 改为 2，k8s 随即删掉一个 Pod，状态机被绕过。同一处还把更新策略置为 `OnDelete`（[graceful\_rollout.go:1136-1141](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L1136-L1141)）。由此产生一个易被误判的中间态：**CR 上 `replicas=2`，STS 上仍为 3**。

第 5 条由 `afterPodDeleted` 直接决定：删除 ordinal=2 后副本数为 2，因此 5→3 必然分两轮（5→4→3），不会一步到位。

### 4.4 可观察性

| 观察点                                | 缩容中                         | 缩容完成                    |
| ---------------------------------- | --------------------------- | ----------------------- |
| CR `spec.computeGroups[].replicas` | 2（用户目标）                     | 2                       |
| STS `spec.replicas`                | 3 → 2（逐个下降）                 | 2                       |
| STS 注解 `graceful-action`           | 存在，含 `Phase` / `CurrentPod` | **已清除**                 |
| `ComputeGroupStatus.Phase`         | `GracefulScaling`           | `Reconciling` → `Ready` |
| STS `spec.updateStrategy`          | `OnDelete`                  | `OnDelete`（保持）          |

排障入口：注解中的 `phase` 与 `currentPod` 指明当前处理对象，`lastMessage` 给出等待原因与 deadline，通常比回溯事件流更直接。

---

## 5 三个问题的论证

### 5.1 论点一（答 Q1）：核心主流程为"停 → 摘 → 删 → 缩"四步串行，一次一个 BE

**回答 Q1**。每步都有硬性条件：

> 停机看容器退出或 `restartCount` 上涨，摘除看 `SHOW BACKENDS` 里还有没有该 BE，删 Pod 看 `IsNotFound`，收缩副本看 `replicas` 有没有写成功。判据不满足即停在原状态，不会跳步。

```
停  写哨兵 → stop_be.sh --grace → 等容器退出（WaitDrain）
摘  DROP BACKEND，FE 元数据移除
删  删除 Pod，以 IsNotFound 为界
缩  replicas = ordinal，STS 收缩一格
```

**为什么"停"要分两段：**

缩容期 Pod 不会重建（副本数锁定 + `OnDelete`），但容器会被 kubelet 重启——仓库未设 `restartPolicy`，默认 `Always`。真正的风险是：BE 已退出，容器被重新拉起，又启动一个新的 BE，排空结果作废。因此停机不是一次 `kill`，而是三个动作：

| 顺序 | 动作                                        | 作用                                                                                       | 位置                                                                                                                                                                                                                                                                               |
| -- | ----------------------------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ①  | 写哨兵 `/var/run/doris-operator/terminating` | 落在 emptyDir 上，容器重启后依然存在                                                                  | [:383-394](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L383-L394)                                                                               |
| ②  | `stop_be.sh --grace`                      | 以 SIGTERM 替代 SIGKILL 停止 `doris_be`，BE 进程通过心跳告诉 FE 自己 `isShutdown`；<br />脚本阻塞至进程消失（有超时时间） | [:398-409](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L398-L409)、[bin/stop\_be.sh](https://github.com/apache/doris/blob/master/bin/stop_be.sh) |
| ③  | 置 `DrainTriggered`                        | 停机只触发一次，后续轮次不重复 `exec`                                                                   | [:410](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L410)                                                                                        |

设置哨兵必须早于容器排空：若顺序颠倒，BE 退出到容器重启之间没有哨兵，entrypoint 会正常拉起第二个 BE。哨兵文件会让重启后的容器在入口处退出——`exit_if_terminating_sentinel_exists` 判断 `-f $TERMINATING_SENTINEL_PATH`（默认 `/var/run/doris-operator/terminating`）成立即 `exit 0`，且该检查在参数校验之前（[be\_disaggregated\_entrypoint.sh](https://github.com/apache/doris/blob/master/docker/runtime/be/resource/be_disaggregated_entrypoint.sh)）。emptyDir 的卷作用域是 Pod 而非容器（[statefulset.go:106-111](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/statefulset.go#L106-L111)、[:177-180](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/statefulset.go#L177-L180)），重启后哨兵仍在。

**于是重启本身成了信号**：`WaitDrain` 期间会拿 `restartCount` 与`InitialRestartCount`比对，上涨即判定 BE 已退出（[graceful\_rollout.go:453-464](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L453-L464)），转入 `DeletePod`。

**摘除与收缩：**

`DeletePod` 开头调用 `dropBackendByPodName`，SQL 为 `ALTER SYSTEM DROPP BACKEND`（[mysql.go:184-191](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/common/utils/mysql/mysql.go#L184-L191)）；Pod 删除后才在 `afterPodDeleted` 写回 `replicas = ordinal`（[graceful\_rollout.go:541-542](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L541-L542)）。顺序不可换：先降副本，StatefulSet 会立即重建被删 Pod，缩容不收敛；先摘后停，BE 已不在成员表但进程仍在跑，存量请求失去归属。

**流量为什么不断：**

两层：BE 侧 `--grace` 以 SIGTERM 通知优雅退出，在途请求结束后进程消失；FE 侧 `DROP` 将其从元数据移除。两者都在删 Pod 之前完成，且哨兵保证窗口内不会有第二个 BE 顶替。

**`exec` 超时与排空超时是两个量：**

`exec` 本身限时 30s（`execTimeout`，注释标注 *not the drain*），而 `stop_be.sh` 会阻塞到进程消失。排空超过 30s 时 `exec` 先失败，这正是"exec 失败仍进 `WaitDrain`"的由来：真正的等待判据是 `DeadlineAt`（默认 200s），不是 `exec` 的返回值。

**验证**：`DetectAction`、`SelectNextPod`、`PreparePinsReplicas`、`ReplicasConvergeByOrdinal`（§6）；emptyDir 卷与挂载由仓库自带 `statefulset_test.go:125-146` 断言。

**边界**：

- `DROP` 不幂等。若 FE 元数据未同步、`SHOW BACKENDS` 仍能看到该 BE，将在下一轮重复尝试——需真实集群验证。
- 哨兵检测脚本在 apache/doris 仓库（[be\_disaggregated\_entrypoint.sh](https://github.com/apache/doris/blob/master/docker/runtime/be/resource/be_disaggregated_entrypoint.sh)），不在 operator 仓库。启动前的能力探测会 grep 镜像里是否含 `TERMINATING_SENTINEL_PATH` 与 `exit_if_terminating_sentinel_exists` 两个字符串（[:777-800](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L777-L800)），缺失即发 Warning 并降级，这一层不静默。静默的是路径：operator 固定写 `/var/run/doris-operator/terminating`，既不给容器注入该环境变量，探测也不比对两侧默认值——镜像若改了默认路径，探测照过，哨兵却落在镜像不读的位置。

---

### 5.2 论点二（答 Q2）：每一步都可重入，异常与重启不中断缩容

**回答 Q2**。状态机不实现重试逻辑，而是将每一步设计为可重复执行，由下一轮 Reconcile 自然重入。

**依据（逐层）**：

| 风险             | 处理机制                                                                   | 位置                                                                                                                                                                                                                                                                                                                                                                                                               |
| -------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DROP` 成功但返回错误 | 执行后 `SHOW BACKENDS` 复核，BE 已不在即视为成功                                     | [prepare\_modify.go:145-168](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/prepare_modify.go#L145-L168)                                                                                                                                                                                               |
| `exec` 停机失败    | 无论成败均进入 `WaitDrain`（注释原文：*"Even if exec fails, proceed to WaitDrain"*） | [graceful\_rollout.go:403-409](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L403-L409)                                                                                                                                                                                           |
| 容器未退出          | `DeadlineAt` 超时强制推进（默认 200s）                                           | [graceful\_rollout.go:372-378](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L372-L378)、[:467](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L467) |
| 删 Pod 回执丢失     | `getPod` / `Delete` 遇 `IsNotFound` 视为已删除                               | [graceful\_rollout.go:501-515](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L501-L515)                                                                                                                                                                                           |
| 副本更新重试         | 先 `Get` 比较再 `Update`，相等即 no-op                                         | [graceful\_rollout.go:562-565](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L562-L565)                                                                                                                                                                                           |
| 进程被终止          | 进度在 STS 注解，重启后由 `getGracefulAction` 反序列化并继续执行                          | [graceful\_rollout.go:1157](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L1157)                                                                                                                                                                                                  |


**验证**：`WaitDrainThreeExits`、`ReplicasPatchIdempotent`、`ActionAnnotationRoundTrip`、`PriorityAndStarted`。

**边界**：FE DROP 的异步性未在单测覆盖（fake client 无法模拟 FE 元数据延迟）。

---

### 5.3 论点三（答 Q3）：缩容期间改主意，能不能撤回？

**回答 Q3**。能，但只在当前这个 Pod 被选中之前；一旦选中，该 Pod 必然走完停 / 摘 / 删，此后只能止损，无法复活。判据是"当前这个 Pod 有没有被选中"，不是"有没有删过 Pod"（[graceful\_rollout.go:115](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L115)）：

```go
if requestedReplicas >= currentReplicas && !gracefulScaleDownPodStarted(storedAction) {
```

| 档位     | 条件                                                   | 结果                                |
| ------ | ---------------------------------------------------- | --------------------------------- |
| A 无损撤销 | 尚未选 Pod（`CurrentPod`、`DrainTriggered`、`Phase` 三者都未动） | 清注解、恢复常规 apply，没有任何 Pod 被触碰       |
| B 止损撤销 | 已选 Pod 或已开始排空                                        | 在途 Pod 照常走完停 / 摘 / 删，随后按新目标把副本数补回 |

两档的差别只是"有没有一个 Pod 被重启"，不是数据丢失：B 档被删的 Pod 会以同名重建，PVC 不变，BE 重新注册。

`gracefulScaleDownPodStarted` 是三个条件取或（[:851-856](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L851-L856)）：

```go
return ga.CurrentPod != "" || ga.DrainTriggered || ga.Phase != dv1.GracefulPhaseTriggerDrain
```

因此 A 档窗口极短：动作创建后要等下一轮 Reconcile 才选 Pod，无损撤销通常只有一轮（约 10s）的机会；`CurrentPod` 一旦落盘，之后改回 replicas 只能走 B 档。窗口按 Pod 计而非按动作计——`advanceToNextPod` 会把 `CurrentPod`、`DrainTriggered` 清零（[:979-998](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L979-L998)），所以每删完一个 Pod，下一个被选中之前窗口会重开一轮；但对已被选中的那个 Pod 本身，窗口已经关闭。

三个现状要点：

1. **只有 ScaleDown 有 cancel 分支**。`RollingUpdate`、`Delete` 没有对应逻辑，改 replicas 不会中止它们。
2. **撤销是隐式的**。没有 cancel 接口，也没有注解开关，只能把 `replicas` 改回不小于当前值。动作的目标值会在同一处被改写（[:111-114](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L111-L114)），这正是 B 档只丢一个 Pod 的原因：目标一改，`desired < current` 立刻不成立，循环在删完在途 Pod 后即收口。
3. **撤销留下两个痕迹**。`finalizeGracefulAction` 仍会调 `ensureOnDeleteStrategy`，STS 的更新策略停在 `OnDelete`；cancel 分支只写 `klog`、不发 Event（[:116-122](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L116-L122)），`kubectl describe` 里看不到"缩容已撤销"。

**两处可改进**

1. **撤销时补一条 Event**。cancel 分支加 `EventNormal`（如 `GracefulScaleDownWithdrawn`），写明"在途 Pod X 已排空，正按新目标补回"。零逻辑风险，补上可观测性缺口。
2. **把边界从"选中 Pod"后移到"DROP 之前"**。`DROP BACKEND` 发生在 `handleDeletePod`（[:493-497](https://github.com/apache/doris-operator/blob/f705d058c128c052820e588d6971df251d7afc78/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_rollout.go#L493-L497)），早于 `DELETE Pod`，但晚于整个 `WaitDrain`——排空完成到真正摘除之间仍有一段窗口，此时 BE 只是停机，成员关系还在。若允许在这段窗口内撤销，删掉哨兵文件就能让容器下次重启正常拉起 BE（BE 无状态，数据在共享存储），在途 Pod 原地复活。

> 落地有两个前提：1. 需要新增"已摘除"标记（如 `GracefulAction.BackendDropped`），因为 DROP 与删 Pod 同处 `DeletePod` 状态，仅凭 `Phase` 无法区分，删 Pod 失败重试时尤甚；2. 复活要么 `exec rm` 哨兵后等 kubelet 退避重试（退避上限 5 分钟），要么直接删 Pod 让 STS 按原副本数重建（更快，语义等同一次重启）。

**验证**：`CancelOnRevert`、`WithdrawBoundary`。

**边界**：上述两处改进为分析建议，代码中未实现。

---

## 6 单元测试

**文件**：`pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_scaledown_verify_test.go`（新增）

### 6.1 用例清单

| #  | 用例                          | 验证点                                                       |
| -- | --------------------------- | --------------------------------------------------------- |
| 1  | `DetectAction`              | `2<3` → `ScaleDown`，`DesiredReplicas=2`，状态 `TriggerDrain` |
| 2  | `SelectNextPod`             | 先选最高 ordinal `sts-2`；`2≥2` 时判未找到（收敛）                      |
| 3  | `PreparePinsReplicas`       | **缩容期 `st.replicas` 被改回 3、策略改 OnDelete；滚动更新不受此约束**        |
| 4  | `ReplicasConvergeByOrdinal` | 5→3 需两轮：删 ordinal 4 → 4；删 ordinal 3 → 3；随后 `Done`         |
| 5  | `WaitDrainThreeExits`       | 容器终止 / restartCount 增加 / 超时，三条出口均进 `DeletePod`            |
| 6  | `ReplicasPatchIdempotent`   | `updateStatefulSetReplicas` 3→2 落库；重复调用 no-op             |
| 7  | `ActionAnnotationRoundTrip` | `graceful-action` 注解 JSON 往返不丢字段                          |
| 8  | `PriorityAndStarted`        | ScaleDown 优先级高于 RollingUpdate；未开始/已开始的判定语义                |
| 9  | `CancelOnRevert`            | replicas 回退 3 → 取消、清注解、`Phase=Reconciling`、恢复 apply       |
| 10 | `PVCCleanup`                | `replicas=2` 时删 `doris-cg1-2`，保留 0/1                      |
| 11 | `PVCCleanupSkipsDeletedCG`  | spec 中已无该 CG 时 PVC 不被清理（已知缺口）                             |
| 12 | `WithdrawBoundary`          | 撤销边界：选 Pod / 已排空 / 阶段推进后即不可无损撤销；非 ScaleDown 不适用           |

### 6.2 运行方式

```bash
# 测试文件位于 pkg/controller/sub_controller/disaggregated_cluster/computegroups/
go test -count=1 -v -run 'TestVerifyScaleDown' \
  ./pkg/controller/sub_controller/disaggregated_cluster/computegroups/
```

**判定**：12 个用例、21 条结果（9 条来自子用例）全部 `PASS`，无 `SKIP`。新增文件不影响仓库原有测试，整包回归同样通过。

### 6.3 真实输出（2026-11-01）

```
--- PASS: TestVerifyScaleDownDetectAction (0.00s)
--- PASS: TestVerifyScaleDownSelectNextPod (0.00s)
--- PASS: TestVerifyScaleDownActionAnnotationRoundTrip (0.00s)
--- PASS: TestVerifyScaleDownReplicasPatchIdempotent (0.00s)
--- PASS: TestVerifyScaleDownPriorityAndStarted (0.00s)
--- PASS: TestVerifyScaleDownCancelOnRevert (0.00s)
--- PASS: TestVerifyScaleDownWithdrawBoundary (0.00s)
    --- PASS: TestVerifyScaleDownWithdrawBoundary/no_action (0.00s)
    --- PASS: TestVerifyScaleDownWithdrawBoundary/fresh_action,_no_pod_selected (0.00s)
    --- PASS: TestVerifyScaleDownWithdrawBoundary/pod_selected (0.00s)
    --- PASS: TestVerifyScaleDownWithdrawBoundary/drain_triggered (0.00s)
    --- PASS: TestVerifyScaleDownWithdrawBoundary/phase_advanced (0.00s)
    --- PASS: TestVerifyScaleDownWithdrawBoundary/non_scale-down_action (0.00s)
--- PASS: TestVerifyScaleDownPVCCleanup (0.00s)
--- PASS: TestVerifyScaleDownPVCCleanupSkipsDeletedCG (0.00s)
--- PASS: TestVerifyScaleDownPreparePinsReplicas (0.00s)
--- PASS: TestVerifyScaleDownWaitDrainThreeExits (0.00s)
    --- PASS: TestVerifyScaleDownWaitDrainThreeExits/container_terminated (0.00s)
    --- PASS: TestVerifyScaleDownWaitDrainThreeExits/kubelet_restarted_the_container (0.00s)
    --- PASS: TestVerifyScaleDownWaitDrainThreeExits/drain_deadline_exceeded (0.00s)
--- PASS: TestVerifyScaleDownReplicasConvergeByOrdinal (0.00s)
PASS
ok  github.com/apache/doris-operator/pkg/controller/.../computegroups  0.772s
```

**全量回归**：`go test ./pkg/controller/sub_controller/disaggregated_cluster/computegroups/ ./pkg/common/utils/mysql/` → 两包均 `ok`；`go vet` 无告警，`gofmt` 无差异。

### 6.4 覆盖边界

| 未覆盖                                 | 原因                                  | 影响                       |
| ----------------------------------- | ----------------------------------- | ------------------------ |
| `exec` 进 Pod 写哨兵 / `stop_be.sh`     | 需 kubelet exec 通道；脚本行为按上游 master 核对 | 该路径由 `WaitDrain` 的超时出口覆盖 |
| `DROP BACKEND` 与 `SHOW BACKENDS` 复核 | 需真实 FE                              | 幂等语义由代码保证，未运行时验证         |
| `errBackendNotFound` 短路（跳过排空）       | 依赖 SQL client                       | 仅静态分析，需集群验证              |
| `supportsTerminatingSentinel` 探测    | 依赖 exec                             | 降级路径未执行                  |

---

## 附录 A：验证测试源码

**文件**：`pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_scaledown_verify_test.go`

<https://github.com/207cheny/doris-operator/blob/dev/feat-26.0.1/pkg/controller/sub_controller/disaggregated_cluster/computegroups/graceful_scaledown_verify_test.go>

```go

// Baseline: tag 26.0.1, commit f705d058c128c052820e588d6971df251d7afc78.
// Run:  go test -run 'TestVerify.*ScaleDown' -v ./pkg/controller/sub_controller/disaggregated_cluster/computegroups/
package computegroups

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dv1 "github.com/apache/doris-operator/api/disaggregated/v1"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// verifyScaleDownSTS builds a StatefulSet named stsName with the given replicas.
func verifyScaleDownSTS(namespace, name string, replicas int32) *appv1.StatefulSet {
	return &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appv1.StatefulSetSpec{
			Replicas: &replicas,
		},
	}
}

// TestVerifyScaleDownDetectAction: detectGracefulAction must yield a ScaleDown action
// when desired replicas (2) < existing replicas (3), targeting phase TriggerDrain.
func TestVerifyScaleDownDetectAction(t *testing.T) {
	dcgs := &DisaggregatedComputeGroupsController{}
	desired := verifyScaleDownSTS("default", "doris-cg1", 2)  // st.Spec.Replicas = 2
	existing := verifyScaleDownSTS("default", "doris-cg1", 3) // est.Spec.Replicas = 3
	cgStatus := &dv1.ComputeGroupStatus{StatefulsetName: "doris-cg1"}

	action := dcgs.detectGracefulAction(desired, existing, cgStatus)
	if action == nil {
		t.Fatal("detectGracefulAction returned nil, want ScaleDown action")
	}
	if action.Type != dv1.GracefulActionScaleDown {
		t.Fatalf("action.Type = %s, want %s", action.Type, dv1.GracefulActionScaleDown)
	}
	if action.Phase != dv1.GracefulPhaseTriggerDrain {
		t.Fatalf("action.Phase = %s, want %s", action.Phase, dv1.GracefulPhaseTriggerDrain)
	}
	if action.DesiredReplicas == nil || *action.DesiredReplicas != 2 {
		t.Fatalf("action.DesiredReplicas = %v, want 2", action.DesiredReplicas)
	}
}

// TestVerifyScaleDownSelectNextPod: the scale-down picker must select the highest
// existing ordinal (sts-2) first, and report not-found once desired >= current.
func TestVerifyScaleDownSelectNextPod(t *testing.T) {
	dcgs := &DisaggregatedComputeGroupsController{}
	cluster := &dv1.DorisDisaggregatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "default"},
	}
	cg := &dv1.ComputeGroup{UniqueId: "cg1"}
	cgStatus := &dv1.ComputeGroupStatus{StatefulsetName: "doris-cg1"}
	ctx := context.Background()

	// Step 1: 3 -> 2, first pod to drain is the highest ordinal.
	existing3 := verifyScaleDownSTS("default", "doris-cg1", 3)
	desired := int32(2)
	ga := &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, DesiredReplicas: &desired}
	podName, ordinal, found := dcgs.selectNextPod(ctx, cluster, cg, cgStatus, existing3, ga)
	if !found {
		t.Fatal("selectNextPod reported not-found for 3->2, want sts-2")
	}
	if podName != "doris-cg1-2" || ordinal != 2 {
		t.Fatalf("selectNextPod = (%s, %d), want (doris-cg1-2, 2)", podName, ordinal)
	}

	// Step 2: after the highest pod is drained, replicas are patched to 2.
	// desired(2) >= current(2) must terminate the scale-down loop.
	existing2 := verifyScaleDownSTS("default", "doris-cg1", 2)
	_, _, found = dcgs.selectNextPod(ctx, cluster, cg, cgStatus, existing2, ga)
	if found {
		t.Fatal("selectNextPod reported found for 2->2, want not-found (scale-down finished)")
	}
}

// TestVerifyScaleDownActionAnnotationRoundTrip: the graceful action must survive
// JSON (de)serialization through the StatefulSet annotation, which is the external
// progress store used to resume after a reconcile crash / restart.
func TestVerifyScaleDownActionAnnotationRoundTrip(t *testing.T) {
	desired := int32(2)
	ga := &dv1.GracefulAction{
		Type:            dv1.GracefulActionScaleDown,
		Phase:           dv1.GracefulPhaseDeletePod,
		CurrentPod:      "doris-cg1-2",
		CurrentOrdinal:  2,
		DesiredReplicas: &desired,
		DrainTriggered:  true,
		SentinelWritten: true,
		LastMessage:     "backend dropped",
	}
	st := &appv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "doris-cg1", Namespace: "default"}}
	setGracefulAction(st, ga)

	// st is the "desired" object; simulate it being persisted, then re-read as est.
	got, err := getGracefulAction(st)
	if err != nil {
		t.Fatalf("getGracefulAction failed: %v", err)
	}
	if got == nil {
		t.Fatal("getGracefulAction returned nil after setGracefulAction")
	}
	if got.Type != dv1.GracefulActionScaleDown || got.CurrentPod != "doris-cg1-2" || got.CurrentOrdinal != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if !got.DrainTriggered || !got.SentinelWritten || got.LastMessage != "backend dropped" {
		t.Fatalf("round-trip lost fields: %+v", got)
	}
	if got.DesiredReplicas == nil || *got.DesiredReplicas != 2 {
		t.Fatalf("round-trip lost DesiredReplicas: %+v", got.DesiredReplicas)
	}
}

// TestVerifyScaleDownReplicasPatchIdempotent: updateStatefulSetReplicas must patch the
// live StatefulSet from 3 -> 2 and be a no-op when the value already matches, which is
// what makes a retry after an API failure safe.
func TestVerifyScaleDownReplicasPatchIdempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	existing := verifyScaleDownSTS("default", "doris-cg1", 3)
	dcgs := &DisaggregatedComputeGroupsController{}
	dcgs.K8sclient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	// est is the controller's in-memory copy; updateStatefulSetReplicas patches the live object.
	est := verifyScaleDownSTS("default", "doris-cg1", 3)
	if err := dcgs.updateStatefulSetReplicas(context.Background(), est, 2); err != nil {
		t.Fatalf("updateStatefulSetReplicas(2) failed: %v", err)
	}
	if est.Spec.Replicas == nil || *est.Spec.Replicas != 2 {
		t.Fatalf("est.Spec.Replicas = %v, want 2", est.Spec.Replicas)
	}

	var live appv1.StatefulSet
	if err := dcgs.K8sclient.Get(context.Background(), objectKey("default", "doris-cg1"), &live); err != nil {
		t.Fatalf("get live statefulset failed: %v", err)
	}
	if live.Spec.Replicas == nil || *live.Spec.Replicas != 2 {
		t.Fatalf("live.Spec.Replicas = %v, want 2 (patch not persisted)", live.Spec.Replicas)
	}

	// Idempotency: a second call with the same target must not error and must not re-patch.
	if err := dcgs.updateStatefulSetReplicas(context.Background(), est, 2); err != nil {
		t.Fatalf("second updateStatefulSetReplicas(2) failed: %v", err)
	}
}

// TestVerifyScaleDownPriorityAndStarted: verifies scale-down has higher priority than
// rolling update, and gracefulScaleDownPodStarted distinguishes "about to start" from
// "already draining".
func TestVerifyScaleDownPriorityAndStarted(t *testing.T) {
	if gracefulActionPriority(dv1.GracefulActionScaleDown) <= gracefulActionPriority(dv1.GracefulActionRollingUpdate) {
		t.Fatalf("scale-down priority (%d) must exceed rolling-update priority (%d)",
			gracefulActionPriority(dv1.GracefulActionScaleDown), gracefulActionPriority(dv1.GracefulActionRollingUpdate))
	}

	// Fresh action: no pod selected, no drain triggered -> not started.
	fresh := &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, Phase: dv1.GracefulPhaseTriggerDrain}
	if gracefulScaleDownPodStarted(fresh) {
		t.Fatal("gracefulScaleDownPodStarted(fresh) = true, want false (cancel is still possible)")
	}

	// Once a pod is selected or drain is triggered, the scale-down must not be cancelled.
	started := &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, Phase: dv1.GracefulPhaseWaitDrain, CurrentPod: "doris-cg1-2"}
	if !gracefulScaleDownPodStarted(started) {
		t.Fatal("gracefulScaleDownPodStarted(started) = false, want true")
	}
}

// TestVerifyScaleDownCancelOnRevert: if a ScaleDown action was stored (existing replicas
// already 2) but the user reverts the spec back to 3 before any pod was drained, the
// operator must cancel the graceful action and clear the annotation instead of deleting pods.
func TestVerifyScaleDownCancelOnRevert(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// Live StatefulSet already scaled to 2, but carries a stored (not yet started) ScaleDown action.
	existing := verifyScaleDownSTS("default", "doris-cg1", 2)
	desired := int32(2)
	stored := &dv1.GracefulAction{
		Type:            dv1.GracefulActionScaleDown,
		Phase:           dv1.GracefulPhaseTriggerDrain,
		DesiredReplicas: &desired,
	}
	setGracefulAction(existing, stored)

	dcgs := &DisaggregatedComputeGroupsController{}
	dcgs.K8sclient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	// Desired spec reverts to 3 replicas.
	desiredSTS := verifyScaleDownSTS("default", "doris-cg1", 3)
	cgStatus := &dv1.ComputeGroupStatus{StatefulsetName: "doris-cg1", Phase: dv1.GracefulScaling}
	cluster := &dv1.DorisDisaggregatedCluster{ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "default"}}
	cg := &dv1.ComputeGroup{UniqueId: "cg1"}

	skipApply, err := dcgs.gracefulRolloutReconcile(context.Background(), nil, desiredSTS, existing, cluster, cg, cgStatus)
	if err != nil {
		t.Fatalf("gracefulRolloutReconcile returned error on cancel: %v", err)
	}
	if skipApply {
		t.Fatal("gracefulRolloutReconcile returned skipApply=true, want false (action cancelled, normal apply resumes)")
	}
	if cgStatus.Phase != dv1.Reconciling {
		t.Fatalf("cgStatus.Phase = %s, want Reconciling after cancel", cgStatus.Phase)
	}

	// The stored annotation must have been removed from the live object.
	var live appv1.StatefulSet
	if err := dcgs.K8sclient.Get(context.Background(), objectKey("default", "doris-cg1"), &live); err != nil {
		t.Fatalf("get live statefulset failed: %v", err)
	}
	if live.Annotations != nil && live.Annotations[gracefulActionAnnotation] != "" {
		t.Fatalf("graceful-action annotation not cleared after cancel: %q", live.Annotations[gracefulActionAnnotation])
	}
}

// TestVerifyScaleDownWithdrawBoundary pins the predicate that decides whether an
// in-flight scale-down can still be withdrawn without losing the pod. The boundary is
// "no pod selected yet", not "no pod deleted yet": as soon as CurrentPod is set, the
// drain was triggered, or the phase left TriggerDrain, the cancel branch in
// gracefulRolloutReconcile is skipped and the in-flight pod is dropped.
//
// The caller additionally gates the cancel branch on Type == ScaleDown, so the
// predicate is only meaningful for scale-down actions.
func TestVerifyScaleDownWithdrawBoundary(t *testing.T) {
	desired := int32(2)
	cases := []struct {
		name    string
		action  *dv1.GracefulAction
		started bool
	}{
		{"no action", nil, false},
		{"fresh action, no pod selected", &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, Phase: dv1.GracefulPhaseTriggerDrain, DesiredReplicas: &desired}, false},
		{"pod selected", &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, Phase: dv1.GracefulPhaseTriggerDrain, CurrentPod: "doris-cg1-2"}, true},
		{"drain triggered", &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, Phase: dv1.GracefulPhaseTriggerDrain, DrainTriggered: true}, true},
		{"phase advanced", &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, Phase: dv1.GracefulPhaseWaitDrain}, true},
		{"non scale-down action", &dv1.GracefulAction{Type: dv1.GracefulActionRollingUpdate, Phase: dv1.GracefulPhaseTriggerDrain, CurrentPod: "doris-cg1-2"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := gracefulScaleDownPodStarted(c.action); got != c.started {
				t.Fatalf("gracefulScaleDownPodStarted = %v, want %v", got, c.started)
			}
		})
	}
}

func objectKey(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// TestVerifyScaleDownPVCCleanup: once a StatefulSet has been scaled 3 -> 2,
// ClearStatefulsetUnusedPVCs must delete the PVC whose ordinal is >= replicas
// (doris-cg1-2) and keep the active PVCs (doris-cg1-0, doris-cg1-1). This is the
// resource-cleanup guarantee that frees the scaled-down BE's persistent volume.
func TestVerifyScaleDownPVCCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	// StatefulSet already scaled to 2.
	st := verifyScaleDownSTS("default", "doris-cg1", 2)

	// PVCs named <sts>-<ordinal>, tagged with the compute-group selector labels.
	dcgs := &DisaggregatedComputeGroupsController{}
	labels := dcgs.newCGPodsSelector("doris", "cg1")
	var objs []client.Object
	objs = append(objs, st)
	for i := 0; i < 3; i++ {
		objs = append(objs, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("doris-cg1-%d", i),
				Namespace: "default",
				Labels:    labels,
			},
		})
	}
	dcgs.K8sclient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	ddc := &dv1.DorisDisaggregatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "default"},
		Spec: dv1.DorisDisaggregatedClusterSpec{
			ComputeGroups: []dv1.ComputeGroup{{UniqueId: "cg1"}},
		},
	}
	cgs := dv1.ComputeGroupStatus{UniqueId: "cg1"}
	if err := dcgs.ClearStatefulsetUnusedPVCs(context.Background(), ddc, cgs); err != nil {
		t.Fatalf("ClearStatefulsetUnusedPVCs failed: %v", err)
	}

	var l corev1.PersistentVolumeClaimList
	if err := dcgs.K8sclient.List(context.Background(), &l, client.InNamespace("default")); err != nil {
		t.Fatalf("list pvc failed: %v", err)
	}
	names := map[string]bool{}
	for _, p := range l.Items {
		names[p.Name] = true
	}
	if names["doris-cg1-2"] {
		t.Fatal("PVC doris-cg1-2 (ordinal >= replicas) was not cleaned up")
	}
	if !names["doris-cg1-0"] || !names["doris-cg1-1"] {
		t.Fatal("active PVCs doris-cg1-0 / doris-cg1-1 should be kept")
	}
}

// ---------------------------------------------------------------------------
// Tests added for the "core turning point" + "state machine" chapters.
// ---------------------------------------------------------------------------

// TestVerifyScaleDownPVCCleanupSkipsDeletedCG pins down the known gap of the cleanup
// layer: ClearStatefulsetUnusedPVCs resolves the ComputeGroup from spec first and
// returns early when the CG is gone. So when a whole ComputeGroup is deleted, its
// PVCs are NOT removed by this function - they can only be reclaimed by ownerRef
// cascade GC. This test documents the behaviour instead of assuming it.
func TestVerifyScaleDownPVCCleanupSkipsDeletedCG(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	dcgs := &DisaggregatedComputeGroupsController{}
	labels := dcgs.newCGPodsSelector("doris", "cg1")
	objs := []client.Object{verifyScaleDownSTS("default", "doris-cg1", 0)}
	for i := 0; i < 2; i++ {
		objs = append(objs, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("doris-cg1-%d", i),
				Namespace: "default",
				Labels:    labels,
			},
		})
	}
	dcgs.K8sclient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	// spec no longer references cg1 -> cg == nil inside the cleanup function.
	ddc := &dv1.DorisDisaggregatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "default"},
		Spec:       dv1.DorisDisaggregatedClusterSpec{ComputeGroups: nil},
	}
	cgs := dv1.ComputeGroupStatus{UniqueId: "cg1"}
	if err := dcgs.ClearStatefulsetUnusedPVCs(context.Background(), ddc, cgs); err != nil {
		t.Fatalf("ClearStatefulsetUnusedPVCs failed: %v", err)
	}

	var l corev1.PersistentVolumeClaimList
	if err := dcgs.K8sclient.List(context.Background(), &l, client.InNamespace("default")); err != nil {
		t.Fatalf("list pvc failed: %v", err)
	}
	if len(l.Items) != 2 {
		t.Fatalf("PVC count = %d, want 2 (deleted CG's PVCs are left to ownerRef GC)", len(l.Items))
	}
}

// TestVerifyScaleDownPreparePinsReplicas is the single most important turning
// point of the whole design: while a ScaleDown action is in progress, the desired
// StatefulSet (st) is NOT allowed to carry the user's target replicas. It is pinned
// back to the live value (est), so the normal apply cannot collapse 3 -> 2 in one
// shot and skip the graceful flow. The real reduction only happens inside
// afterPodDeleted via updateStatefulSetReplicas.
func TestVerifyScaleDownPreparePinsReplicas(t *testing.T) {
	st := verifyScaleDownSTS("default", "doris-cg1", 2)  // user's target
	est := verifyScaleDownSTS("default", "doris-cg1", 3) // live
	desired := int32(2)
	ga := &dv1.GracefulAction{Type: dv1.GracefulActionScaleDown, DesiredReplicas: &desired}

	prepareGracefulStatefulSet(st, est, ga)

	if st.Spec.Replicas == nil || *st.Spec.Replicas != 3 {
		t.Fatalf("st.Spec.Replicas = %v, want 3 (pinned to live value during scale-down)", st.Spec.Replicas)
	}
	if st.Spec.UpdateStrategy.Type != appv1.OnDeleteStatefulSetStrategyType {
		t.Fatalf("st.Spec.UpdateStrategy.Type = %s, want OnDelete", st.Spec.UpdateStrategy.Type)
	}

	// A rolling update must NOT pin replicas (it keeps the desired template).
	st2 := verifyScaleDownSTS("default", "doris-cg1", 2)
	prepareGracefulStatefulSet(st2, est, &dv1.GracefulAction{Type: dv1.GracefulActionRollingUpdate})
	if *st2.Spec.Replicas != 2 {
		t.Fatalf("rolling update pinned replicas to %d, want 2 (unpinned)", *st2.Spec.Replicas)
	}
}

// TestVerifyScaleDownWaitDrainThreeExits covers the three mutually exclusive exits
// of WaitDrain. All of them advance to DeletePod, which is what makes the wait
// bounded: it can never hang forever on a BE that refuses to exit.
func TestVerifyScaleDownWaitDrainThreeExits(t *testing.T) {
	past := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	future := metav1.NewTime(time.Now().Add(10 * time.Minute))

	cases := []struct {
		name        string
		pod         *corev1.Pod
		deadline    metav1.Time
		wantAnomaly bool
		wantMessage string
	}{
		{
			name: "container terminated",
			pod: verifyScaleDownPod("default", "doris-cg1-2", corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"},
			}, 0),
			deadline: future,
		},
		{
			name: "kubelet restarted the container",
			pod: verifyScaleDownPod("default", "doris-cg1-2", corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{},
			}, 1), // restartCount 0 -> 1
			deadline:    future,
			wantAnomaly: true,
		},
		{
			name: "drain deadline exceeded",
			pod: verifyScaleDownPod("default", "doris-cg1-2", corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{},
			}, 0),
			deadline:    past,
			wantMessage: "timeout",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			dcgs := &DisaggregatedComputeGroupsController{}
			dcgs.K8sclient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.pod).Build()
			dcgs.K8srecorder = record.NewFakeRecorder(20)

			ga := &dv1.GracefulAction{
				Type:                dv1.GracefulActionScaleDown,
				Phase:               dv1.GracefulPhaseWaitDrain,
				CurrentPod:          "doris-cg1-2",
				CurrentOrdinal:      2,
				InitialRestartCount: 0,
				DeadlineAt:          tc.deadline,
			}
			cluster := &dv1.DorisDisaggregatedCluster{ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "default"}}
			cg := &dv1.ComputeGroup{UniqueId: "cg1"}
			cgStatus := &dv1.ComputeGroupStatus{StatefulsetName: "doris-cg1", Phase: dv1.GracefulScaling}

			if err := dcgs.handleWaitDrain(context.Background(), cluster, cg, cgStatus, ga); err != nil {
				t.Fatalf("handleWaitDrain failed: %v", err)
			}
			if ga.Phase != dv1.GracefulPhaseDeletePod {
				t.Fatalf("phase = %s, want DeletePod (all three exits must advance)", ga.Phase)
			}
			if ga.RestartAnomalyDetected != tc.wantAnomaly {
				t.Fatalf("RestartAnomalyDetected = %t, want %t", ga.RestartAnomalyDetected, tc.wantAnomaly)
			}
			if tc.wantMessage != "" && !strings.Contains(strings.ToLower(ga.LastMessage), tc.wantMessage) {
				t.Fatalf("LastMessage = %q, want it to mention %q", ga.LastMessage, tc.wantMessage)
			}
		})
	}
}

// TestVerifyScaleDownReplicasConvergeByOrdinal proves the convergence rule
// "newReplicas = CurrentOrdinal" (0-indexed). Scaling 5 -> 3 therefore takes two
// rounds, one ordinal at a time, and never jumps: 5 -> 4 -> 3.
func TestVerifyScaleDownReplicasConvergeByOrdinal(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	live := verifyScaleDownSTS("default", "doris-cg1", 5)
	dcgs := &DisaggregatedComputeGroupsController{}
	dcgs.K8sclient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()
	dcgs.K8srecorder = record.NewFakeRecorder(20)

	cluster := &dv1.DorisDisaggregatedCluster{ObjectMeta: metav1.ObjectMeta{Name: "doris", Namespace: "default"}}
	cg := &dv1.ComputeGroup{UniqueId: "cg1"}
	cgStatus := &dv1.ComputeGroupStatus{StatefulsetName: "doris-cg1"}

	// Round 1: highest ordinal 4 deleted -> replicas 4.
	est := verifyScaleDownSTS("default", "doris-cg1", 5)
	desired := int32(3)
	ga := &dv1.GracefulAction{
		Type:            dv1.GracefulActionScaleDown,
		Phase:           dv1.GracefulPhaseDeletePod,
		CurrentPod:      "doris-cg1-4",
		CurrentOrdinal:  4,
		DesiredReplicas: &desired,
	}
	if err := dcgs.afterPodDeleted(ga, cluster, cg, cgStatus, est); err != nil {
		t.Fatalf("afterPodDeleted(round1) failed: %v", err)
	}
	if got := verifyLiveReplicas(t, dcgs, "default", "doris-cg1"); got != 4 {
		t.Fatalf("after deleting ordinal 4, replicas = %d, want 4 (one step only)", got)
	}
	if ga.Phase != dv1.GracefulPhaseTriggerDrain || ga.CurrentPod != "" {
		t.Fatalf("after round1 phase = %s pod = %q, want TriggerDrain with empty pod", ga.Phase, ga.CurrentPod)
	}

	// Round 2: ordinal 3 deleted -> replicas 3, which now equals the target.
	est2 := verifyScaleDownSTS("default", "doris-cg1", 4)
	ga.CurrentOrdinal = 3
	ga.CurrentPod = "doris-cg1-3"
	if err := dcgs.afterPodDeleted(ga, cluster, cg, cgStatus, est2); err != nil {
		t.Fatalf("afterPodDeleted(round2) failed: %v", err)
	}
	if got := verifyLiveReplicas(t, dcgs, "default", "doris-cg1"); got != 3 {
		t.Fatalf("after deleting ordinal 3, replicas = %d, want 3", got)
	}

	// Round 3: no more work -> selectNextPod reports not-found, TriggerDrain ends in Done.
	est3 := verifyScaleDownSTS("default", "doris-cg1", 3)
	if err := dcgs.handleTriggerDrain(context.Background(), nil, cluster, cg, cgStatus, est3, ga); err != nil {
		t.Fatalf("handleTriggerDrain failed: %v", err)
	}
	if ga.Phase != dv1.GracefulPhaseDone {
		t.Fatalf("phase = %s, want Done once desired >= current", ga.Phase)
	}
}

// verifyScaleDownPod builds a pod whose main "compute" container has the given state.
func verifyScaleDownPod(namespace, name string, state corev1.ContainerState, restartCount int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: beMainContainerName}},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         beMainContainerName,
				State:        state,
				RestartCount: restartCount,
				ContainerID:  "docker://old",
			}},
		},
	}
}

func verifyLiveReplicas(t *testing.T, dcgs *DisaggregatedComputeGroupsController, namespace, name string) int32 {
	t.Helper()
	var live appv1.StatefulSet
	if err := dcgs.K8sclient.Get(context.Background(), objectKey(namespace, name), &live); err != nil {
		t.Fatalf("get live statefulset failed: %v", err)
	}
	if live.Spec.Replicas == nil {
		t.Fatal("live.Spec.Replicas is nil")
	}
	return *live.Spec.Replicas
}

```
