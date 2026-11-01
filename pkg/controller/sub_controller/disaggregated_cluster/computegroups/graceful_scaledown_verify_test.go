// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This test file is an ADDITIONAL verification script (appended to the report appendix).
// It deterministically exercises the ComputeGroup graceful scale-down (3 -> 2) state
// machine without requiring a real cluster: all branches that touch Doris FE SQL or
// exec-into-pod are avoided, so the assertions are fully reproducible.
//
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
