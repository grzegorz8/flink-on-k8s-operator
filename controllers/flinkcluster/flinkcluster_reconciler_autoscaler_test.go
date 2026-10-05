package flinkcluster

import (
	"context"
	"fmt"
	"testing"

	v1beta1 "github.com/spotify/flink-on-k8s-operator/apis/flinkcluster/v1beta1"
	"gotest.tools/v3/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAutoscalerReconcileCreation(t *testing.T) {
	// given: a configured autoscaler with no resources
	ctx := context.Background()
	cluster := autoscalerTestCluster()
	k8s := autoscalerTestClient(t)
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}
	cmKey := types.NamespacedName{Namespace: cluster.Namespace, Name: "example-autoscaler-config"}
	depKey := types.NamespacedName{Namespace: cluster.Namespace, Name: "example-autoscaler"}

	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: it is reconciled
	err := reconciler.reconcileAutoscaler(ctx)

	// then: ConfigMap and Deployment are created and both carry the same spec checksum
	assert.NilError(t, err)
	cm, dep := &corev1.ConfigMap{}, &appsv1.Deployment{}
	assert.NilError(t, k8s.Get(ctx, cmKey, cm))
	assert.NilError(t, k8s.Get(ctx, depKey, dep))
	checksum := dep.Annotations[autoscalerSpecHashAnnotation]
	assert.Assert(t, checksum != "")
	assert.Equal(t, cm.Annotations[autoscalerSpecHashAnnotation], checksum)
	assert.Equal(t, dep.Spec.Template.Annotations[autoscalerSpecHashAnnotation], checksum)
}

func TestAutoscalerReconcileUnchangedSpec(t *testing.T) {
	// given: autoscaler resources matching the configured spec
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	k8s := autoscalerTestClient(t, cm, dep)
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(context.Background()))

	// when: reconciliation runs without a spec change
	err = reconciler.reconcileAutoscaler(context.Background())

	// then: neither object is written again
	assert.NilError(t, err)
	assert.Equal(t, len(k8s.writes), 0)
}

func TestAutoscalerReconcileFlinkConfigurationChange(t *testing.T) {
	// given: autoscaler resources matching the configured spec
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	k8s := autoscalerTestClient(t, cm, dep)
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: Flink configuration has changed
	cluster.Spec.FlinkProperties = map[string]string{"parallelism.default": "10"}
	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(context.Background()))

	// when: reconciliation runs after Flink configuration changes
	err = reconciler.reconcileAutoscaler(context.Background())

	// then: the autoscaler resources remain unchanged
	assert.NilError(t, err)
	assert.Equal(t, len(k8s.writes), 0)
}

func TestAutoscalerReconcilePropertiesChange(t *testing.T) {
	// given: autoscaler resources matching the configured spec
	ctx := context.Background()
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	originalHash := dep.Annotations[autoscalerSpecHashAnnotation]
	k8s := autoscalerTestClient(t, cm, dep)
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: autoscaler properties have changed
	cluster.Spec.Autoscaler.AutoscalerProperties = map[string]string{"key": "new"}
	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: reconciliation runs after autoscaler properties change
	err = reconciler.reconcileAutoscaler(ctx)

	// then: the ConfigMap is updated before the Deployment
	assert.NilError(t, err)
	assert.DeepEqual(t, k8s.writes, []string{"update:example-autoscaler-config", "update:example-autoscaler"})
	assert.NilError(t, k8s.Get(ctx, client.ObjectKeyFromObject(cm), cm))
	assert.NilError(t, k8s.Get(ctx, client.ObjectKeyFromObject(dep), dep))
	assert.Assert(t, dep.Annotations[autoscalerSpecHashAnnotation] != originalHash)
	assert.Equal(t, cm.Annotations[autoscalerSpecHashAnnotation], dep.Annotations[autoscalerSpecHashAnnotation])
	assert.Equal(t, dep.Spec.Template.Annotations[autoscalerSpecHashAnnotation], dep.Annotations[autoscalerSpecHashAnnotation])
	assert.Equal(t, cm.Data["config.yaml"], "key: new\n")
}

func TestAutoscalerReconcileImageChange(t *testing.T) {
	// given: autoscaler resources matching the configured spec
	ctx := context.Background()
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	originalHash := dep.Annotations[autoscalerSpecHashAnnotation]
	originalConfig := cm.Data["config.yaml"]
	k8s := autoscalerTestClient(t, cm, dep)
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: the autoscaler image has changed
	cluster.Spec.Autoscaler.PodTemplate.Spec.Containers[0].Image = "example/autoscaler:2"
	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: reconciliation runs after the autoscaler image changes
	err = reconciler.reconcileAutoscaler(ctx)

	// then: both resources advance to the new checksum and the Pod image changes
	assert.NilError(t, err)
	assert.DeepEqual(t, k8s.writes, []string{"update:example-autoscaler-config", "update:example-autoscaler"})
	assert.NilError(t, k8s.Get(ctx, client.ObjectKeyFromObject(cm), cm))
	assert.NilError(t, k8s.Get(ctx, client.ObjectKeyFromObject(dep), dep))
	assert.Assert(t, dep.Annotations[autoscalerSpecHashAnnotation] != originalHash)
	assert.Equal(t, cm.Annotations[autoscalerSpecHashAnnotation], dep.Annotations[autoscalerSpecHashAnnotation])
	assert.Equal(t, dep.Spec.Template.Annotations[autoscalerSpecHashAnnotation], dep.Annotations[autoscalerSpecHashAnnotation])
	assert.Equal(t, cm.Data["config.yaml"], originalConfig)
	assert.Equal(t, dep.Spec.Template.Spec.Containers[0].Image, "example/autoscaler:2")
}

func TestAutoscalerRecreatesMissingDeployment(t *testing.T) {
	// given: an autoscaler whose Deployment has been removed
	ctx := context.Background()
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	k8s := autoscalerTestClient(t, cm, dep)
	assert.NilError(t, k8s.Client.Delete(ctx, dep))
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: autoscaler resources are reconciled
	err = reconciler.reconcileAutoscaler(ctx)

	// then: only the Deployment is recreated from the spec
	assert.NilError(t, err)
	assert.DeepEqual(t, k8s.writes, []string{"create:example-autoscaler"})
	actual := &appsv1.Deployment{}
	assert.NilError(t, k8s.Get(ctx, client.ObjectKeyFromObject(dep), actual))
	assert.DeepEqual(t, actual.Spec, dep.Spec)
}

func TestAutoscalerRecreatesMissingConfigMap(t *testing.T) {
	// given: an autoscaler whose ConfigMap has been removed
	ctx := context.Background()
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	k8s := autoscalerTestClient(t, cm, dep)
	assert.NilError(t, k8s.Client.Delete(ctx, cm))
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: autoscaler resources are reconciled
	err = reconciler.reconcileAutoscaler(ctx)

	// then: only the ConfigMap is recreated with properties from the spec
	assert.NilError(t, err)
	assert.DeepEqual(t, k8s.writes, []string{"create:example-autoscaler-config"})
	actual := &corev1.ConfigMap{}
	assert.NilError(t, k8s.Get(ctx, client.ObjectKeyFromObject(cm), actual))
	assert.DeepEqual(t, actual.Data, cm.Data)
}

func TestAutoscalerReconciliationSkipsWhenClusterBeingDeleted(t *testing.T) {
	// given: a cluster being deleted
	cluster := autoscalerTestCluster()
	now := metav1.Now()
	cluster.DeletionTimestamp = &now
	k8s := autoscalerTestClient(t)
	ctx := context.Background()
	reconciler := &ClusterReconciler{
		k8sClient: k8s,
		observed:  ObservedClusterState{cluster: cluster},
	}

	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: autoscaler reconciliation runs
	err := reconciler.reconcileAutoscaler(ctx)

	// then: no resources are created or changed
	assert.NilError(t, err)
	assert.Equal(t, len(k8s.writes), 0)
}

func TestAutoscalerRemovedSpecDeletesResources(t *testing.T) {
	// given: existing autoscaler resources and an omitted autoscaler spec
	cluster := autoscalerTestCluster()
	cm, dep, err := autoscalerTestResources(cluster)
	assert.NilError(t, err)
	cluster.Spec.Autoscaler = nil
	k8s := autoscalerTestClient(t, cm, dep)
	ctx := context.Background()
	reconciler := &ClusterReconciler{k8sClient: k8s, observed: ObservedClusterState{cluster: cluster}}

	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: autoscaler reconciliation runs
	err = reconciler.reconcileAutoscaler(ctx)

	// then: both resources are deleted
	assert.NilError(t, err)
	assert.DeepEqual(t, k8s.writes, []string{"delete:example-autoscaler-config", "delete:example-autoscaler"})
	assert.Assert(t, apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})))
	assert.Assert(t, apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(dep), &appsv1.Deployment{})))

	// given: both autoscaler resources have already been deleted
	k8s.writes = nil
	// and: desired and observed autoscaler resources are prepared
	assert.NilError(t, reconciler.setDesiredAutoscalerResources())
	assert.NilError(t, reconciler.observeAutoscalerResources(ctx))

	// when: reconciliation repeats after deletion
	err = reconciler.reconcileAutoscaler(ctx)

	// then: deleting already-missing resources succeeds
	assert.NilError(t, err)
	assert.Equal(t, len(k8s.writes), 0)
}

type autoscalerRecordingClient struct {
	client.Client
	writes     []string
	failCreate string
	failGet    string
	failUpdate string
	failDelete string
}

func (c *autoscalerRecordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.writes = append(c.writes, "create:"+obj.GetName())
	if obj.GetName() == c.failCreate {
		return fmt.Errorf("injected create failure")
	}
	return c.Client.Create(ctx, obj, opts...)
}
func (c *autoscalerRecordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.writes = append(c.writes, "update:"+obj.GetName())
	if obj.GetName() == c.failUpdate {
		return fmt.Errorf("injected update failure")
	}
	return c.Client.Update(ctx, obj, opts...)
}
func (c *autoscalerRecordingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if key.Name == c.failGet {
		return fmt.Errorf("injected get failure")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
func (c *autoscalerRecordingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.writes = append(c.writes, "delete:"+obj.GetName())
	if obj.GetName() == c.failDelete {
		return fmt.Errorf("injected delete failure")
	}
	return c.Client.Delete(ctx, obj, opts...)
}
func autoscalerTestClient(t *testing.T, objects ...client.Object) *autoscalerRecordingClient {
	t.Helper()
	scheme := runtime.NewScheme()
	assert.NilError(t, corev1.AddToScheme(scheme))
	assert.NilError(t, appsv1.AddToScheme(scheme))
	assert.NilError(t, v1beta1.AddToScheme(scheme))
	return &autoscalerRecordingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
}

func autoscalerTestResources(cluster *v1beta1.FlinkCluster) (*corev1.ConfigMap, *appsv1.Deployment, error) {
	checksum, err := autoscalerSpecChecksum(cluster)
	if err != nil {
		return nil, nil, err
	}
	configMap, err := newAutoscalerConfigMap(cluster, checksum)
	if err != nil {
		return nil, nil, err
	}
	deployment, err := newAutoscalerDeployment(cluster, checksum)
	if err != nil {
		return nil, nil, err
	}
	return configMap, deployment, nil
}

func (reconciler *ClusterReconciler) setDesiredAutoscalerResources() error {
	cluster := reconciler.observed.cluster
	reconciler.desired.AutoscalerConfigMap = nil
	reconciler.desired.AutoscalerDeployment = nil
	if cluster == nil || cluster.Spec.Autoscaler == nil || !cluster.DeletionTimestamp.IsZero() {
		return nil
	}
	configMap, deployment, err := autoscalerTestResources(cluster)
	if err != nil {
		return err
	}
	reconciler.desired.AutoscalerConfigMap = configMap
	reconciler.desired.AutoscalerDeployment = deployment
	return nil
}

func (reconciler *ClusterReconciler) observeAutoscalerResources(ctx context.Context) error {
	cluster := reconciler.observed.cluster
	if cluster == nil {
		return nil
	}
	observer := &ClusterStateObserver{
		k8sClient: reconciler.k8sClient,
		request: ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: cluster.Namespace,
			Name:      cluster.Name,
		}},
	}
	return observer.observeAutoscaler(ctx, &reconciler.observed)
}
