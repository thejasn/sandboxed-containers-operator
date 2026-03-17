/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	nodeapi "k8s.io/api/node/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// RuntimeClassReconciler reconciles RuntimeClass objects to handle finalizers
type RuntimeClassReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

const (
	runtimeClassFinalizerName = "kataconfig.openshift.io/runtime-class-cleanup"
	requeueInterval           = time.Second * 10
)

// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile handles RuntimeClass lifecycle, specifically finalizer cleanup
func (r *RuntimeClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("runtimeclass", req.NamespacedName.Name)

	runtimeClass := &nodeapi.RuntimeClass{}
	err := r.Get(ctx, req.NamespacedName, runtimeClass)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get RuntimeClass")
		return ctrl.Result{}, err
	}

	if !controllerutil.ContainsFinalizer(runtimeClass, runtimeClassFinalizerName) {
		return ctrl.Result{}, nil
	}

	if runtimeClass.DeletionTimestamp != nil {
		return r.handleRuntimeClassDeletion(ctx, runtimeClass, log)
	}

	return ctrl.Result{}, nil
}

// handleRuntimeClassDeletion manages the cleanup process when a RuntimeClass is being deleted
func (r *RuntimeClassReconciler) handleRuntimeClassDeletion(ctx context.Context, runtimeClass *nodeapi.RuntimeClass, log logr.Logger) (ctrl.Result, error) {
	log.Info("RuntimeClass is being deleted, checking for active pods")

	hasActivePods, err := r.hasPodsUsingRuntimeClass(ctx, runtimeClass.Name)
	if err != nil {
		log.Error(err, "Failed to check for pods using runtime class")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	if hasActivePods {
		log.Info("Runtime class still has active pods, waiting for them to be deleted manually or terminate naturally")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	log.Info("No active pods found, removing finalizer to allow deletion")
	controllerutil.RemoveFinalizer(runtimeClass, runtimeClassFinalizerName)
	err = r.Update(ctx, runtimeClass)
	if err != nil {
		log.Error(err, "Failed to remove finalizer from RuntimeClass")
		return ctrl.Result{}, err
	}

	log.Info("Successfully removed finalizer, RuntimeClass will be deleted")
	return ctrl.Result{}, nil
}

// hasPodsUsingRuntimeClass checks if there are any active pods using the specified runtime class
func (r *RuntimeClassReconciler) hasPodsUsingRuntimeClass(ctx context.Context, runtimeClassName string) (bool, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList); err != nil {
		return true, err
	}

	for _, pod := range podList.Items {
		if pod.Spec.RuntimeClassName != nil &&
			*pod.Spec.RuntimeClassName == runtimeClassName {
			if pod.Status.Phase == corev1.PodRunning ||
				pod.Status.Phase == corev1.PodPending ||
				pod.Status.Phase == corev1.PodUnknown {
				r.Log.Info("Found active pod using runtime class",
					"pod", pod.Name,
					"namespace", pod.Namespace,
					"runtimeClass", runtimeClassName,
					"phase", pod.Status.Phase)
				return true, nil
			}
		}
	}

	return false, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *RuntimeClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nodeapi.RuntimeClass{}, builder.WithPredicates(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				rc := e.ObjectNew.(*nodeapi.RuntimeClass)
				return controllerutil.ContainsFinalizer(rc, runtimeClassFinalizerName) &&
					rc.DeletionTimestamp != nil
			},
		})).
		Complete(r)
}

// createRuntimeClass creates a runtimeclass if it doesn't exist
// When checkNodeEligibility is set to true, prior to creation
// it verifies if nodes that support this runtime class exist. This
// is done by checking if nodes have all labels in additionalNodeLabels.
func (r *KataConfigOpenShiftReconciler) createRuntimeClass(
	runtimeClassName string,
	cpuOverhead string,
	memoryOverhead string,
	extResOverhead string,
	handler string,
	additionalNodeLabels map[string]string) error {

	if r.kataConfig.Spec.CheckNodeEligibility {

		r.Log.Info("filtering nodes with labels", "labels", additionalNodeLabels)
		selector, err := r.getKataConfigNodeSelectorAsSelector()
		if err != nil {
			return fmt.Errorf("failed to build node selector: %w", err)
		}

		nodes := &corev1.NodeList{}
		listOpts := []client.ListOption{
			client.MatchingLabelsSelector{Selector: selector},
			client.MatchingLabels(additionalNodeLabels),
		}
		if err := r.Client.List(context.TODO(), nodes, listOpts...); err != nil {
			return fmt.Errorf("failed to list nodes: %w", err)
		}

		if len(nodes.Items) == 0 {
			r.Log.Info("skipping creating runtimeclass due to missing labels", "runtimeclass", runtimeClassName)
			return nil
		}
	}

	rc := func() *nodeapi.RuntimeClass {
		podFixed := corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuOverhead),
			corev1.ResourceMemory: resource.MustParse(memoryOverhead),
		}

		// Add extended resource if provided
		if extResOverhead != "" {
			podFixed[corev1.ResourceName(extResOverhead)] = resource.MustParse("1")
		}

		rc := &nodeapi.RuntimeClass{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "node.k8s.io/v1",
				Kind:       "RuntimeClass",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:       runtimeClassName,
				Finalizers: []string{runtimeClassFinalizerName},
			},
			Handler: handler,
			Overhead: &nodeapi.Overhead{
				PodFixed: podFixed,
			},
		}

		nodeSelector := r.getNodeSelectorAsMap()

		// Add additional node label if provided
		if r.kataConfig.Spec.CheckNodeEligibility && additionalNodeLabels != nil {
			maps.Copy(nodeSelector, additionalNodeLabels)
		}

		rc.Scheduling = &nodeapi.Scheduling{
			NodeSelector: nodeSelector,
		}

		r.Log.Info("RuntimeClass", "name", runtimeClassName, "nodeSelector", nodeSelector)

		return rc
	}()

	// Set Kataconfig r.kataConfig as the owner and controller
	if err := controllerutil.SetControllerReference(r.kataConfig, rc, r.Scheme); err != nil {
		return err
	}

	foundRc := &nodeapi.RuntimeClass{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: rc.Name}, foundRc)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return err
		}

		r.Log.Info("Creating a new RuntimeClass", "rc.Name", rc.Name)
		err = r.Client.Create(context.TODO(), rc)
		if err != nil {
			return fmt.Errorf("error creating %s runtime class: %w", rc.Name, err)
		}
	}

	if !contains(r.kataConfig.Status.RuntimeClasses, runtimeClassName) {
		r.kataConfig.Status.RuntimeClasses = append(r.kataConfig.Status.RuntimeClasses, runtimeClassName)
	}

	return nil
}

func (r *KataConfigOpenShiftReconciler) deleteRuntimeClass(runtimeClassName string) error {

	foundRc := &nodeapi.RuntimeClass{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: runtimeClassName}, foundRc)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if err := r.Client.Delete(context.TODO(), foundRc); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	for i, name := range r.kataConfig.Status.RuntimeClasses {
		if name == runtimeClassName {
			r.kataConfig.Status.RuntimeClasses = append(r.kataConfig.Status.RuntimeClasses[:i], r.kataConfig.Status.RuntimeClasses[i+1:]...)
			break
		}
	}

	return nil
}
