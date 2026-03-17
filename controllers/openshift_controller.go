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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	"k8s.io/apimachinery/pkg/labels"

	ignTypes "github.com/coreos/ignition/v2/config/v3_2/types"
	"github.com/go-logr/logr"
	configv1 "github.com/openshift/api/config/v1"
	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	secv1 "github.com/openshift/api/security/v1"
	"github.com/openshift/machine-config-operator/pkg/apihelpers"
	kataconfigurationv1 "github.com/openshift/sandboxed-containers-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	nodeapi "k8s.io/api/node/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// blank assignment to verify that KataConfigOpenShiftReconciler implements reconcile.Reconciler
// var _ reconcile.Reconciler = &KataConfigOpenShiftReconciler{}

// KataConfigOpenShiftReconciler reconciles a KataConfig object
type KataConfigOpenShiftReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme

	kataConfig *kataconfigurationv1.KataConfig

	ImgMc *mcfgv1.MachineConfig

	DeploymentMode DeploymentMode
}

type customKernelConfig struct {
	Image      string
	KernelPath string
}

const (
	OperatorNamespace             = "openshift-sandboxed-containers-operator"
	dashboardConfigMapName        = "grafana-dashboard-sandboxed-containers"
	DashboardConfigMapNamespace   = "openshift-config-managed"
	container_runtime_config_name = "kata-crio-config"
	extension_mc_name             = "50-enable-sandboxed-containers-extension"
	KataAddonConfigMapName        = "kata-addon-artifacts"
	// Use same Pod Overhead as upstream kata-deploy using, see
	// https://github.com/kata-containers/kata-containers/blob/main/tools/packaging/kata-deploy/runtimeclasses/kata-qemu.yaml#L7
	kataRuntimeClassName        = "kata"
	kataRuntimeClassCpuOverhead = "0.25"
	// We need a higher value than upstream (see https://github.com/openshift/sandboxed-containers-operator/pull/84)
	kataRuntimeClassMemOverhead = "350Mi"

	kataNvidiaGPURuntimeClassName        = "kata-nvidia-gpu"
	kataNvidiaGPURuntimeClassCpuOverhead = "1"
	kataNvidiaGPURuntimeClassMemOverhead = "4096Mi"
)

var (
	// node labels for NVIDIA GPU
	nvidiaGPUNodeLabels = map[string]string{
		"nvidia.com/gpu.present":                           "true",
		"nvidia.com/gpu.deploy.vfio-manager":               "true",
		"nvidia.com/gpu.deploy.kata-sandbox-device-plugin": "true",
		"nvidia.com/cc.mode.state":                         "off",
		"nvidia.com/cc.ready.state":                        "false",
	}
)

// +kubebuilder:rbac:groups=kataconfiguration.openshift.io,resources=kataconfigs;kataconfigs/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kataconfiguration.openshift.io,resources=kataconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;daemonsets;replicasets;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=daemonsets/finalizers,resourceNames=manager-role,verbs=update
// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.openshift.io,resources=clusterversions,verbs=get;list;watch
// +kubebuilder:rbac:groups="";machineconfiguration.openshift.io,resources=nodes;machineconfigs;machineconfigpools;containerruntimeconfigs;pods;services;services/finalizers;endpoints;persistentvolumeclaims;events;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=use;get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;update
// +kubebuilder:rbac:groups="",resources=nodes/status,verbs=patch
// +kubebuilder:rbac:groups=confidentialcontainers.org,resources=peerpodconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=confidentialcontainers.org,resources=peerpodconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=confidentialcontainers.org,resources=peerpodconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups=confidentialcontainers.org,resources=peerpods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=confidentialcontainers.org,resources=peerpods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=confidentialcontainers.org,resources=peerpods/finalizers,verbs=update
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=config.openshift.io,resources=infrastructures,verbs=get;list;watch
// +kubebuilder:rbac:groups="batch",resources=jobs,verbs=create;get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list

func (r *KataConfigOpenShiftReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = r.Log.WithValues("kataconfig", req.NamespacedName)
	r.Log.Info("Reconciling KataConfig in OpenShift Cluster")

	// Fetch the KataConfig instance
	r.kataConfig = &kataconfigurationv1.KataConfig{}
	err := r.Client.Get(context.TODO(), req.NamespacedName, r.kataConfig)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Request object not found, could have been deleted after ctrl request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		r.Log.Error(err, "Cannot retrieve kataConfig")
		return ctrl.Result{}, err
	}

	oldObjStatus := r.kataConfig.Status.DeepCopy()

	err = r.migratePeerPodsLimit()
	if err != nil {
		r.Log.Info("Failed to migrate PeerPodConfig limit", "err", err)
		return ctrl.Result{}, err
	}

	err = r.ensureRuntimeClassFinalizers()
	if err != nil {
		r.Log.Info("Failed to ensure runtime class finalizers", "err", err)
		return ctrl.Result{}, err
	}

	err = r.processFeatureGates()
	if err != nil {
		r.Log.Info("Unable to process feature gates", "err", err)
		return ctrl.Result{}, err
	}

	return func() (ctrl.Result, error) {

		// k8s resource correctness checking on creation/modification
		// isn't fully reliable for matchExpressions.  Specifically,
		// it doesn't catch an invalid value of matchExpressions.operator.
		// With this work-around we check early if our kata node selector
		// is workable and bail out before making any changes to the
		// cluster if it turns out it isn't.
		_, err := r.getKataConfigNodeSelectorAsSelector()
		if err != nil {
			r.Log.Info("Invalid KataConfig.spec.kataConfigPoolSelector - please fix your KataConfig", "err", err)
			return ctrl.Result{}, nil
		}

		// Check if the KataConfig instance is marked to be deleted, which is
		// indicated by the deletion timestamp being set.  However, don't let
		// uninstallation commence if another operation (installation, update)
		// is underway.
		if r.kataConfig.GetDeletionTimestamp() != nil && !r.isInstalling() && !r.isUpdating() {
			var res ctrl.Result

			switch r.DeploymentMode {
			case MachineConfigMode:
				res, err = r.processKataConfigDeleteRequest()
			case DaemonSetMode:
				res, err = r.processKataConfigDeleteRequestDaemonSet()
			default:
				res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
				err = fmt.Errorf("unknown deployment mode: %d", r.DeploymentMode)
			}

			updateErr := r.Client.Status().Update(context.TODO(), r.kataConfig)
			// The finalizer test is to get rid of the
			// "Operation cannot be fulfilled [...] Precondition failed"
			// error which happens when returning from a reconciliation that
			// deleted our KataConfig by removing its finalizer.  So if the
			// finalizer is missing the actual KataConfig object is probably
			// already gone from the cluster, hence the error.
			if updateErr != nil && controllerutil.ContainsFinalizer(r.kataConfig, kataConfigFinalizer) {
				r.Log.Info("Updating KataConfig failed", "err", updateErr)
				return ctrl.Result{}, updateErr
			}
			return res, err
		}

		var res ctrl.Result

		switch r.DeploymentMode {
		case MachineConfigMode:
			res, err = r.processKataConfigInstallRequest()
		case DaemonSetMode:
			res, err = r.processKataConfigInstallRequestDaemonSet()
		default:
			res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
			err = fmt.Errorf("unknown deployment mode: %d", r.DeploymentMode)
		}

		if err != nil {
			return res, err
		}

		if r.IsKataConfigStatusChanged(oldObjStatus, &r.kataConfig.Status) {
			r.Log.Info("KataConfig's status changed, updating...")
			updateErr := r.Client.Status().Update(context.TODO(), r.kataConfig)
			if updateErr != nil {
				return ctrl.Result{}, updateErr
			}
		}

		cMap := r.processDashboardConfigMap()
		if cMap == nil {
			r.Log.Info("failed to generate config map for metrics dashboard")
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, nil
		}
		foundCm := &corev1.ConfigMap{}
		err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cMap.Name, Namespace: cMap.Namespace}, foundCm)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				r.Log.Info("Installing metrics dashboard")
				err = r.Client.Create(context.TODO(), cMap)
				if err != nil {
					r.Log.Error(err, "Error when creating the dashboard configmap")
					res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
				}
			} else {
				r.Log.Error(err, "could not get dashboard info, try again")
				res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
			}
		}

		// FIXME: Ideally, we should have a processLogLevelDaemonSet() function,
		// or alternatively, call processLogLevel() from a location that isn't MCO-specific.
		// Currently, log level is handled by the KataInstallDaemonSet in DaemonSetMode.
		if r.DeploymentMode == MachineConfigMode {
			err = r.processLogLevel(r.kataConfig.Spec.LogLevel)
			if err != nil {
				res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
			}
		}

		return res, err
	}()
}

func (r *KataConfigOpenShiftReconciler) IsKataConfigStatusChanged(oldStatus, newStatus *kataconfigurationv1.KataConfigStatus) bool {
	oldStatusCopy := oldStatus.DeepCopy()
	newStatusCopy := newStatus.DeepCopy()

	for i := range oldStatusCopy.Conditions {
		oldStatusCopy.Conditions[i].LastTransitionTime = metav1.Time{}
	}
	for i := range newStatusCopy.Conditions {
		newStatusCopy.Conditions[i].LastTransitionTime = metav1.Time{}
	}

	return !reflect.DeepEqual(oldStatusCopy, newStatusCopy)
}

func makeContainerRuntimeConfig(desiredLogLevel string, mcpSelector *metav1.LabelSelector) *mcfgv1.ContainerRuntimeConfig {
	return &mcfgv1.ContainerRuntimeConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "machineconfiguration.openshift.io/v1",
			Kind:       "ContainerRuntimeConfig",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: container_runtime_config_name,
		},
		Spec: mcfgv1.ContainerRuntimeConfigSpec{
			MachineConfigPoolSelector: mcpSelector,
			ContainerRuntimeConfig: &mcfgv1.ContainerRuntimeConfiguration{
				LogLevel: desiredLogLevel,
			},
		},
	}
}

func (r *KataConfigOpenShiftReconciler) processLogLevel(desiredLogLevel string) error {

	if desiredLogLevel == "" {
		r.Log.Info("desired logLevel value is empty, setting to default ('info')")
		desiredLogLevel = "info"
	}

	ctrRuntimeCfg := &mcfgv1.ContainerRuntimeConfig{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: container_runtime_config_name}, ctrRuntimeCfg)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			r.Log.Error(err, "could not get ContainerRuntimeConfig, try again")
			return err
		}

		r.Log.Info("no existing ContainerRuntimeConfig found")

		if desiredLogLevel == "info" {
			// if there's no ContainerRuntimeConfig - meaning that logLevel
			// wasn't set yet and thus is at the default value in the cluster -
			// *and* the desired value is the default one as well, there's
			// nothing to do
			r.Log.Info("current and desired logLevel values are both default, no action necessary")
			return nil
		}

		machineConfigPoolSelectorLabels := map[string]string{"pools.operator.machineconfiguration.openshift.io/kata-oc": ""}
		isConvergedCluster, err := r.checkConvergedCluster()
		if isConvergedCluster && err == nil {
			machineConfigPoolSelectorLabels = map[string]string{"pools.operator.machineconfiguration.openshift.io/master": ""}
		}

		machineConfigPoolSelector := &metav1.LabelSelector{
			MatchLabels: machineConfigPoolSelectorLabels,
		}

		ctrRuntimeCfg = makeContainerRuntimeConfig(desiredLogLevel, machineConfigPoolSelector)

		r.Log.Info("creating ContainerRuntimeConfig")
		err = r.Client.Create(context.TODO(), ctrRuntimeCfg)
		if err != nil {
			r.Log.Error(err, "error creating ContainerRuntimeConfig")
			return err
		}
		r.Log.Info("ContainerRuntimeConfig created successfully")
	} else {
		r.Log.Info("existing ContainerRuntimeConfig found")
		if ctrRuntimeCfg.Spec.ContainerRuntimeConfig.LogLevel == desiredLogLevel {
			r.Log.Info("existing ContainerRuntimeConfig is up-to-date, no action necessary")
			return nil
		}
		// We only update LogLevel and don't touch MachineConfigPoolSelector
		// as that shouldn't be necessary.  It selects an MCP based only on
		// whether the cluster is converged or not.  Assuming that being
		// converged is an immutable property of any given cluster, the initial
		// choice of MachineConfigPoolSelector value should always be valid.
		ctrRuntimeCfg.Spec.ContainerRuntimeConfig.LogLevel = desiredLogLevel

		r.Log.Info("updating ContainerRuntimeConfig")
		err = r.Client.Update(context.TODO(), ctrRuntimeCfg)
		if err != nil {
			r.Log.Error(err, "error updating ContainerRuntimeConfig")
			return err
		}
		r.Log.Info("ContainerRuntimeConfig updated successfully")
	}

	return nil
}

func (r *KataConfigOpenShiftReconciler) removeLogLevel() error {

	r.Log.Info("removing logLevel ContainerRuntimeConfig")

	ctrRuntimeCfg := &mcfgv1.ContainerRuntimeConfig{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: container_runtime_config_name}, ctrRuntimeCfg)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("no logLevel ContainerRuntimeConfig found, nothing to do")
			return nil
		} else {
			r.Log.Info("could not get ContainerRuntimeConfig", "err", err)
			return err
		}
	}

	err = r.Client.Delete(context.TODO(), ctrRuntimeCfg)
	if err != nil {
		r.Log.Info("error deleting ContainerRuntimeConfig", "err", err)
		return err
	}
	r.Log.Info("logLevel ContainerRuntimeConfig deleted successfully")
	return nil
}

func (r *KataConfigOpenShiftReconciler) processDaemonsetForMonitor() *appsv1.DaemonSet {
	var (
		runPrivileged = false
		runUserID     = int64(1001)
		runGroupID    = int64(1001)
	)

	kataMonitorImage := os.Getenv("RELATED_IMAGE_KATA_MONITOR")
	if len(kataMonitorImage) == 0 {
		// kata-monitor image URL is generally impossible to verify or sanitise,
		// with the empty value being pretty much the only exception where it's
		// fairly clear what good it is.  If we can only detect a single one
		// out of an infinite number of bad values, we choose not to return an
		// error here (giving an impression that we can actually detect errors)
		// but just log this incident and plow ahead.
		r.Log.Info("RELATED_IMAGE_KATA_MONITOR env var is unset or empty, kata-monitor pods will not run")
	}

	r.Log.Info("Creating monitor DaemonSet with image file: \"" + kataMonitorImage + "\"")
	dsName := "openshift-sandboxed-containers-monitor"
	dsLabels := map[string]string{
		"name": dsName,
	}

	nodeSelector := r.getNodeSelectorAsMap()

	return &appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "DaemonSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dsName,
			Namespace: OperatorNamespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: dsLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: dsLabels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "monitor",
					NodeSelector:       nodeSelector,
					Tolerations: []corev1.Toleration{
						{
							Operator: corev1.TolerationOpExists,
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "kata-monitor",
							Image:           kataMonitorImage,
							ImagePullPolicy: "Always",
							SecurityContext: &corev1.SecurityContext{
								Privileged: &runPrivileged,
								RunAsUser:  &runUserID,
								RunAsGroup: &runGroupID,
								SELinuxOptions: &corev1.SELinuxOptions{
									Type: "osc_monitor.process",
								},
							},
							Command: []string{"/usr/bin/kata-monitor", "--listen-address=:8090", "--log-level=debug", "--runtime-endpoint=/run/crio/crio.sock"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "crio-sock",
									MountPath: "/run/crio/",
								},
								{
									Name:      "sbs",
									MountPath: "/run/vc/sbs/",
								}},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "crio-sock",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/run/crio/",
								},
							},
						},
						{
							Name: "sbs",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/run/vc/sbs/",
								},
							},
						},
					},
				},
			},
		},
	}
}

func (r *KataConfigOpenShiftReconciler) processDashboardConfigMap() *corev1.ConfigMap {

	r.Log.Info("Creating sandboxed containers dashboard in the OpenShift console")
	cmLabels := map[string]string{
		"console.openshift.io/dashboard": "true",
	}

	// retrieve content of the dashboard from our own namespace
	foundCm := &corev1.ConfigMap{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: dashboardConfigMapName, Namespace: OperatorNamespace}, foundCm)
	if err != nil {
		r.Log.Error(err, "could not get dashboard data")
		return nil
	}

	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      dashboardConfigMapName,
			Namespace: DashboardConfigMapNamespace,
			Labels:    cmLabels,
		},
		Data: foundCm.Data,
	}
}

func (r *KataConfigOpenShiftReconciler) newMCPforCR() *mcfgv1.MachineConfigPool {
	lsr := metav1.LabelSelectorRequirement{
		Key:      "machineconfiguration.openshift.io/role",
		Operator: metav1.LabelSelectorOpIn,
		Values:   []string{"kata-oc", "worker"},
	}

	mcp := &mcfgv1.MachineConfigPool{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "machineconfiguration.openshift.io/v1",
			Kind:       "MachineConfigPool",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "kata-oc",
			Labels: map[string]string{
				// This label is added to make it possible to form a label
				// selector that selects this MCP.  One use case is the
				// ContainerRuntimeConfig resource which selects MCPs based
				// on labels and is used to implement KataConfig.spec.logLevel
				// handling.
				"pools.operator.machineconfiguration.openshift.io/kata-oc": "",
			},
		},

		Spec: mcfgv1.MachineConfigPoolSpec{
			MachineConfigSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{lsr},
			},
			NodeSelector: r.getNodeSelectorAsLabelSelector(),
		},
	}

	return mcp
}

func (r *KataConfigOpenShiftReconciler) getExtensionName() (string, error) {
	// RHCOS uses "sandboxed-containers" as thats resolved/translated in the machine-config-operator to "kata-containers"
	// FCOS/SCOS however does not get any translation in the machine-config-operator so we need to
	// send in "kata-containers".
	// Both are later send to rpm-ostree for installation.
	//
	extension := os.Getenv("SANDBOXED_CONTAINERS_EXTENSION")
	if len(extension) != 0 {
		return extension, nil
	}

	// FIXME: Look into having a single util function to return the ClusterVersion
	clusterVersion := &configv1.ClusterVersion{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: "version"}, clusterVersion)
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(clusterVersion.Status.Desired.Image, "quay.io/openshift-release-dev/ocp-release") {
		return "sandboxed-containers", nil // RHCOS
	}

	if strings.HasPrefix(clusterVersion.Status.Desired.Image, "quay.io/okd/scos-release") {
		return "kata-containers", nil // SCOS
	}

	cmdline, err := os.ReadFile("/proc/cmdline")
	if err == nil && strings.Contains(string(cmdline), "ostree/rhcos") {
		return "sandboxed-containers", nil // RHCOS
	}

	// As RHCOS is rather special variant, use "kata-containers" by default, which also applies to FCOS/SCOS
	return "kata-containers", nil
}

// getCustomKernelConfig retrieves the kata addon configuration from the "kata-addon-artifacts" ConfigMap in the operator namespace.
// This configuration contains the addon image reference and kernel path required for kata-se (IBM Secure Execution) deployments.
// NOTE: This logic is applicable only for kata-se / IBM Secure Execution (s390x).
func (r *KataConfigOpenShiftReconciler) getCustomKernelConfig(ctx context.Context) (*customKernelConfig, error) {
	cm := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, types.NamespacedName{
		Name:      KataAddonConfigMapName,
		Namespace: OperatorNamespace,
	}, cm)

	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("Skipping custom kernel addon, ConfigMap not found", "ConfigMap", KataAddonConfigMapName)
			return nil, nil
		}
		return nil, err
	}

	image := cm.Data["addonImage"]
	kernel := cm.Data["kernelPath"]

	if image == "" || kernel == "" {
		r.Log.Info("Skipping custom kernel addon, image or kernel not found in ConfigMap", "ConfigMap", KataAddonConfigMapName)
		return nil, nil
	}

	return &customKernelConfig{
		Image:      image,
		KernelPath: kernel,
	}, nil
}

func (r *KataConfigOpenShiftReconciler) newMCForCR(machinePool string, customKernelCfg *customKernelConfig) (*mcfgv1.MachineConfig, error) {
	r.Log.Info("Creating MachineConfig for Custom Resource")

	if r.ImgMc != nil {
		r.Log.Info("Image based MachineConfig", "MachineConfig", r.ImgMc)
		return r.ImgMc, nil
	}

	// Create extension MachineConfig
	ic := ignTypes.Config{
		Ignition: ignTypes.Ignition{
			Version: "3.2.0",
		},
	}

	if customKernelCfg != nil {
		mode := 0644

		configContent := fmt.Sprintf(
			"IMAGE=%s\nKERNEL=%s\n",
			customKernelCfg.Image,
			customKernelCfg.KernelPath,
		)

		source := "data:text/plain;base64," + base64.StdEncoding.EncodeToString([]byte(configContent))

		ic.Storage.Files = append(ic.Storage.Files, ignTypes.File{
			Node: ignTypes.Node{
				Path: "/etc/kata-containers/kata-addon-kernel.conf",
			},
			FileEmbedded1: ignTypes.FileEmbedded1{
				Contents: ignTypes.Resource{
					Source: &source,
				},
				Mode: &mode,
			},
		})
	}

	icb, err := json.Marshal(ic)
	if err != nil {
		return nil, err
	}

	extension, err := r.getExtensionName()
	if err != nil {
		return nil, err
	}

	mc := mcfgv1.MachineConfig{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "machineconfiguration.openshift.io/v1",
			Kind:       "MachineConfig",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: extension_mc_name,
			Labels: map[string]string{
				"machineconfiguration.openshift.io/role": machinePool,
				"app":                                    r.kataConfig.Name,
			},
			Namespace: OperatorNamespace,
		},
		Spec: mcfgv1.MachineConfigSpec{
			Extensions: []string{extension},
			Config: runtime.RawExtension{
				Raw: icb,
			},
		},
	}

	r.Log.Info("Extension based MachineConfig", "MachineConfig", mc)

	return &mc, nil
}

func (r *KataConfigOpenShiftReconciler) addFinalizer() error {
	r.Log.Info("Adding Finalizer for the KataConfig")
	controllerutil.AddFinalizer(r.kataConfig, kataConfigFinalizer)

	// Update CR
	err := r.Client.Update(context.TODO(), r.kataConfig)
	if err != nil {
		r.Log.Error(err, "Failed to update KataConfig with finalizer")
		return err
	}
	return nil
}

func (r *KataConfigOpenShiftReconciler) removeFinalizer() error {
	r.Log.Info("Removing finalizer from the KataConfig")
	controllerutil.RemoveFinalizer(r.kataConfig, kataConfigFinalizer)

	err := r.Client.Update(context.TODO(), r.kataConfig)
	if err != nil {
		r.Log.Error(err, "Unable to update KataConfig")
		return err
	}
	return nil
}

func (r *KataConfigOpenShiftReconciler) listKataPods() error {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(corev1.NamespaceAll),
	}
	if err := r.Client.List(context.TODO(), podList, listOpts...); err != nil {
		return fmt.Errorf("failed to list kata pods: %v", err)
	}
	for _, pod := range podList.Items {
		if pod.Spec.RuntimeClassName != nil {
			if contains(r.kataConfig.Status.RuntimeClasses, *pod.Spec.RuntimeClassName) {
				return fmt.Errorf("existing pods using \"%v\" RuntimeClass found. Please delete the pods manually for KataConfig deletion to proceed", *pod.Spec.RuntimeClassName)
			}
		}
	}
	return nil
}

//lint:ignore U1000 This method is unused, but let's keep it for now
func (r *KataConfigOpenShiftReconciler) kataOcExists() (bool, error) {
	kataOcMcp := &mcfgv1.MachineConfigPool{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: "kata-oc"}, kataOcMcp)
	if err != nil && k8serrors.IsNotFound(err) {
		r.Log.Info("kata-oc MachineConfigPool not found")
		return false, nil
	} else if err != nil {
		r.Log.Error(err, "Could not get the kata-oc MachineConfigPool")
		return false, err
	}

	return true, nil
}

func (r *KataConfigOpenShiftReconciler) checkConvergedCluster() (bool, error) {
	//Check if only master and worker MCP exists
	//Worker machinecount should be 0
	listOpts := []client.ListOption{}
	mcpList := &mcfgv1.MachineConfigPoolList{}
	err := r.Client.List(context.TODO(), mcpList, listOpts...)
	if err != nil {
		r.Log.Error(err, "Unable to get the list of MCPs")
		return false, err
	}

	numMcp := len(mcpList.Items)
	r.Log.Info("Number of MCPs", "numMcp", numMcp)
	if numMcp == 2 {
		for _, mcp := range mcpList.Items {
			if mcp.Name == "worker" && mcp.Status.MachineCount == 0 {
				r.Log.Info("Converged Cluster")
				return true, nil
			}
		}
	}

	return false, nil

}

func (r *KataConfigOpenShiftReconciler) checkNodeEligibility() error {
	r.Log.Info("Check Node Eligibility to run Kata containers")
	// Check if node eligibility label exists

	if r.kataConfig.Spec.EnablePeerPods {
		r.Log.Info("enablePeerPods is true. Skipping since they are mutually exclusive.")
		return nil
	}
	nodes, err := r.getNodesWithLabels(map[string]string{"feature.node.kubernetes.io/runtime.kata": "true"})
	if err != nil {
		r.Log.Error(err, "Error in getting list of nodes with label: feature.node.kubernetes.io/runtime.kata")
		return err
	}
	if len(nodes.Items) == 0 {
		err = fmt.Errorf("no Nodes with required labels found. Is NFD running?")
		return err
	}

	return nil
}

func (r *KataConfigOpenShiftReconciler) getMcpName() (string, error) {
	isConvergedCluster, err := r.checkConvergedCluster()
	if err != nil {
		r.Log.Info("Error trying to find out if cluster is converged", "err", err)
		return "", err
	}
	if isConvergedCluster {
		return "master", nil
	} else {
		return "kata-oc", nil
	}
}

func (r *KataConfigOpenShiftReconciler) createScc() error {

	scc := GetScc()
	// Set Kataconfig r.kataConfig as the owner and controller
	if err := controllerutil.SetControllerReference(r.kataConfig, scc, r.Scheme); err != nil {
		return err
	}

	foundScc := &secv1.SecurityContextConstraints{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: scc.Name}, foundScc)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return err
		}

		r.Log.Info("Creating a new Scc", "scc.Name", scc.Name)
		err = r.Client.Create(context.TODO(), scc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *KataConfigOpenShiftReconciler) createDaemonsetForMonitor() error {
	ds := r.processDaemonsetForMonitor()
	// Set KataConfig instance as the owner and controller
	if err := controllerutil.SetControllerReference(r.kataConfig, ds, r.Scheme); err != nil {
		r.Log.Error(err, "failed to set controller reference on the monitor daemonset")
		return err
	}
	r.Log.Info("controller reference set for the monitor daemonset")

	foundDS := &appsv1.DaemonSet{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: ds.Name, Namespace: ds.Namespace}, foundDS)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("Creating a new installation monitor daemonset", "ds.Namespace", ds.Namespace, "ds.Name", ds.Name)
			err = r.Client.Create(context.TODO(), ds)
			if err != nil {
				r.Log.Error(err, "error when creating monitor daemonset")
				return err
			}
		} else {
			r.Log.Error(err, "could not get monitor daemonset, try again")
			return err
		}
	} else {
		r.Log.Info("Updating monitor daemonset", "ds.Namespace", ds.Namespace, "ds.Name", ds.Name)
		err = r.Client.Update(context.TODO(), ds)
		if err != nil {
			r.Log.Error(err, "error when updating monitor daemonset")
			return err
		}
	}
	return nil
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

// "KataConfigNodeSelector" in the names of the following couple of helper
// functions refers to the value of KataConfig.spec.kataConfigPoolSelector,
// i.e. the original selector supplied by the user of KataConfig.
func (r *KataConfigOpenShiftReconciler) getKataConfigNodeSelectorAsLabelSelector() *metav1.LabelSelector {

	isConvergedCluster, err := r.checkConvergedCluster()
	if err == nil && isConvergedCluster {
		// master MCP cannot be customized
		return &metav1.LabelSelector{MatchLabels: map[string]string{"node-role.kubernetes.io/master": ""}}
	}

	nodeSelector := &metav1.LabelSelector{}
	if r.kataConfig.Spec.KataConfigPoolSelector != nil {
		nodeSelector = r.kataConfig.Spec.KataConfigPoolSelector.DeepCopy()
	}

	if r.kataConfig.Spec.CheckNodeEligibility {
		nodeSelector = metav1.AddLabelToSelector(nodeSelector, "feature.node.kubernetes.io/runtime.kata", "true")
	}
	r.Log.Info("getKataConfigNodeSelectorAsLabelSelector()", "selector", nodeSelector)
	return nodeSelector
}

func (r *KataConfigOpenShiftReconciler) getKataConfigNodeSelectorAsSelector() (labels.Selector, error) {
	selector, err := metav1.LabelSelectorAsSelector(r.getKataConfigNodeSelectorAsLabelSelector())
	r.Log.Info("getKataConfigNodeSelectorAsSelector()", "selector", selector, "err", err)
	return selector, err
}

// "NodeSelector" in the names of the following couple of helper
// functions refers to the selector we pass to resources we create that
// need to select kata-enabled nodes (currently the "kata-oc" MCP, the pod
// template in the monitor daemonset and the runtimeclass).  It's guaranteed
// to be a simple map[string]string (AKA MatchLabels) which is good because the
// pod template's and runtimeclass' node selectors don't support
// MatchExpressions and thus cannot hold the full value of
// KataConfig.spec.kataConfigPoolSelector.
func (r *KataConfigOpenShiftReconciler) getNodeSelectorAsMap() map[string]string {

	isConvergedCluster, err := r.checkConvergedCluster()
	if err == nil && isConvergedCluster {
		// master MCP cannot be customized
		return map[string]string{"node-role.kubernetes.io/master": ""}
	} else {
		return map[string]string{"node-role.kubernetes.io/kata-oc": ""}
	}
}

func (r *KataConfigOpenShiftReconciler) getNodeSelectorAsLabelSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: r.getNodeSelectorAsMap()}
}

func (r *KataConfigOpenShiftReconciler) isMcpUpdating(mcpName string) bool {
	mcp := &mcfgv1.MachineConfigPool{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: mcpName}, mcp)
	if err != nil {
		r.Log.Info("Getting MachineConfigPool failed ", "machinePool", mcpName, "err", err)
		return false
	}
	return apihelpers.IsMachineConfigPoolConditionTrue(mcp.Status.Conditions, mcfgv1.MachineConfigPoolUpdating)
}

func (r *KataConfigOpenShiftReconciler) processKataConfigDeleteRequest() (ctrl.Result, error) {
	r.Log.Info("KataConfig deletion in progress: ")
	machinePool, err := r.getMcpName()
	if err != nil {
		return reconcile.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
	}

	res, err := r.checkDeletionEligibility()
	if err != nil {
		return res, err
	}

	kataNodeSelector, err := r.getKataConfigNodeSelectorAsSelector()
	if err != nil {
		r.Log.Info("Couldn't get node selector for unlabelling nodes", "err", err)
		return ctrl.Result{Requeue: true}, nil
	}
	labelingChanged, err := r.unlabelNodes(kataNodeSelector)

	if err != nil {
		if k8serrors.IsConflict(err) {
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
		} else {
			return ctrl.Result{Requeue: true}, nil
		}
	}

	r.Log.Info("Making sure parent MCP is synced properly, SCNodeRole=" + machinePool)
	r.setInProgressConditionToUninstalling()

	mc, err := r.newMCForCR(machinePool, nil)
	if err != nil {
		return ctrl.Result{}, err
	}

	var isMcDeleted bool

	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: mc.Name}, mc)
	if err != nil && k8serrors.IsNotFound(err) {
		isMcDeleted = true
		// Reset ImgMc
		r.ImgMc = nil
	} else if err != nil {
		return ctrl.Result{}, err
	}

	if !isMcDeleted {
		err = r.Client.Delete(context.TODO(), mc)
		if err != nil {
			// error during removing mc, don't block the uninstall. Just log the error and move on.
			r.Log.Error(err, "Error found deleting machine config. If the machine config exists after installation it can be safely deleted manually.",
				"mc", mc.Name)
		}
	}

	isConvergedCluster, _ := r.checkConvergedCluster()

	// Conditions to detect whether we need to wait for the MCO to start
	// reconciliation differ based on whether the cluster is converged.
	// If so then it's the fact we've just deleted the extension MC, if not
	// then it's the node-role labeling change (if there's none it means
	// we're deleting a KataConfig on a cluster where no nodes matched the
	// kataConfigPoolSelector and thus there will be no change for the MCO
	// to reconciliate).
	if (isConvergedCluster && !isMcDeleted) || (!isConvergedCluster && labelingChanged) {
		r.Log.Info("Starting to wait for MCO to start reconciliation")
		r.kataConfig.Status.WaitingForMcoToStart = true
	}

	// When nodes migrate from a source pool to a target pool the source
	// pool is drained immediately and the nodes then slowly join the target
	// pool.  Thus the operation duration is dominated by the target pool
	// part and the target pool is what we need to watch to find out when
	// the operation is finished.  When uninstalling kata on a regular
	// cluster nodes leave "kata-oc" to rejoin "worker" so "worker" is our
	// target pool.  On a converged cluster, nodes leave "master" to rejoin
	// it so "master" is both source and target in this case.
	targetPool := "worker"
	if isConvergedCluster {
		targetPool = "master"
	}
	isMcoUpdating := r.isMcpUpdating(targetPool)

	if !isMcoUpdating && r.kataConfig.Status.WaitingForMcoToStart {
		r.Log.Info("Waiting for MCO to start updating.")
		// We don't requeue, an MCP going Updated->Updating will
		// trigger reconciliation by itself thanks to our watching MCPs.
		return reconcile.Result{}, nil
	} else {
		r.Log.Info("No need to wait for MCO to start updating.", "isMcoUpdating", isMcoUpdating, "Status.WaitingForMcoToStart", r.kataConfig.Status.WaitingForMcoToStart)
		r.kataConfig.Status.WaitingForMcoToStart = false
	}

	err = r.updateStatus()
	if err != nil {
		r.Log.Info("Error updating KataConfig.status", "err", err)
	}

	if isMcoUpdating {
		r.Log.Info("Waiting for MachineConfigPool to be fully updated", "machinePool", targetPool)
		return reconcile.Result{}, nil
	}

	r.resetInProgressCondition()

	if !isConvergedCluster {
		r.Log.Info("Get()'ing MachineConfigPool to delete it", "machinePool", "kata-oc")
		kataOcMcp := &mcfgv1.MachineConfigPool{}
		err = r.Client.Get(context.TODO(), types.NamespacedName{Name: "kata-oc"}, kataOcMcp)
		if err == nil {
			r.Log.Info("Deleting MachineConfigPool ", "machinePool", "kata-oc")
			err = r.Client.Delete(context.TODO(), kataOcMcp)
			if err != nil {
				r.Log.Error(err, "Unable to delete kata-oc MachineConfigPool")
				return ctrl.Result{}, err
			}
		} else if k8serrors.IsNotFound(err) {
			r.Log.Info("MachineConfigPool not found", "machinePool", "kata-oc")
		} else {
			r.Log.Error(err, "Unable to get MachineConfigPool ", "machinePool", "kata-oc")
			return ctrl.Result{}, err
		}
	}

	err = r.deleteDaemonsetForMonitor()
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 15}, err
	}

	if r.kataConfig.Spec.EnablePeerPods {
		res, err := r.disablePeerPods()
		if res != nil || err != nil {
			return *res, err
		}
	}

	err = r.deleteScc()
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
	}

	err = r.removeLogLevel()
	if err != nil {
		return ctrl.Result{Requeue: true}, nil
	}

	r.Log.Info("Uninstallation completed. Proceeding with the KataConfig deletion")
	if err = r.removeFinalizer(); err != nil {
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, nil
}

func (r *KataConfigOpenShiftReconciler) processKataConfigInstallRequest() (ctrl.Result, error) {
	r.Log.Info("Kata installation in progress")

	// Check Node Eligibility
	if r.kataConfig.Spec.CheckNodeEligibility {
		err := r.checkNodeEligibility()
		if err != nil {
			// If no nodes are found, requeue to check again for eligible nodes
			r.Log.Error(err, "Failed to check Node eligibility for running Kata containers")
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 20}, err
		}
	}

	// If converged cluster, then MCP == master, otherwise "kata-oc" if it exists
	machinePool, err := r.getMcpName()
	if err != nil {
		r.Log.Error(err, "Failed to get the MachineConfigPool name")
		return ctrl.Result{}, err
	}

	isConvergedCluster := machinePool == "master"

	// Add finalizer for this CR
	if !contains(r.kataConfig.GetFinalizers(), kataConfigFinalizer) {
		if err := r.addFinalizer(); err != nil {
			return ctrl.Result{}, err
		}
		r.Log.Info("SCNodeRole is: " + machinePool)
	}

	customKernelCfg, err := r.getCustomKernelConfig(context.TODO())
	if err != nil {
		return ctrl.Result{}, err
	}

	wasMcJustCreated, err := r.createMc(machinePool, customKernelCfg)
	if err != nil {
		return ctrl.Result{Requeue: true}, nil
	}

	if wasMcJustCreated {
		r.setInProgressConditionToInstalling()
	}

	// Create kata-oc MCP only if it's not a converged cluster
	if !isConvergedCluster {
		labelingChanged, err := r.updateNodeLabels()
		if err != nil {
			if k8serrors.IsConflict(err) {
				return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
			} else {
				return ctrl.Result{Requeue: true}, nil
			}
		}
		if labelingChanged {
			r.Log.Info("node labels updated")

			isInstallationInProgress := r.isMcpUpdating(machinePool)
			if !isInstallationInProgress {
				r.Log.Info("Starting to wait for MCO to start")
				r.kataConfig.Status.WaitingForMcoToStart = true
			} else {
				r.Log.Info("installation already in progress")
			}
		}

		// Create kata-oc only if it doesn't exist
		mcp := &mcfgv1.MachineConfigPool{}
		err = r.Client.Get(context.TODO(), types.NamespacedName{Name: machinePool}, mcp)
		if err != nil && k8serrors.IsNotFound(err) {
			r.Log.Info("Creating a new MachineConfigPool ", "machinePool", machinePool)
			mcp = r.newMCPforCR()
			err = r.Client.Create(context.TODO(), mcp)
			if err != nil {
				r.Log.Error(err, "Error in creating new MachineConfigPool ", "machinePool", machinePool)
				return ctrl.Result{}, err
			}
			// Don't requeue - the MCP creation will send us
			// a reconcile request via our MCP watching so we should be
			// guaranteed to run again at due time even without requeueing.
		} else if err != nil {
			r.Log.Error(err, "Error in retreiving MachineConfigPool ", "machinePool", machinePool)
			return ctrl.Result{}, err
		}
	} else {
		if wasMcJustCreated {
			r.kataConfig.Status.WaitingForMcoToStart = true
		}
	}

	isMcoUpdating := r.isMcpUpdating(machinePool)
	r.Log.Info("MCP updating state", "MCP name", machinePool, "is updating", isMcoUpdating)

	if isMcoUpdating && r.getInProgressConditionValue() == corev1.ConditionFalse {
		r.setInProgressConditionToUpdating()
	}

	// This condition might look tricky so here's a quick rundown of
	// what each possible state means:
	// - isMcoUpdating && WaitingForMcoToStart:
	//     We've just finished waiting for the MCO to start updating.
	//     The MCO is updating already but "Waiting" is still 'true'
	//     (it will be set to 'false' shortly).
	// - !isMcoUpdating && WaitingForMcoToStart:
	//     We're waiting for the MCO to pick up our recent changes and
	//     start updating.
	// - isMcoUpdating && !WaitingForMcoToStart:
	//     The MCO is updating (it hasn't yet finished processing our
	//     recent changes).
	// - !isMcoUpdating && !WaitingForMcoToStart:
	//     The MCO isn't updating nor do we think it should be.  This is
	//     the case e.g. when we're reconciliating a KataConfig change
	//     that doesn't affect kata installation on cluster.
	if !isMcoUpdating && r.kataConfig.Status.WaitingForMcoToStart {
		r.Log.Info("Waiting for MCO to start updating.")
		// We don't requeue, an MCP going Updated->Updating will
		// trigger reconciliation by itself thanks to our watching MCPs.
		return reconcile.Result{}, nil
	} else {
		r.Log.Info("No need to wait for MCO to start updating.", "isMcoUpdating", isMcoUpdating, "Status.WaitingForMcoToStart", r.kataConfig.Status.WaitingForMcoToStart)
		r.kataConfig.Status.WaitingForMcoToStart = false
	}

	err = r.updateStatus()
	if err != nil {
		r.Log.Info("Error updating KataConfig.status", "err", err)
	}

	if !isMcoUpdating {
		res, err := r.postKataInstallation()
		if res != nil {
			return *res, err
		}
		// FIXME : dead code, revisit postKataInstallation() return paths
		if err != nil {
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}
	} else {
		// We don't requeue - we're waiting for an MCP to go
		// Updating->Updated which will trigger reconciliation
		// by itself thanks to our watching MCPs.
		r.Log.Info("Waiting for MachineConfigPool to be fully updated", "machinePool", machinePool)
	}
	return ctrl.Result{}, nil
}

// If the first return value is 'true' it means that the MC was just created
// by this call, 'false' means that it's already existed.  As usual, the first
// return value is only valid if the second one is nil.
func (r *KataConfigOpenShiftReconciler) createMc(machinePool string, customKernelCfg *customKernelConfig) (bool, error) {

	// In case we're returning an error we want to make it explicit that
	// the first return value is "not care".  Unfortunately golang seems
	// to lack syntax for creating an expression with default bool value
	// hence this work-around.
	var dummy bool

	/* Create Machine Config object to install sandboxed containers */

	r.Log.Info("creating RHCOS MachineConfig")
	mc, err := r.newMCForCR(machinePool, customKernelCfg)
	if err != nil {
		return dummy, err
	}

	existingMc := &mcfgv1.MachineConfig{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: mc.Name}, existingMc)
	if err != nil && (k8serrors.IsNotFound(err) || k8serrors.IsGone(err)) {

		err = r.Client.Create(context.TODO(), mc)
		if err != nil {
			r.Log.Error(err, "Failed to create a new MachineConfig ", "mc.Name", mc.Name)
			return dummy, err
		}
		r.Log.Info("MachineConfig successfully created", "mc.Name", mc.Name)
		return true, nil
	} else if err != nil {
		r.Log.Info("failed to retrieve MachineConfig", "err", err)
		return dummy, err
	} else if !reflect.DeepEqual(existingMc.Spec, mc.Spec) {
		r.Log.Info("MachineConfig spec changed, updating", "mc.Name", mc.Name)
		existingMc.Spec = mc.Spec
		if err := r.Client.Update(context.TODO(), existingMc); err != nil {
			r.Log.Error(err, "Failed to update MachineConfig", "mc.Name", mc.Name)
			return dummy, err
		}
		return false, nil
	} else {
		r.Log.Info("MachineConfig already exists")
		return false, nil
	}

}

func (r *KataConfigOpenShiftReconciler) makeReconcileRequest() reconcile.Request {
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name: r.kataConfig.Name,
		},
	}
}

func (r *KataConfigOpenShiftReconciler) nodeMatchesKataSelector(nodeLabels map[string]string) bool {
	nodeSelector, err := r.getKataConfigNodeSelectorAsSelector()

	if err != nil {
		r.Log.Info("couldn't get kata node selector", "err", err)
		// If we cannot find out whether a Node matches assuming that it
		// doesn't seems to be the safer assumption.  This also seems
		// consistent with error-handling semantics of earlier similar code.
		return false
	}

	return nodeSelector.Matches(labels.Set(nodeLabels))
}

func (r *KataConfigOpenShiftReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kataconfigurationv1.KataConfig{}).
		Watches(
			&mcfgv1.MachineConfigPool{},
			&McpEventHandler{r}).
		Watches(
			&corev1.Node{},
			&NodeEventHandler{r}).
		Watches(
			&corev1.ConfigMap{},
			&ConfigMapEventHandler{r},
		).Complete(r)
}

func (r *KataConfigOpenShiftReconciler) getNodes() (*corev1.NodeList, error) {
	nodes := &corev1.NodeList{}
	labelSelector := labels.SelectorFromSet(map[string]string{"node-role.kubernetes.io/worker": ""})
	listOpts := []client.ListOption{
		client.MatchingLabelsSelector{Selector: labelSelector},
	}

	if err := r.Client.List(context.TODO(), nodes, listOpts...); err != nil {
		r.Log.Error(err, "Getting list of nodes failed")
		return &corev1.NodeList{}, err
	}
	return nodes, nil
}

func (r *KataConfigOpenShiftReconciler) getNodesWithLabels(nodeLabels map[string]string) (*corev1.NodeList, error) {
	nodes := &corev1.NodeList{}
	labelSelector := labels.SelectorFromSet(nodeLabels)
	listOpts := []client.ListOption{
		client.MatchingLabelsSelector{Selector: labelSelector},
	}

	if err := r.Client.List(context.TODO(), nodes, listOpts...); err != nil {
		r.Log.Error(err, "Getting list of nodes having specified labels failed")
		return &corev1.NodeList{}, err
	}
	return nodes, nil
}

func (r *KataConfigOpenShiftReconciler) updateNodeLabels() (labelingChanged bool, err error) {
	workerNodeList := &corev1.NodeList{}
	workerSelector := labels.SelectorFromSet(map[string]string{"node-role.kubernetes.io/worker": ""})
	listOpts := []client.ListOption{
		client.MatchingLabelsSelector{Selector: workerSelector},
	}

	if err := r.Client.List(context.TODO(), workerNodeList, listOpts...); err != nil {
		r.Log.Error(err, "Getting list of nodes failed")
		return false, err
	}

	kataNodeSelector, err := r.getKataConfigNodeSelectorAsSelector()
	if err != nil {
		r.Log.Info("Couldn't getKataConfigNodeSelectorAsSelector()", "err", err)
		return false, err
	}

	for _, worker := range workerNodeList.Items {
		workerMatchesKata := kataNodeSelector.Matches(labels.Set(worker.Labels))
		_, workerLabeledForKata := worker.Labels["node-role.kubernetes.io/kata-oc"]

		isLabelUpToDate := (workerMatchesKata && workerLabeledForKata) || (!workerMatchesKata && !workerLabeledForKata)

		if isLabelUpToDate {
			continue
		}

		if workerMatchesKata && !workerLabeledForKata {
			r.Log.Info("worker labeled", "node", worker.GetName())
			worker.Labels["node-role.kubernetes.io/kata-oc"] = ""
		} else if !workerMatchesKata && workerLabeledForKata {
			r.Log.Info("worker unlabeled", "node", worker.GetName())
			delete(worker.Labels, "node-role.kubernetes.io/kata-oc")
		}

		err = r.Client.Update(context.TODO(), &worker)
		if err != nil {
			r.Log.Error(err, "Error when adding labels to node", "node", worker)
			return labelingChanged, err
		}

		labelingChanged = true
	}

	return labelingChanged, nil
}

func (r *KataConfigOpenShiftReconciler) unlabelNodes(nodeSelector labels.Selector) (labelingChanged bool, err error) {
	nodeList := &corev1.NodeList{}
	listOpts := []client.ListOption{
		client.MatchingLabelsSelector{Selector: nodeSelector},
	}

	if err := r.Client.List(context.TODO(), nodeList, listOpts...); err != nil {
		r.Log.Error(err, "Getting list of nodes failed")
		return false, err
	}

	for _, node := range nodeList.Items {
		if _, ok := node.Labels["node-role.kubernetes.io/kata-oc"]; ok {
			delete(node.Labels, "node-role.kubernetes.io/kata-oc")
			err = r.Client.Update(context.TODO(), &node)
			if err != nil {
				r.Log.Error(err, "Error when removing labels from node", "node", node)
				return labelingChanged, err
			}
			labelingChanged = true
		}
	}
	return labelingChanged, nil
}

//lint:ignore U1000 This method is unused, but let's keep it for now
func (r *KataConfigOpenShiftReconciler) getConditionReason(conditions []mcfgv1.MachineConfigPoolCondition, conditionType mcfgv1.MachineConfigPoolConditionType) string {
	for _, c := range conditions {
		if c.Type == conditionType {
			return c.Message
		}
	}

	return ""
}

func (r *KataConfigOpenShiftReconciler) isInstalling() bool {
	cond := r.findInProgressCondition()
	if cond == nil {
		return false
	}
	return cond.Status == corev1.ConditionTrue && cond.Reason == "Installing"
}

func (r *KataConfigOpenShiftReconciler) isUpdating() bool {
	cond := r.findInProgressCondition()
	if cond == nil {
		return false
	}
	return cond.Status == corev1.ConditionTrue && cond.Reason == "Updating"
}

func (r *KataConfigOpenShiftReconciler) checkDeletionEligibility() (ctrl.Result, error) {
	if contains(r.kataConfig.GetFinalizers(), kataConfigFinalizer) {
		// Get the list of pods that might be running using kata runtime
		err := r.listKataPods()
		if err != nil {
			r.setInProgressConditionToBlockedByExistingKataPods(err.Error())
			updErr := r.Client.Status().Update(context.TODO(), r.kataConfig)
			if updErr != nil {
				return ctrl.Result{}, updErr
			}
			r.Log.Info("Kata pods are present. Requeue for reconciliation ")
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *KataConfigOpenShiftReconciler) deleteDaemonsetForMonitor() error {
	ds := r.processDaemonsetForMonitor()
	err := r.Client.Delete(context.TODO(), ds)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("monitor daemonset was already deleted")
		} else {
			r.Log.Error(err, "error when deleting monitor Daemonset, try again")
			return err
		}
	}
	return nil
}

func (r *KataConfigOpenShiftReconciler) deleteScc() error {
	scc := GetScc()
	err := r.Client.Delete(context.TODO(), scc)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("SCC was already deleted")
		} else {
			r.Log.Error(err, "error when deleting SCC, retrying")
			return err
		}
	}
	return nil
}

func (r *KataConfigOpenShiftReconciler) postKataInstallation() (*ctrl.Result, error) {
	r.Log.Info("create runtime class")
	r.resetInProgressCondition()

	// creating kata runtime class if node labels exist
	err := r.createRuntimeClass(
		kataRuntimeClassName,
		kataRuntimeClassCpuOverhead,
		kataRuntimeClassMemOverhead,
		"",                   /* nil extended resource overhead */
		kataRuntimeClassName, /* reused for handler */
		map[string]string{
			"feature.node.kubernetes.io/runtime.kata": "true",
		})
	if err != nil {
		return &ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
	}

	// creating kata-nvidia-gpu runtime class if node labels exist
	err = r.createRuntimeClass(
		kataNvidiaGPURuntimeClassName,
		kataNvidiaGPURuntimeClassCpuOverhead,
		kataNvidiaGPURuntimeClassMemOverhead,
		"",                            /* nil extended resource overhead */
		kataNvidiaGPURuntimeClassName, /* reused for handler */
		nvidiaGPUNodeLabels)

	if err != nil {
		return &ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
	}

	r.Log.Info("create Scc")
	err = r.createScc()
	if err != nil {
		// Give sometime for the error to go away before reconciling again
		return &ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
	}

	err = r.createDaemonsetForMonitor()
	if err != nil {
		return &ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
	}

	// create Pod VM image CRD and runtimeclass for peerpods
	if r.kataConfig.Spec.EnablePeerPods {
		res, err := r.enablePeerPods()
		if res != nil || err != nil {
			return res, err
		}
	}
	return nil, nil
}
