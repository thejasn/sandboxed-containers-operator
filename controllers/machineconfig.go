package controllers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	ignTypes "github.com/coreos/ignition/v2/config/v3_2/types"
	configv1 "github.com/openshift/api/config/v1"
	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type customKernelConfig struct {
	Image      string
	KernelPath string
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
