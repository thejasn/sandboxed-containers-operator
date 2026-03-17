package controllers

import (
	"context"

	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	"github.com/openshift/machine-config-operator/pkg/apihelpers"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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

func (r *KataConfigOpenShiftReconciler) isMcpUpdating(mcpName string) bool {
	mcp := &mcfgv1.MachineConfigPool{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: mcpName}, mcp)
	if err != nil {
		r.Log.Info("Getting MachineConfigPool failed ", "machinePool", mcpName, "err", err)
		return false
	}
	return apihelpers.IsMachineConfigPoolConditionTrue(mcp.Status.Conditions, mcfgv1.MachineConfigPoolUpdating)
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
