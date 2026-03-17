package controllers

import (
	"context"
	"fmt"

	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	mcfgconsts "github.com/openshift/machine-config-operator/pkg/daemon/constants"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// "Working"
	NodeWorking = mcfgconsts.MachineConfigDaemonStateWorking
	// "Done"
	NodeDone = mcfgconsts.MachineConfigDaemonStateDone
	// "Degraded"
	NodeDegraded = mcfgconsts.MachineConfigDaemonStateDegraded
)

func (r *KataConfigOpenShiftReconciler) getMcpByName(mcpName string) (*mcfgv1.MachineConfigPool, error) {

	mcp := &mcfgv1.MachineConfigPool{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: mcpName}, mcp)
	if err != nil {
		r.Log.Info("Getting MachineConfigPool failed ", "machinePool", mcp, "err", err)
		return nil, err
	}

	return mcp, nil
}

// If multiple errors occur during execution of this function the last one
// will be returned.
func (r *KataConfigOpenShiftReconciler) updateStatus() error {

	nodeList, err := r.getNodes()
	if err != nil {
		return err
	}

	r.clearNodeStatusLists()

	r.kataConfig.Status.KataNodes.NodeCount = func() int {
		nodes, err := r.getNodesWithLabels(r.getNodeSelectorAsMap())
		if err != nil {
			r.Log.Info("Error retrieving kata-oc labelled Nodes to count them", "err", err)
			return 0
		}
		return len(nodes.Items)
	}()

	for _, node := range nodeList.Items {
		e := r.putNodeOnStatusList(&node)
		if e != nil {
			err = e
		}
	}

	r.kataConfig.Status.KataNodes.ReadyNodeCount = len(r.kataConfig.Status.KataNodes.Installed)

	return err
}

// A set of mutually exclusive predicate functions that figure out kata
// installation status on a given Node from four pieces of data:
// - Node's MCO state
// - MachineConfig the Node is currently at
// - MachineConfig the Node is supposed to be at
// - and whether kata is enabled for the Node
func isNodeInstalled(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeDone && nodeCurrMc == nodeTargetMc && isKataEnabledOnNode
}

func isNodeNotInstalled(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeDone && nodeCurrMc == nodeTargetMc && !isKataEnabledOnNode
}

func isNodeInstalling(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeWorking && isKataEnabledOnNode
}

func isNodeUninstalling(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeWorking && !isKataEnabledOnNode
}

func isNodeWaitingToInstall(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeDone && nodeCurrMc != nodeTargetMc && isKataEnabledOnNode
}

func isNodeWaitingToUninstall(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeDone && nodeCurrMc != nodeTargetMc && !isKataEnabledOnNode
}

func isNodeFailedToInstall(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeDegraded && isKataEnabledOnNode
}

func isNodeFailedToUninstall(nodeMcoState string, nodeCurrMc string, nodeTargetMc string, isKataEnabledOnNode bool) bool {
	return nodeMcoState == NodeDegraded && !isKataEnabledOnNode
}

func (r *KataConfigOpenShiftReconciler) putNodeOnStatusList(node *corev1.Node) error {

	isConvergedCluster, err := r.checkConvergedCluster()
	if err != nil {
		return err
	}

	targetMcpName := func() string {
		if isConvergedCluster {
			return "master"
		}
		_, nodeLabeledForKata := node.Labels["node-role.kubernetes.io/kata-oc"]
		if nodeLabeledForKata {
			return "kata-oc"
		} else {
			return "worker"
		}
	}()

	targetMcp, err := r.getMcpByName(targetMcpName)
	if err != nil {
		return err
	}

	nodeMcoState, ok := node.Annotations["machineconfiguration.openshift.io/state"]
	if !ok {
		return fmt.Errorf("missing machineconfiguration.openshift.io/state on node %v", node.GetName())
	}

	nodeCurrMc, ok := node.Annotations["machineconfiguration.openshift.io/currentConfig"]
	if !ok {
		return fmt.Errorf("missing machineconfiguration.openshift.io/currentConfig on node %v", node.GetName())
	}

	// Note that to figure out the MachineConfig our Node should be at we
	// unfortunately cannot use
	// machineconfiguration.openshift.io/desiredConfig as would seem
	// logical and easy.  The reason is that the MCO only sets
	// `desiredConfig` to the actual desired config right before it starts
	// updating the Node.  So to get the correct target MachineConfig we
	// need to look at the MachineConfigPool that our Node belongs to or
	// will belong to shortly.
	nodeTargetMc := targetMcp.Spec.Configuration.Name

	// `isKataEnabledOnNode` is a per Node condition on regular clusters
	// but cluster-wide on converged ones.
	// On regular clusters, this is ultimately determined by
	// KataConfig.spec.kataConfigPoolSelector (we use the
	// node-role.kubernetes.io/kata-oc to find this above in this function,
	// and the node-role is in turn assigned to Nodes based on the pool
	// selector).
	// On converged clusters, basically only two operations are possible:
	// installing kata on all masters and uninstalling kata from all
	// masters, no per-Node options can be supported.  We find if kata is
	// supposed to be installed on the cluster by examining the "master"
	// MCP's MachineConfig to see if it installs the kata containers
	// extension.
	var isKataEnabledOnNode bool
	if isConvergedCluster {
		targetMc := &mcfgv1.MachineConfig{}
		err := r.Client.Get(context.TODO(), types.NamespacedName{Name: targetMcp.Spec.Configuration.Name}, targetMc)
		if err != nil {
			r.Log.Info("Failed to retrieve MachineConfig", "MC name", targetMcp.Spec.Configuration.Name, targetMc, "MCP name", targetMcpName)
			return err
		}

		isKataEnabledOnNode, err = func() (bool, error) {
			extensionName, err := r.getExtensionName()
			if err != nil {
				return false, err
			}
			for _, extName := range targetMc.Spec.Extensions {
				if extName == extensionName {
					return true, nil
				}
			}
			return false, nil
		}()
		if err != nil {
			return err
		}
	} else {
		isKataEnabledOnNode = targetMcpName == "kata-oc"
	}

	if isNodeInstalled(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is Installed", "node", node.GetName())
		r.kataConfig.Status.KataNodes.Installed = append(r.kataConfig.Status.KataNodes.Installed, node.GetName())
	} else if isNodeNotInstalled(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is NotInstalled", "node", node.GetName())
	} else if isNodeInstalling(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is Installing", "node", node.GetName())
		r.kataConfig.Status.KataNodes.Installing = append(r.kataConfig.Status.KataNodes.Installing, node.GetName())
	} else if isNodeUninstalling(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is Uninstalling", "node", node.GetName())
		r.kataConfig.Status.KataNodes.Uninstalling = append(r.kataConfig.Status.KataNodes.Uninstalling, node.GetName())
	} else if isNodeWaitingToInstall(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is WaitingToInstall", "node", node.GetName())
		r.kataConfig.Status.KataNodes.WaitingToInstall = append(r.kataConfig.Status.KataNodes.WaitingToInstall, node.GetName())
	} else if isNodeWaitingToUninstall(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is WaitingToUninstall", "node", node.GetName())
		r.kataConfig.Status.KataNodes.WaitingToUninstall = append(r.kataConfig.Status.KataNodes.WaitingToUninstall, node.GetName())
	} else if isNodeFailedToInstall(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is FailedToInstall", "node", node.GetName())
		r.kataConfig.Status.KataNodes.FailedToInstall = append(r.kataConfig.Status.KataNodes.FailedToInstall, node.GetName())
		r.setInProgressConditionToFailed(node)
	} else if isNodeFailedToUninstall(nodeMcoState, nodeCurrMc, nodeTargetMc, isKataEnabledOnNode) {
		r.Log.Info("node is FailedToUninstall", "node", node.GetName())
		r.kataConfig.Status.KataNodes.FailedToUninstall = append(r.kataConfig.Status.KataNodes.FailedToUninstall, node.GetName())
		r.setInProgressConditionToFailed(node)
	}

	return nil
}

func (r *KataConfigOpenShiftReconciler) clearNodeStatusLists() {
	r.kataConfig.Status.KataNodes.Installed = nil
	r.kataConfig.Status.KataNodes.Installing = nil
	r.kataConfig.Status.KataNodes.WaitingToInstall = nil
	r.kataConfig.Status.KataNodes.FailedToInstall = nil

	r.kataConfig.Status.KataNodes.Uninstalling = nil
	r.kataConfig.Status.KataNodes.WaitingToUninstall = nil
	r.kataConfig.Status.KataNodes.FailedToUninstall = nil
}
