package controllers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

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
