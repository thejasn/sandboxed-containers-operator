package controllers

import (
	"context"
	"reflect"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type NodeEventHandler struct {
	reconciler *KataConfigOpenShiftReconciler
}

func isWorkerNode(node client.Object) bool {
	if _, ok := node.GetLabels()["node-role.kubernetes.io/worker"]; ok {
		return true
	}
	return false
}

func getStringMapDiff(oldMap, newMap map[string]string) (added, modified, removed map[string]string) {
	added = map[string]string{}
	modified = map[string]string{}
	removed = map[string]string{}

	if oldMap == nil {
		added = newMap
		return
	}

	if newMap == nil {
		removed = oldMap
		return
	}

	for newKey, newVal := range newMap {
		if _, ok := oldMap[newKey]; !ok {
			added[newKey] = newVal
		}
	}
	for oldKey, oldVal := range oldMap {
		if _, ok := newMap[oldKey]; !ok {
			removed[oldKey] = oldVal
		} else {
			if oldMap[oldKey] != newMap[oldKey] {
				modified[oldKey] = newMap[oldKey]
			}
		}
	}
	return added, modified, removed
}

func (eh *NodeEventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	node := event.Object

	log := eh.reconciler.Log.WithName("NodeCreate").WithValues("node name", node.GetName())
	log.Info("node created")

	if !isWorkerNode(node) {
		return
	}

	if eh.reconciler.kataConfig == nil {
		return
	}

	if !eh.reconciler.nodeMatchesKataSelector(node.GetLabels()) {
		return
	}
	log.Info("node matches kata node selector", "node labels", node.GetLabels())

	queue.Add(eh.reconciler.makeReconcileRequest())
}

func (eh *NodeEventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// This function assumes that a node cannot change its role from master to
	// worker or vice-versa.
	nodeOld := event.ObjectOld
	nodeNew := event.ObjectNew

	log := eh.reconciler.Log.WithName("NodeUpdate").WithValues("node name", nodeNew.GetName())

	if !isWorkerNode(nodeNew) {
		return
	}

	if eh.reconciler.kataConfig == nil {
		return
	}

	foundRelevantChange := false

	if eh.reconciler.DeploymentMode == DaemonSetMode {
		// "" is not a valid kataconfiguration.openshift.io/kata-ds-rpm-install value
		kataStateOld := nodeOld.GetLabels()[kataInstallationDaemonSetLabel]
		kataStateNew := nodeNew.GetLabels()[kataInstallationDaemonSetLabel]
		if kataStateOld != kataStateNew {
			foundRelevantChange = true
			log.Info("kataconfiguration.openshift.io/kata-ds-rpm-install changed", "old", kataStateOld, "new", kataStateNew)
		}
	} else {
		// no need to check the second return value of the indexing operation
		// as "" is not a valid machineconfiguration.openshift.io/state value
		stateOld := nodeOld.GetAnnotations()["machineconfiguration.openshift.io/state"]
		stateNew := nodeNew.GetAnnotations()["machineconfiguration.openshift.io/state"]
		if stateOld != stateNew {
			foundRelevantChange = true
			log.Info("machineconfiguration.openshift.io/state changed", "old", stateOld, "new", stateNew)
		}
	}

	labelsOld := nodeOld.GetLabels()
	labelsNew := nodeNew.GetLabels()

	if !reflect.DeepEqual(labelsOld, labelsNew) {

		log.Info("labels changed", "old", labelsOld, "new", labelsNew)
		added, modified, removed := getStringMapDiff(labelsOld, labelsNew)
		log.Info("labels diff", "added", added, "modified", modified, "removed", removed)

		matchOld := eh.reconciler.nodeMatchesKataSelector(labelsOld)
		matchNew := eh.reconciler.nodeMatchesKataSelector(labelsNew)

		log.Info("labels matching kata node selector", "old", matchOld, "new", matchNew)
		if matchOld != matchNew {
			foundRelevantChange = true
		}
	}

	if foundRelevantChange {
		queue.Add(eh.reconciler.makeReconcileRequest())
	}
}

func (eh *NodeEventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (eh *NodeEventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}
