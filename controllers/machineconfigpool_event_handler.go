package controllers

import (
	"context"
	"reflect"

	"github.com/go-logr/logr"
	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	"github.com/openshift/machine-config-operator/pkg/apihelpers"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const missingMcpStatusConditionStr = "<missing>"

type McpEventHandler struct {
	reconciler *KataConfigOpenShiftReconciler
}

func isMcpRelevant(mcp client.Object) bool {
	mcpName := mcp.GetName()
	// TODO Try to find a way to include "master" only if cluster is
	// converged.  It doesn't seem to hurt to watch it even on regular
	// clusters as it doesn't really seem to change much there but it
	// would be cleaner to watch it only when it's actually needed.
	if mcpName == "kata-oc" || mcpName == "worker" || mcpName == "master" {
		return true
	}
	return false
}

func logMcpChange(log logr.Logger, statusOld, statusNew mcfgv1.MachineConfigPoolStatus) {

	log.Info("MCP updated")

	if statusOld.MachineCount != statusNew.MachineCount {
		log.Info("MachineCount changed", "old", statusOld.MachineCount, "new", statusNew.MachineCount)
	} else {
		log.Info("MachineCount", "#", statusNew.MachineCount)
	}
	if statusOld.ReadyMachineCount != statusNew.ReadyMachineCount {
		log.Info("ReadyMachineCount changed", "old", statusOld.ReadyMachineCount, "new", statusNew.ReadyMachineCount)
	} else {
		log.Info("ReadyMachineCount", "#", statusNew.ReadyMachineCount)
	}
	if statusOld.UpdatedMachineCount != statusNew.UpdatedMachineCount {
		log.Info("UpdatedMachineCount changed", "old", statusOld.UpdatedMachineCount, "new", statusNew.UpdatedMachineCount)
	} else {
		log.Info("UpdatedMachineCount", "#", statusNew.UpdatedMachineCount)
	}
	if statusOld.DegradedMachineCount != statusNew.DegradedMachineCount {
		log.Info("DegradedMachineCount changed", "old", statusOld.DegradedMachineCount, "new", statusNew.DegradedMachineCount)
	} else {
		log.Info("DegradedMachineCount", "#", statusNew.DegradedMachineCount)
	}
	if statusOld.ObservedGeneration != statusNew.ObservedGeneration {
		log.Info("ObservedGeneration changed", "old", statusOld.ObservedGeneration, "new", statusNew.ObservedGeneration)
	}

	if !reflect.DeepEqual(statusOld.Conditions, statusNew.Conditions) {

		for _, condType := range []mcfgv1.MachineConfigPoolConditionType{"Updating", "Updated"} {
			condOld := apihelpers.GetMachineConfigPoolCondition(statusOld, condType)
			condNew := apihelpers.GetMachineConfigPoolCondition(statusNew, condType)
			condStatusOld := missingMcpStatusConditionStr
			if condOld != nil {
				condStatusOld = string(condOld.Status)
			}
			condStatusNew := missingMcpStatusConditionStr
			if condNew != nil {
				condStatusNew = string(condNew.Status)
			}

			if condStatusOld != condStatusNew {
				log.Info("mcp.status.conditions[] changed", "type", condType, "old", condStatusOld, "new", condStatusNew)
			}
		}
	}
}

func (eh *McpEventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	mcp := event.Object

	if !isMcpRelevant(mcp) {
		return
	}

	// Don't reconcile on MCP creation since we're unlikely to witness "worker"
	// creation and "kata-oc" should be only created by this controller.
	// Log the event anyway.
	log := eh.reconciler.Log.WithName("McpCreate").WithValues("MCP name", mcp.GetName())
	log.Info("MCP created")
}

func (eh *McpEventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	mcpOld := event.ObjectOld
	mcpNew := event.ObjectNew

	if !isMcpRelevant(mcpNew) {
		return
	}

	statusOld := mcpOld.(*mcfgv1.MachineConfigPool).Status
	statusNew := mcpNew.(*mcfgv1.MachineConfigPool).Status
	if reflect.DeepEqual(statusOld, statusNew) {
		return
	}

	foundRelevantChange := false

	if statusOld.MachineCount != statusNew.MachineCount {
		foundRelevantChange = true
	} else if statusOld.ReadyMachineCount != statusNew.ReadyMachineCount {
		foundRelevantChange = true
	} else if statusOld.UpdatedMachineCount != statusNew.UpdatedMachineCount {
		foundRelevantChange = true
	} else if statusOld.DegradedMachineCount != statusNew.DegradedMachineCount {
		foundRelevantChange = true
	}

	if !reflect.DeepEqual(statusOld.Conditions, statusNew.Conditions) {

		for _, condType := range []mcfgv1.MachineConfigPoolConditionType{"Updating", "Updated"} {
			condOld := apihelpers.GetMachineConfigPoolCondition(statusOld, condType)
			condNew := apihelpers.GetMachineConfigPoolCondition(statusNew, condType)
			condStatusOld := missingMcpStatusConditionStr
			if condOld != nil {
				condStatusOld = string(condOld.Status)
			}
			condStatusNew := missingMcpStatusConditionStr
			if condNew != nil {
				condStatusNew = string(condNew.Status)
			}

			if condStatusOld != condStatusNew {
				foundRelevantChange = true
			}
		}
	}

	if eh.reconciler.kataConfig != nil && foundRelevantChange {

		log := eh.reconciler.Log.WithName("McpUpdate").WithValues("MCP name", mcpOld.GetName())
		logMcpChange(log, statusOld, statusNew)

		queue.Add(eh.reconciler.makeReconcileRequest())
	}
}

func (eh *McpEventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	mcp := event.Object

	if !isMcpRelevant(mcp) {
		return
	}

	// Don't reconcile on MCP deletion since "worker" should never be deleted and
	// "kata-oc" should be only deleted by this controller.  Log the event anyway.
	log := eh.reconciler.Log.WithName("McpDelete").WithValues("MCP name", mcp.GetName())
	log.Info("MCP deleted")
}

func (eh *McpEventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	mcp := event.Object

	if !isMcpRelevant(mcp) {
		return
	}

	// Don't reconcile on MCP generic event since it's not quite clear ATM
	// what it even means (we might revisit this later).  Log the event anyway.
	log := eh.reconciler.Log.WithName("McpGenericEvt").WithValues("MCP name", mcp.GetName())
	log.Info("MCP generic event")
}
