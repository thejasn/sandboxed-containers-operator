package controllers

import (
	kataconfigurationv1 "github.com/openshift/sandboxed-containers-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (r *KataConfigOpenShiftReconciler) findInProgressCondition() *kataconfigurationv1.KataConfigCondition {
	for i := 0; i < len(r.kataConfig.Status.Conditions); i++ {
		if r.kataConfig.Status.Conditions[i].Type == kataconfigurationv1.KataConfigInProgress {
			return &r.kataConfig.Status.Conditions[i]
		}
	}
	return nil
}

func (r *KataConfigOpenShiftReconciler) getInProgressConditionValue() corev1.ConditionStatus {
	cond := r.findInProgressCondition()
	if cond == nil {
		return corev1.ConditionUnknown
	}
	return cond.Status
}

func (r *KataConfigOpenShiftReconciler) addInProgressCondition() *kataconfigurationv1.KataConfigCondition {
	r.kataConfig.Status.Conditions = append(r.kataConfig.Status.Conditions, kataconfigurationv1.KataConfigCondition{Type: kataconfigurationv1.KataConfigInProgress})

	r.Log.Info("InProgress Condition added")

	return &r.kataConfig.Status.Conditions[len(r.kataConfig.Status.Conditions)-1]
}

// This is just a technical helper to all InProgress Condition mutators,
// factoring their common preamble out into an own function.
func (r *KataConfigOpenShiftReconciler) retrieveInProgressConditionForChange() *kataconfigurationv1.KataConfigCondition {
	cond := r.findInProgressCondition()
	if cond == nil {
		cond = r.addInProgressCondition()
	}

	cond.LastTransitionTime = metav1.Now()

	return cond
}

func (r *KataConfigOpenShiftReconciler) setInProgressConditionToInstalling() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = "Installing"
	cond.Message = "Performing initial installation of kata on cluster"

	r.Log.Info("InProgress Condition set to Installing")
}

func (r *KataConfigOpenShiftReconciler) setInProgressConditionToUninstalling() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = "Uninstalling"
	cond.Message = "Removing kata from cluster"

	r.Log.Info("InProgress Condition set to Uninstalling")
}

func (r *KataConfigOpenShiftReconciler) setInProgressConditionToUpdating() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = "Updating"
	cond.Message = "Adding and/or removing kata-enabled nodes"

	r.Log.Info("InProgress Condition set to Updating")
}

func (r *KataConfigOpenShiftReconciler) setInProgressConditionToFailed(failingNode *corev1.Node) {
	reasonForDegraded, ok := failingNode.Annotations["machineconfiguration.openshift.io/reason"]
	if !ok {
		r.Log.Info("Missing machineconfiguration.openshift.io/reason on Degraded node", "node", failingNode.GetName())
	}

	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = "Failed"
	cond.Message = "Node " + failingNode.GetName() + " Degraded: " + reasonForDegraded

	r.Log.Info("InProgress Condition set to Failed")
}

func (r *KataConfigOpenShiftReconciler) setInProgressConditionToBlockedByExistingKataPods(message string) {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionFalse
	cond.Reason = "BlockedByExistingKataPods"
	cond.Message = message

	r.Log.Info("InProgress Condition set to BlockedByExistingKataPods")
}

func (r *KataConfigOpenShiftReconciler) resetInProgressCondition() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionFalse
	cond.Reason = ""
	cond.Message = ""

	r.Log.Info("InProgress Condition reset")
}

// Method to set the InProgress condition to indicate that the Pod VM Image is being created
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageCreating() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageJobRunning
	cond.Message = "Creating Pod VM Image"

	r.Log.Info("InProgress Condition set to PodVMImageJobRunning")
}

// Method to set the InProgress condition to indicate that the Pod VM Image has been created
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageCreated() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageJobCompleted
	cond.Message = "Created Pod VM Image"

	r.Log.Info("InProgress Condition set to PodVMImageJobCompleted")
}

// Method to set the InProgress condition to indicate that the Pod VM Image creation has failed
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageCreationFailed() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageJobFailed
	cond.Message = "Failed to create Pod VM Image"

	r.Log.Info("InProgress Condition set to PodVMImageJobFailed")
}

// Method to set the InProgress condition to indicate that the Pod VM Image creation status is unknown
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageCreationUnknown() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionUnknown
	cond.Reason = PodVMImageJobStatusUnknown
	cond.Message = "Pod VM Image creation status is unknown"

	r.Log.Info("InProgress Condition set to PodVMImageJobStatusUnknown")
}

// Method to set the InProgress condition to indicate that the Pod VM Image is being deleted
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageDeleting() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageJobRunning
	cond.Message = "Deleting Pod VM Image"

	r.Log.Info("InProgress Condition set to PodVMImageJobRunning")
}

// Method to set the InProgress condition to indicate that the Pod VM Image has been deleted
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageDeleted() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageJobCompleted
	cond.Message = "Deleted Pod VM Image"

	r.Log.Info("InProgress Condition set to PodVMImageJobCompleted")
}

// Method to set the InProgress condition to indicate that the Pod VM Image deletion has failed
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageDeletionFailed() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageJobFailed
	cond.Message = "Failed to delete Pod VM Image"

	r.Log.Info("InProgress Condition set to PodVMImageJobFailed")
}

// Method to set the InProgress condition to indicate that the Pod VM Image deletion status is unknown
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageDeletionUnknown() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionUnknown
	cond.Reason = PodVMImageJobStatusUnknown
	cond.Message = "Pod VM Image deletion status is unknown"

	r.Log.Info("InProgress Condition set to PodVMImageJobStatusUnknown")
}

// Method to set the InProgress condition to indicate that the Pod VM image provider is unsupported
func (r *KataConfigOpenShiftReconciler) setInProgressConditionToPodVMImageUnsupportedProvider() {
	cond := r.retrieveInProgressConditionForChange()
	cond.Status = corev1.ConditionTrue
	cond.Reason = PodVMImageUnsupportedProvider
	cond.Message = "Pod VM image provider is unsupported"

	r.Log.Info("InProgress Condition set to PodVMImageUnsupportedProvider")
}
