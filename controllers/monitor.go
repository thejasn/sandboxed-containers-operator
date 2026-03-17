package controllers

import (
	"context"
	"os"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

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
