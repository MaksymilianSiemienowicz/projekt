package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type FlowServer struct {
	K8sClient client.Client
	Clientset *kubernetes.Clientset
	Namespace string
}

type FlowRequest struct {
	FlowID string `json:"flow_id"`
}

func (f *FlowServer) StartServer() {
	http.HandleFunc("/create-flow", f.handleFlow)
	go func() {
		fmt.Println("HTTP server listening on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			panic(err)
		}
	}()
}

func (f *FlowServer) handleFlow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FlowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.FlowID,
			Namespace: f.Namespace,
		},
		Data: map[string]string{
			"FLOW_ID":        req.FlowID,
			"MONGO_DB":       "SBTG_data",
			"KAFKA_BOOTSTRAP": "kafka1.projekt.home",
		},
	}

	_, err := f.Clientset.CoreV1().ConfigMaps(f.Namespace).Create(context.Background(), cm, metav1.CreateOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create ConfigMap: %v", err), http.StatusInternalServerError)
		return
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-processing-" + req.FlowID,
			Namespace: f.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "data-processing-" + req.FlowID},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "data-processing-" + req.FlowID},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "data-processing",
							Image: "maks0x073/data-processing:1",
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: req.FlowID, // nasza dynamiczna ConfigMap
										},
									},
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "MONGO_URI",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "mongo-uri", // istniejący Secret
											},
											Key: "MONGO_URI",
										},
									},
								},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									"memory": resourceMustParse("512Mi"),
									"cpu":    resourceMustParse("500m"),
								},
								Requests: corev1.ResourceList{
									"memory": resourceMustParse("256Mi"),
									"cpu":    resourceMustParse("250m"),
								},
							},
						},
					},
				},
			},
		},
	}

	_, err = f.Clientset.AppsV1().Deployments(f.Namespace).Create(context.Background(), dep, metav1.CreateOptions{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create Deployment: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Flow %s created successfully", req.FlowID)
}

func int32Ptr(i int32) *int32 { return &i }
func resourceMustParse(val string) resource.Quantity {
	q, _ := resource.ParseQuantity(val)
	return q
}
