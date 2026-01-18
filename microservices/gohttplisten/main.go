package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// Request payload
type DeploymentRequest struct {
	FlowID    string `json:"flow_id"`
	UserID    string `json:"user_id"`
	Replicas  int32  `json:"replicas,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

// Delete request payload
type DeleteRequest struct {
	FlowID    string `json:"flow_id"`
	UserID    string `json:"user_id"`
	Namespace string `json:"namespace,omitempty"`
}

type DeploymentResponse struct {
	Success         bool     `json:"success"`
	Message         string   `json:"message"`
	DeploymentName  string   `json:"deployment_name,omitempty"`
	TerminatingPods []string `json:"terminating_pods,omitempty"`
}

var clientset *kubernetes.Clientset

func main() {
	var port string
	flag.StringVar(&port, "port", "8080", "HTTP server port")
	flag.Parse()

	cfg, err := config.GetConfig()
	if err != nil {
		log.Fatalf("Error getting kubernetes config: %v", err)
	}

	clientset, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("Error creating kubernetes client: %v", err)
	}

	// HTTP handlers
	http.HandleFunc("/deployment", createDeploymentHandler)
	http.HandleFunc("/deployment/delete", deleteDeploymentHandler)
	http.HandleFunc("/health", healthHandler)

	log.Printf("Starting HTTP server on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// checkTerminatingPods sprawdza czy istnieją pody w stanie Terminating dla danego flow_id
// Zwraca listę nazw podów w stanie Terminating
func checkTerminatingPods(ctx context.Context, namespace, flowID string) ([]string, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("flow-id=%s", flowID),
	})
	if err != nil {
		return nil, fmt.Errorf("error listing pods: %v", err)
	}

	var terminatingPods []string
	for _, pod := range pods.Items {
		// Pod jest w stanie Terminating gdy ma ustawiony DeletionTimestamp
		if pod.DeletionTimestamp != nil {
			terminatingPods = append(terminatingPods, pod.Name)
		}
	}

	return terminatingPods, nil
}

func createDeploymentHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		respondError(w, http.StatusMethodNotAllowed, "Only POST method is allowed", "", nil)
		return
	}

	var req DeploymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err), "", nil)
		return
	}

	if req.FlowID == "" {
		respondError(w, http.StatusBadRequest, "flow_id is required", "", nil)
		return
	}

	// Default values
	namespace := req.Namespace
	if namespace == "" {
		namespace = "sbtg-data"
	}

	replicas := req.Replicas
	if replicas == 0 {
		replicas = 3
	}

	// Nazwa deploymentu bazująca na flow_id
	deploymentName := fmt.Sprintf("data-processing-%s", req.FlowID)

	ctx := context.Background()

	// Sprawdź czy są pody w stanie Terminating
	terminatingPods, err := checkTerminatingPods(ctx, namespace, req.FlowID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Error checking pod status: %v", err), deploymentName, nil)
		return
	}

	if len(terminatingPods) > 0 {
		respondError(w, http.StatusConflict,
			fmt.Sprintf("Cannot create/update deployment: %d pod(s) are still terminating. Please wait for them to finish.", len(terminatingPods)),
			deploymentName, terminatingPods)
		return
	}

	// Tworzenie Deployment
	deployment := createDeployment(deploymentName, namespace, req.FlowID, replicas)

	// Sprawdź czy Deployment już istnieje
	existing, err := clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err == nil {
		// Deployment istnieje, zaktualizuj
		existing.Spec.Replicas = &replicas
		existing.Spec.Template.Spec.Containers[0].Env = deployment.Spec.Template.Spec.Containers[0].Env
		existing.Spec.Template.Spec.Containers[0].EnvFrom = deployment.Spec.Template.Spec.Containers[0].EnvFrom

		_, err = clientset.AppsV1().Deployments(namespace).Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("Error updating Deployment: %v", err), deploymentName, nil)
			return
		}
		log.Printf("Updated Deployment %s in namespace %s with flow_id %s", deploymentName, namespace, req.FlowID)
	} else {
		// Deployment nie istnieje, utwórz nowy
		_, err = clientset.AppsV1().Deployments(namespace).Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Sprintf("Error creating Deployment: %v", err), deploymentName, nil)
			return
		}
		log.Printf("Created Deployment %s in namespace %s with flow_id %s", deploymentName, namespace, req.FlowID)
	}

	respondSuccess(w, http.StatusCreated, "Deployment created/updated successfully", deploymentName)
}

func deleteDeploymentHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		respondError(w, http.StatusMethodNotAllowed, "Only POST or DELETE methods are allowed", "", nil)
		return
	}

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err), "", nil)
		return
	}

	if req.FlowID == "" {
		respondError(w, http.StatusBadRequest, "flow_id is required", "", nil)
		return
	}

	// Default namespace
	namespace := req.Namespace
	if namespace == "" {
		namespace = "sbtg-data"
	}

	// Nazwa deploymentu bazująca na flow_id
	deploymentName := fmt.Sprintf("data-processing-%s", req.FlowID)

	ctx := context.Background()

	// Sprawdź czy są pody w stanie Terminating
	terminatingPods, err := checkTerminatingPods(ctx, namespace, req.FlowID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Error checking pod status: %v", err), deploymentName, nil)
		return
	}

	if len(terminatingPods) > 0 {
		respondError(w, http.StatusConflict,
			fmt.Sprintf("Cannot delete deployment: %d pod(s) are still terminating. Please wait for them to finish.", len(terminatingPods)),
			deploymentName, terminatingPods)
		return
	}

	// Opcje usuwania - natychmiastowe usunięcie
	deletePolicy := metav1.DeletePropagationForeground
	deleteOptions := metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}

	// Sprawdź czy Deployment istnieje
	_, err = clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			respondError(w, http.StatusNotFound, fmt.Sprintf("Deployment %s not found in namespace %s", deploymentName, namespace), deploymentName, nil)
			return
		}
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Error checking Deployment: %v", err), deploymentName, nil)
		return
	}

	// Usuń Deployment
	err = clientset.AppsV1().Deployments(namespace).Delete(ctx, deploymentName, deleteOptions)
	if err != nil {
		respondError(w, http.StatusInternalServerError, fmt.Sprintf("Error deleting Deployment: %v", err), deploymentName, nil)
		return
	}

	log.Printf("Deleted Deployment %s in namespace %s with flow_id %s", deploymentName, namespace, req.FlowID)
	respondSuccess(w, http.StatusOK, "Deployment deleted successfully", deploymentName)
}

func createDeployment(name, namespace, flowID string, replicas int32) *appsv1.Deployment {
	maxSurge := intstr.FromInt(1)
	maxUnavailable := intstr.FromInt(0)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app":        "data-processing",
				"flow-id":    flowID,
				"created-by": "flow-operator",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":     "data-processing",
					"flow-id": flowID,
				},
			},
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxSurge:       &maxSurge,
					MaxUnavailable: &maxUnavailable,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":     "data-processing",
						"flow-id": flowID,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "data-processing",
							Image:           "maks0x073/data-processing:8",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{
									Name:  "FLOW_ID",
									Value: flowID,
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "data-processing-secret",
										},
									},
								},
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "data-processing-config",
										},
									},
								},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("512Mi"),
									corev1.ResourceCPU:    resource.MustParse("500m"),
								},
								Requests: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("256Mi"),
									corev1.ResourceCPU:    resource.MustParse("250m"),
								},
							},
						},
					},
				},
			},
		},
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func respondError(w http.ResponseWriter, code int, message string, deploymentName string, terminatingPods []string) {
	w.WriteHeader(code)
	response := DeploymentResponse{
		Success:         false,
		Message:         message,
		DeploymentName:  deploymentName,
		TerminatingPods: terminatingPods,
	}
	json.NewEncoder(w).Encode(response)
	log.Printf("Error [%d]: %s", code, message)
}

func respondSuccess(w http.ResponseWriter, code int, message string, name string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(DeploymentResponse{
		Success:        true,
		Message:        message,
		DeploymentName: name,
	})
}
