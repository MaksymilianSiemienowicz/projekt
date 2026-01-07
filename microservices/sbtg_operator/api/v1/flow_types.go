package v1

import (
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FlowSpec struct {
    FlowID string `json:"flow_id"`
}

type FlowStatus struct {
    Created bool `json:"created"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
type Flow struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   FlowSpec   `json:"spec,omitempty"`
    Status FlowStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
type FlowList struct {
    metav1.TypeMeta `json:",inline"`
    metav1.ListMeta `json:"metadata,omitempty"`
    Items           []Flow `json:"items"`
}
