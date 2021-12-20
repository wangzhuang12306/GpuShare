/*
Copyright 2021 wz123456.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


// +genclient
package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math/rand"
	"time"
	"unsafe"
)

const (
	letterIdxBits = 5                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	letterBytes   = "abcdefghijklmnopqrstuvwxyz"

	// gpushare constants
	GPUShareResourceGPURequest = "gpushare/gpu_request"
	GPUShareResourceGPULimit   = "gpushare/gpu_limit"
	GPUShareResourceGPUMemory  = "gpushare/gpu_memory"
	GPUShareResourcePriority   = "gpushare/resource_priority" // resource_priority: 0、1、2
	GPUShareResourceGPUID      = "gpushare/GPUID"
	GPUShareClientPodName       = "gpushare-vgpu"
	GPUShareNodeName           = "gpushare/nodeName"
	GPUShareRole               = "gpushare/role"
	GPUShareNodeGPUInfo        = "gpushare/gpu_info"
	ResourceNVIDIAGPU          = "nvidia.com/gpu"
	GPUShareTaskID			   = "gpushare/taskid" //taskid : namespace/name
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VirtualPod is the Schema for the virtualpods API
type VirtualPod struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status VirtualPodStatus `json:"status,omitempty"`
	// +optional
	Spec corev1.PodSpec `json:"spec,omitempty"`
}

// VirtualPodStatus defines the observed state of VirtualPod
type VirtualPodStatus struct {
	/*PodPhase          corev1.PodPhase
	ConfigFilePhase   ConfigFilePhase
	BoundDeviceID     string
	StartTime         *metav1.Time
	ContainerStatuses []corev1.ContainerStatus*/
	PodStatus      *corev1.PodStatus `json:"status,omitempty"`
	PodObjectMeta  *metav1.ObjectMeta `json:"metadata,omitempty"`
	BoundDeviceID  string `json:"deviceid,omitempty"`
	PodManagerPort int		`json:"podmanagerport,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VirtualPodList contains a list of VirtualPod
type VirtualPodList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items           []VirtualPod `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VirtualPod{}, &VirtualPodList{})
}

var src = rand.NewSource(time.Now().UnixNano())
func NewGPUID(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return *(*string)(unsafe.Pointer(&b))
}