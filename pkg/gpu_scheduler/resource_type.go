package gpu_scheduler

import "k8s.io/klog/v2"

/* ------------------- struct NodeResources start ------------------- */

// NodeResources: Available resources in cluster to schedule Training Jobs
type NodeResources map[string]*NodeResource

func (this *NodeResources) DeepCopy() *NodeResources {
	copy := make(NodeResources, len(*this))
	for k, v := range *this {
		copy[k] = v.DeepCopy()
	}
	return &copy
}

func (this *NodeResources) PrintMe() {
	for name, res := range *this {
		klog.Infof("============ Node: %s ============", name)
		klog.Infof("CpuTotal: %d", res.CpuTotal)
		klog.Infof("MemTotal: %d", res.MemTotal)
		klog.Infof("GpuTotal: %d", res.GpuTotal)
		klog.Infof("GpuMemTotal: %d", res.GpuMemTotal)
		klog.Infof("CpuFree: %d", res.CpuFree)
		klog.Infof("MemFree: %d", res.MemFree)
		klog.Infof("GpuFree: %d", res.GpuFreeCount)
		klog.Infof("GpuId:")
		for id, gpu := range res.GpuFree {
			klog.Infof("    %s: %d, %d", id, (*gpu).GPUFreeReq, (*gpu).GPUFreeMem)
		}
	}
	klog.Infof("============ Node Info End ============")
}

/* ------------------- struct NodeResources end ------------------- */

/* ------------------- struct NodeResource start ------------------- */

type NodeResource struct {
	CpuTotal int64
	MemTotal int64
	GpuTotal int
	// GpuMemTotal in bytes
	GpuMemTotal int64
	CpuFree     int64
	MemFree     int64
	/* Available GPU calculate */
	// Total GPU count - Pods using nvidia.com/gpu
	GpuFreeCount int
	// GPUs available usage (1.0 - VirtualPod usage)
	// GPUID to integer index mapping
	GpuFree map[string]*GPUInfo
}

func (this *NodeResource) DeepCopy() *NodeResource {
	gpuFreeCopy := make(map[string]*GPUInfo, len(this.GpuFree))
	for k, v := range this.GpuFree {
		gpuFreeCopy[k] = v.DeepCopy()
	}
	return &NodeResource{
		CpuTotal:     this.CpuTotal,
		MemTotal:     this.MemTotal,
		GpuTotal:     this.GpuTotal,
		GpuMemTotal:  this.GpuMemTotal,
		CpuFree:      this.CpuFree,
		MemFree:      this.MemFree,
		GpuFreeCount: this.GpuFreeCount,
		GpuFree:      gpuFreeCopy,
	}
}

/* ------------------- struct NodeResource end ------------------- */

/* ------------------- struct GPUInfo start ------------------- */

type GPUInfo struct {
	GPUFreeReq int64
	// GPUFreeMem in bytes
	GPUFreeMem int64

	ResourceGuaranteed []string
	ResourceBurstable []string
	ResourceBesteffort []string
}

func (this *GPUInfo) DeepCopy() *GPUInfo {
	var tmpResourceGuaranteed []string
	var tmpResourceBurstable []string
	var tmpResourceBesteffort []string
	for _, v := range this.ResourceGuaranteed {
		tmpResourceGuaranteed = append(tmpResourceGuaranteed, v)
	}
	for _, v := range this.ResourceBurstable {
		tmpResourceBurstable = append(tmpResourceBurstable, v)
	}
	for _, v := range this.ResourceBesteffort {
		tmpResourceBesteffort = append(tmpResourceBesteffort, v)
	}
	return &GPUInfo{
		GPUFreeReq:          this.GPUFreeReq,
		GPUFreeMem:          this.GPUFreeMem,
		ResourceGuaranteed: tmpResourceGuaranteed,
		ResourceBurstable: tmpResourceBurstable,
		ResourceBesteffort: tmpResourceBesteffort,
	}
}

/* ------------------- struct GPUInfo end ------------------- */