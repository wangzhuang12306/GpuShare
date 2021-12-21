package gpu_scheduler

import (
	_ "fmt"
	_ "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"math"
	"strconv"
	"strings"
	"sync"

	gpusharev1 "github.com/wangzhuang12306/GpuShare/api/gpushare/v1"
	corev1 "k8s.io/api/core/v1"
)

func syncClusterResources(nodeList []*corev1.Node, podList []*corev1.Pod, virtualPodList []*gpusharev1.VirtualPod) (nodeResources NodeResources) {
	//同步节点资源，主要是针对nodeList中的每个节点，获取节点上的cpu、内存、GPU数量和显存 总量 并存入结构体nodeResources中
	nodeResources = syncNodeResources(nodeList)
	klog.Infof("--------Now in sync Cluster Resources--------")
	//同步pod和VirtualPod资源，主要是计算每个节点cpu、显存、Gpu数量的资源剩余；
	//获取每个VirtualPod的资源申请以及tag，计算每个gpuid下的GPU可用的算力和显存大小，并将VirtualPod的tag加到该GPU下。
	syncPodResources(nodeResources, podList, virtualPodList) //
	return
}

func syncPodResources(nodeRes NodeResources, podList []*corev1.Pod, virtualPodList []*gpusharev1.VirtualPod) {
	for _, pod := range podList {
		nodeName := pod.Spec.NodeName
		// 1. If Pod is not scheduled, it don't use resources.
		// 2. If Pod's name contains "VirtualPod-clientpod" is managed by VirtualPod, (VirtualPod-clientpod是由VirtualPod创建的傀儡pod，不耗费节点资源，只用来获取gpu的uuid)
		//    resource usage will be calcuated later.

		//未被调度的的pod和VirtualPod创建的clientpod由于不消耗资源，因此资源之后再计算
		if nodeName == "" || strings.Contains(pod.Name, gpusharev1.GPUShareClientPodName) {
			continue
		}
		// If a Pod is owned by a VirtualPod, calculating their resource usage later.
		ownedByVirtualPod := false
		for _, owneref := range pod.ObjectMeta.OwnerReferences {
			if owneref.Kind == "VirtualPod" {
				ownedByVirtualPod = true
				break
			}
		}
		if ownedByVirtualPod {
			continue
		}

		// If a running Pod is on the node we don't want, don't calculate it.
		// ex. on master has NoSchedule Taint.
		if _, ok := nodeRes[nodeName]; !ok {
			continue
		}

		//若pod的重启策略为不重启或失败后才重启，且pod已经完成或失败，则不计算该pod的资源
		if (pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure &&
			pod.Status.Phase == corev1.PodSucceeded) ||
			(pod.Spec.RestartPolicy == corev1.RestartPolicyNever &&
				(pod.Status.Phase == corev1.PodSucceeded ||
					pod.Status.Phase == corev1.PodFailed)) {
			continue
		}

		//计算节点cpu、显存、Gpu数量的资源剩余
		for _, container := range pod.Spec.Containers {
			nodeRes[nodeName].CpuFree -= container.Resources.Requests.Cpu().MilliValue()
			nodeRes[nodeName].MemFree -= container.Resources.Requests.Memory().MilliValue()
			gpu := container.Resources.Requests[gpusharev1.ResourceNVIDIAGPU]
			nodeRes[nodeName].GpuFreeCount -= int(gpu.Value())
		}
	}



	for _, virtualPod := range virtualPodList {
		nodeName := virtualPod.Spec.NodeName //获取virtualpod所在node的nodename
		// 1. If Pod is not scheduled, it don't use resources.
		if nodeName == "" {
			continue
		}
		// If a running Pod is on the node we don't want, don't calculate it.
		// ex. on master has NoSchedule Taint.
		if _, ok := nodeRes[nodeName]; !ok {
			continue
		}
		if virtualPod.Status.PodStatus != nil {
			// why policy Always is ignored? why??? I forgot why wrote this then
			// if (virtualPod.Spec.RestartPolicy == corev1.RestartPolicyAlways) ||
			if (virtualPod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure &&
				virtualPod.Status.PodStatus.Phase == corev1.PodSucceeded) ||
				(virtualPod.Spec.RestartPolicy == corev1.RestartPolicyNever &&
					(virtualPod.Status.PodStatus.Phase == corev1.PodSucceeded ||
						virtualPod.Status.PodStatus.Phase == corev1.PodFailed)) {
				continue
			}
		}

		//计算node的cpu和内存剩余量
		for _, container := range virtualPod.Spec.Containers {
			nodeRes[nodeName].CpuFree -= container.Resources.Requests.Cpu().MilliValue()
			nodeRes[nodeName].MemFree -= container.Resources.Requests.Memory().MilliValue()
		}

		//virtualpod
		isGPUPod := false
		gpu_request := 0.0
		gpu_limit := 0.0
		gpu_memory := int64(0)
		resource_priority := int64(0)
		GPUID := ""
		GuaranteedTag := ""
		BurstableTag := ""
		BesteffortTag := ""
		TaskID := ""

		if virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest] != "" ||
			virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit] != "" ||
			virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory] != "" ||
			virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID] != ""||
			virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority] != ""  {
			var err error
			gpu_limit, err = strconv.ParseFloat(virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit], 64)
			if err != nil || gpu_limit > 1.0 || gpu_limit < 0.0 {
				continue
			}
			gpu_request, err = strconv.ParseFloat(virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest], 64)
			if err != nil || gpu_request > gpu_limit || gpu_request < 0.0 {
				continue
			}
			gpu_memory, err = strconv.ParseInt(virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory], 10, 64)
			if err != nil || gpu_memory < 0 {
				continue
			}
			resource_priority, err = strconv.ParseInt(virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority], 10, 64)
			if err != nil  {
				continue
			}
			GPUID = virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID]
			isGPUPod = true  //若该GPU存在gpuid，则说明该GPU位于gpu pool中
		}

		if val, ok := virtualPod.ObjectMeta.Annotations[gpusharev1.GPUShareTaskID]; ok {
			TaskID = val
		}


		if resource_priority == 0{
			GuaranteedTag = TaskID
		}else if resource_priority == 1{
			BurstableTag = TaskID
		}else if resource_priority == 2{
			BesteffortTag = TaskID
		}else {
			klog.Errorf("Error! You must assign task priority of 0/1/2 !!")
			return
		}

		if isGPUPod {
			if gpuInfo, ok := nodeRes[nodeName].GpuFree[GPUID]; !ok {
				if nodeRes[nodeName].GpuFreeCount > 0 {
					nodeRes[nodeName].GpuFreeCount--
					nodeRes[nodeName].GpuFree[GPUID] = &GPUInfo{
						GPUFreeReq: 1000 - int64(math.Ceil(gpu_request*(float64)(1000.0))),
						GPUFreeMem: nodeRes[nodeName].GpuMemTotal - gpu_memory,
					}
				} else {
					klog.Errorf("==================================")
					klog.Errorf("Bug! The rest number of free GPU is not enough for virtualpod! GPUID: %s", GPUID)
					for errID, errGPU := range nodeRes[nodeName].GpuFree {
						klog.Errorf("GPUID: %s", errID)
						klog.Errorf("    Req: %d", errGPU.GPUFreeReq)
						klog.Errorf("    Mem: %d", errGPU.GPUFreeMem)
					}
					klog.Errorf("==================================")
					continue
				}
			} else {
				gpuInfo.GPUFreeReq -= int64(math.Ceil(gpu_request * (float64)(1000.0)))
				gpuInfo.GPUFreeMem -= gpu_memory
			}//计算该gpuid下的GPU可用的算力和显存大小

			if GuaranteedTag != "" {
				isFound := false
				for _, val := range nodeRes[nodeName].GpuFree[GPUID].ResourceGuaranteed {
					if val == GuaranteedTag {
						isFound = true
						break
					}
				} //查看该VirtualPod的TaskID是否在该GPU的ResourceGuaranteed列表中，若在的话则isFound置为true
				if !isFound {
					nodeRes[nodeName].GpuFree[GPUID].ResourceGuaranteed = append(nodeRes[nodeName].GpuFree[GPUID].ResourceGuaranteed, GuaranteedTag)
				}//若不在，则为该GPUID加上该资源保证型TaskID
			}

			if BurstableTag != "" {
				isFound := false
				for _, val := range nodeRes[nodeName].GpuFree[GPUID].ResourceBurstable {
					if val == BurstableTag {
						isFound = true
						break
					}
				}
				if !isFound {
					nodeRes[nodeName].GpuFree[GPUID].ResourceBurstable = append(nodeRes[nodeName].GpuFree[GPUID].ResourceBurstable, BurstableTag)
				}
			}//ResourceBurstable步骤与ResourceGuaranteed相同

			if BesteffortTag != "" {
				isFound := false
				for _, val := range nodeRes[nodeName].GpuFree[GPUID].ResourceBesteffort {
					if val == BesteffortTag {
						isFound = true
						break
					}
				}
				if !isFound {
					nodeRes[nodeName].GpuFree[GPUID].ResourceBesteffort = append(nodeRes[nodeName].GpuFree[GPUID].ResourceBesteffort, BesteffortTag)
				}
			}
		}
	}
}

func syncNodeResources(nodeList []*corev1.Node) (nodeResources NodeResources) {

	nodeResources = make(NodeResources, len(nodeList))
	var nodeResourcesMux sync.Mutex
	var wait sync.WaitGroup

	//对于给定的节点，同步节点资源信息
	syncNode := func(node *corev1.Node) {
		// If NoSchedule Taint on the node, don't add to NodeResources! 节点有污点时，不能调度到该节点上
		cannotScheduled := false
		for _, taint := range node.Spec.Taints {
			if string(taint.Effect) == "NoSchedule" {
				cannotScheduled = true
				klog.Info("Node have NoSchedule taint, node name: ", node.ObjectMeta.Name)
				break
			}
		}
		if cannotScheduled {
			return
		}

		//获取节点上可调度的cpu、内存、GPU数量和显存大小
		cpu := node.Status.Allocatable.Cpu().MilliValue()
		mem := node.Status.Allocatable.Memory().MilliValue()
		gpuNum := func() int {
			tmp := node.Status.Allocatable[gpusharev1.ResourceNVIDIAGPU]
			return int(tmp.Value())
		}()
		gpuMem := func() int64 {
			if gpuInfo, ok := node.ObjectMeta.Annotations[gpusharev1.GPUShareNodeGPUInfo]; ok {
				gpuInfoArr := strings.Split(gpuInfo, ",")
				if len(gpuInfoArr) >= 1 {
					gpuArr := strings.Split(gpuInfoArr[0], ":")
					if len(gpuArr) != 2 {
						klog.Errorf("GPU Info format error: %s", gpuInfo)
						return 0
					}
					gpuMem, err := strconv.ParseInt(gpuArr[1], 10, 64)
					if err != nil {
						klog.Errorf("GPU Info format error: %s", gpuInfo)
						return 0
					}
					return gpuMem
				} else {
					return 0
				}
			} else {
				return 0
			}
		}()
		nodeResourcesMux.Lock()

		//记录该节点目前的各资源量
		nodeResources[node.ObjectMeta.Name] = &NodeResource{
			CpuTotal:     cpu,
			MemTotal:     mem,
			GpuTotal:     gpuNum,
			GpuMemTotal:  gpuMem * 1024 * 1024, // in bytes
			CpuFree:      cpu,
			MemFree:      mem,
			GpuFreeCount: gpuNum,
			GpuFree:      make(map[string]*GPUInfo, gpuNum),
		}
		nodeResourcesMux.Unlock()
		wait.Done()
	}

	wait.Add(len(nodeList))

	//遍历nodeList中的每个节点，获取当前各节点的资源信息
	for _, node := range nodeList {
		go syncNode(node)
	}
	wait.Wait()
	return
}
