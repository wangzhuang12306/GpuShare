package gpu_scheduler

import (
	"k8s.io/klog/v2"
	"math"
	"sync"

	gpusharev1 "github.com/wangzhuang12306/GpuShare/api/gpushare/v1"
	corev1 "k8s.io/api/core/v1"
)

func scheduleVirtualPod(isGPUPod bool, gpu_request float64, gpu_memory int64, resource_priority int64, virtualpod *gpusharev1.VirtualPod, nodeList []*corev1.Node, podList []*corev1.Pod, virtualPodList []*gpusharev1.VirtualPod) (string, string) {

	// Implement custom scheduling algorithm and replace the function assignment 实现自定义调度算法并替换函数赋值
	// Prototype: FUNC(bool, string, *kubesharev1.SharePod, NodeResources) (string, string, error)

	sa_0 := ScheduleGuaranteedTask
	sa_1 := ScheduleBurstableTask
	sa_2 := ScheduleBestEffortTask

	nodeResources := syncClusterResources(nodeList, podList, virtualPodList) //同步集群资源，包括节点和pod、virtualpod资源

	ResourcePriorityFilter(nodeResources, virtualpod, resource_priority)


	if resource_priority == 0{
		klog.Infof("--------Now in ScheduleGuaranteedTask--------")
		return sa_0(isGPUPod, virtualpod, gpu_request, gpu_memory, nodeResources)
	}else if resource_priority == 1{
		klog.Infof("--------Now in ScheduleBurstableTask--------")
		return sa_1(isGPUPod, gpu_request, gpu_memory, virtualpod, nodeResources)
	}
		klog.Infof("--------Now in ScheduleBestEffortTask--------")
		return sa_2(isGPUPod, gpu_request, gpu_memory, virtualpod, nodeResources)

}


func ResourcePriorityFilter(nodeResources NodeResources, virtualpod *gpusharev1.VirtualPod, resource_priority int64) {

	GuaranteedTag := ""
	BurstableTag := ""
	BesteffortTag := ""
	TaskID := ""
	if val, ok := virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareTaskID]; ok {
		TaskID = val
	}

	if resource_priority == 0 {
		GuaranteedTag = TaskID
	} else if resource_priority == 1 {
		BurstableTag = TaskID
	} else if resource_priority == 2 {
		BesteffortTag = TaskID
	} else {
		klog.Errorf("Error! You must assign task priority of 0/1/2 !!")
		return
	}

	if GuaranteedTag != "" {
		for _, nodeRes := range nodeResources {
			for GPUID, gpuInfo := range nodeRes.GpuFree {
				notPriority_0_and_1 := true
				if gpuInfo.ResourceGuaranteed != nil || gpuInfo.ResourceBurstable != nil  {
					notPriority_0_and_1 = false
				}
				if !notPriority_0_and_1 {
					delete(nodeRes.GpuFree, GPUID) //若这个GPUID上有Guaranteed或Burstable任务，则从该节点的GPUFree中移除掉该GPUID，表示不调度到这个GPUID上
				}
			}
		}
	}

	if BurstableTag != "" {
		for _, nodeRes := range nodeResources {
			for GPUID, gpuInfo := range nodeRes.GpuFree {
				notPriority_0 := true
				if gpuInfo.ResourceGuaranteed != nil  {
					notPriority_0 = false
				}
				if !notPriority_0 {
					delete(nodeRes.GpuFree, GPUID) //若这个GPUID上有Guaranteed任务，则从该节点的GPUFree中移除掉该GPUID，表示不调度到这个GPUID上
				}
			}
		}
	}

	if BesteffortTag != "" {
		for _, nodeRes := range nodeResources {
			for GPUID, gpuInfo := range nodeRes.GpuFree {
				flag1 := true
				flag2 := true
				flag3 := true

				if gpuInfo.ResourceBurstable != nil {
					if gpuInfo.GPUFreeReq < 200 {
						flag2 = false
					}
				}
				if gpuInfo.ResourceGuaranteed != nil {
					if len(gpuInfo.ResourceBesteffort) >= 1 {
						flag1 = false
					}
				}
				if gpuInfo.ResourceBesteffort != nil {
					if len(gpuInfo.ResourceBesteffort) >= 2 {
						flag3 = false
					}
				}

				if (!flag1)||(!flag2)||(!flag3) {
					delete(nodeRes.GpuFree, GPUID) //
				}
			}
		}
	}

}

func ScheduleGuaranteedTask(isGPUPod bool, virtualpod *gpusharev1.VirtualPod, gpu_request float64, gpu_mem int64, nodeResources NodeResources) (schedNodeName string, schedGPUID string) {
	findTheHole := false //寻找目前gpu pool中是否有满足资源请求的vgpu
	type candidateNodeGPU struct {
		NodeName string
		GPUID    string
		Point    int64
	}
	bestNode := candidateNodeGPU{
		Point:    2147483647,
		NodeName: "",
		GPUID:    "",
	}
	var bestNodeMux sync.Mutex
	//尝试选择最优节点，哪个分数低选哪个
	tryBestNode := func(point int64, nodeName, GPUID string) {
		bestNodeMux.Lock()
		if point < bestNode.Point {
			bestNode.Point = point
			bestNode.NodeName = nodeName
			bestNode.GPUID = GPUID
		}
		bestNodeMux.Unlock()
	}
	//计算该任务请求的CPU和内存总和
	var cpuReqTotal, memReqTotal int64 = 0, 0
	for _, container := range virtualpod.Spec.Containers {
		cpuReqTotal += container.Resources.Requests.Cpu().MilliValue()
		memReqTotal += container.Resources.Requests.Memory().MilliValue()
	}
	var wait sync.WaitGroup

	scheduleNode := func(nodeName string, nodeRes *NodeResource) {
		if nodeRes.CpuFree < cpuReqTotal || nodeRes.MemFree < memReqTotal {
			wait.Done()
			return
		}//如果该CPU空闲资源小于spec中请求的资源，则不选择该节点上的GPU

		cpu_cur :=  (nodeRes.CpuFree - cpuReqTotal)/nodeRes.CpuTotal
		mem_cur :=  (nodeRes.MemFree - memReqTotal)/nodeRes.MemTotal
		Bal_	:=  (cpu_cur + mem_cur)/2
		num     :=  ((cpu_cur-Bal_)*(cpu_cur-Bal_)+(mem_cur-Bal_)*(mem_cur-Bal_))/4
		Balance :=  (math.Sqrt(float64(num)) * 100) //节点cpu和内存的均衡度，越大均衡度越差

		klog.Infof("isGPUPod == '%d' ",isGPUPod)
		if isGPUPod {

			for id, gpu := range nodeRes.GpuFree {

				if gpu.GPUFreeMem < gpu_mem || len(gpu.ResourceBesteffort) >= 2 || gpu.ResourceBurstable != nil || gpu.ResourceGuaranteed != nil {
					continue
				}
				findTheHole = true
				taskNum := len(gpu.ResourceBesteffort)
				klog.Infof("taskNum == '%d' ",taskNum)
				tryBestNode(int64((taskNum+1) * 100 + int(Balance)), nodeName, id)
			}
			klog.Infof("findTheHole == '%d' ， nodeRes.GpuFreeCount ==  '%d'",findTheHole , nodeRes.GpuFreeCount)
			if !findTheHole {
				if nodeRes.GpuFreeCount > 0 {
					tryBestNode(0, nodeName, gpusharev1.NewGPUID(5)) //选择并创建新的vGPU加入gpu pool，并赋予GPUID
				}
			}
		} else {
			tryBestNode(nodeRes.CpuFree-cpuReqTotal, nodeName, "")
		}
		wait.Done()
	}

	wait.Add(len(nodeResources))

	//从node资源列表中按照调度算法选择最合适的节点以及GPU，返回节点名称和GPUID
	for nodeName, nodeRes := range nodeResources { //此处的nodeResources中的GPUFree为经过过滤器过滤后的每个节点上的GPUID
		go scheduleNode(nodeName, nodeRes)
	}
	wait.Wait()

	return bestNode.NodeName, bestNode.GPUID

}

func ScheduleBurstableTask(isGPUPod bool, gpu_request float64, gpu_mem int64, virtualpod *gpusharev1.VirtualPod, nodeResources NodeResources) (schedNodeName string, schedGPUID string) {
	findTheHole := false //寻找目前gpu pool中是否有满足资源请求的vgpu
	type candidateNodeGPU struct {
		NodeName string
		GPUID    string
		Point    int64
	}
	bestNode := candidateNodeGPU{
		Point:    2147483647,
		NodeName: "",
		GPUID:    "",
	}
	var bestNodeMux sync.Mutex
	//尝试选择最优节点，哪个分数低选哪个
	tryBestNode := func(point int64, nodeName, GPUID string) {
		bestNodeMux.Lock()
		if point < bestNode.Point {
			bestNode.Point = point
			bestNode.NodeName = nodeName
			bestNode.GPUID = GPUID
		}
		bestNodeMux.Unlock()
	}
	//计算该任务请求的CPU和内存总和
	var cpuReqTotal, memReqTotal int64 = 0, 0
	for _, container := range virtualpod.Spec.Containers {
		cpuReqTotal += container.Resources.Requests.Cpu().MilliValue()
		memReqTotal += container.Resources.Requests.Memory().MilliValue()
	}
	var wait sync.WaitGroup

	scheduleNode := func(nodeName string, nodeRes *NodeResource) {
		if nodeRes.CpuFree < cpuReqTotal || nodeRes.MemFree < memReqTotal {
			wait.Done()
			return
		}//如果该CPU空闲资源小于spec中请求的资源，则不选择该节点上的GPU
	gpu_request_millivalue := int64(math.Ceil(gpu_request * (float64)(1000.0)))

		if isGPUPod {
			for id, gpu := range nodeRes.GpuFree {
				if gpu.GPUFreeReq < gpu_request_millivalue || gpu.GPUFreeMem < gpu_mem {
					continue
				}//如果该GPU空闲资源小于请求的资源，则不选择该GPU
				if gpu.ResourceBurstable != nil && gpu.ResourceBurstable != nil {
					if (gpu.GPUFreeReq - gpu_request_millivalue) < 200 {
						continue
					}
				}

				gpu_usage_cur :=  (gpu.GPUFreeReq - gpu_request_millivalue)/1000
				gpu_mem_cur :=  (gpu.GPUFreeMem - gpu_mem)/nodeRes.GpuMemTotal
				Bal_	:=  (gpu_usage_cur + gpu_mem_cur)/2
				num     :=  ((gpu_usage_cur-Bal_)*(gpu_usage_cur-Bal_)+(gpu_mem_cur-Bal_)*(gpu_mem_cur-Bal_))/4
				Balance :=  (math.Sqrt(float64(num)) * 100) //节点cpu和内存的均衡度，越大均衡度越差

				findTheHole = true
				taskNum := len(gpu.ResourceBesteffort)
				tryBestNode(int64((taskNum+1) * 100 + int(Balance)), nodeName, id)
			}
			if !findTheHole {
				if nodeRes.GpuFreeCount > 0 {
					tryBestNode(0, nodeName, gpusharev1.NewGPUID(5)) //选择并创建新的vGPU加入gpu pool，并赋予GPUID
				}
			}
		} else {
			tryBestNode(nodeRes.CpuFree-cpuReqTotal, nodeName, "")
		}
		wait.Done()
	}

	wait.Add(len(nodeResources))

	//从node资源列表中按照调度算法选择最合适的节点以及GPU，返回节点名称和GPUID
	for nodeName, nodeRes := range nodeResources { //此处的nodeResources中的GPUFree为经过过滤器过滤后的每个节点上的GPUID
		go scheduleNode(nodeName, nodeRes)
	}
	wait.Wait()

	return bestNode.NodeName, bestNode.GPUID

}

func ScheduleBestEffortTask(isGPUPod bool, gpu_request float64, gpu_mem int64, virtualpod *gpusharev1.VirtualPod, nodeResources NodeResources) (schedNodeName string, schedGPUID string) {
	findTheHole := false //寻找目前gpu pool中是否有满足资源请求的vgpu
	type candidateNodeGPU struct {
		NodeName string
		GPUID    string
		Point    int64
	}
	bestNode := candidateNodeGPU{
		Point:    2147483647,
		NodeName: "",
		GPUID:    "",
	}
	var bestNodeMux sync.Mutex
	//尝试选择最优节点，哪个分数低选哪个
	tryBestNode := func(point int64, nodeName, GPUID string) {
		bestNodeMux.Lock()
		if point < bestNode.Point {
			bestNode.Point = point
			bestNode.NodeName = nodeName
			bestNode.GPUID = GPUID
		}
		bestNodeMux.Unlock()
	}
	//计算该任务请求的CPU和内存总和
	var cpuReqTotal, memReqTotal int64 = 0, 0
	for _, container := range virtualpod.Spec.Containers {
		cpuReqTotal += container.Resources.Requests.Cpu().MilliValue()
		memReqTotal += container.Resources.Requests.Memory().MilliValue()
	}
	var wait sync.WaitGroup

	scheduleNode := func(nodeName string, nodeRes *NodeResource) {
		if nodeRes.CpuFree < cpuReqTotal || nodeRes.MemFree < memReqTotal {
			wait.Done()
			return
		}//如果该CPU空闲资源小于spec中请求的资源，则不选择该节点上的GPU

		cpu_cur :=  (nodeRes.CpuFree - cpuReqTotal)/nodeRes.CpuTotal
		mem_cur :=  (nodeRes.MemFree - memReqTotal)/nodeRes.MemTotal
		Bal_	:=  (cpu_cur + mem_cur)/2
		num     :=  ((cpu_cur-Bal_)*(cpu_cur-Bal_)+(mem_cur-Bal_)*(mem_cur-Bal_))/4
		Balance :=  (math.Sqrt(float64(num)) * 100) //节点cpu和内存的均衡度，越大均衡度越差


		if isGPUPod {
			for id, gpu := range nodeRes.GpuFree {
				if gpu.GPUFreeMem < gpu_mem {
					continue
				}//如果该GPU空闲资源小于请求的资源，则不选择该GPU
				taskNum := len(gpu.ResourceBesteffort)
				if gpu.ResourceGuaranteed == nil && gpu.ResourceBurstable == nil {
					tryBestNode(int64((taskNum+1) * 100 + int(Balance)), nodeName, id)
				}
				if gpu.ResourceGuaranteed == nil && gpu.ResourceBurstable != nil {
					GPU_Free := gpu.GPUFreeReq
					tryBestNode(int64((taskNum+1) * 100 + 1000 - int(GPU_Free)), nodeName, id)
				}
				if gpu.ResourceGuaranteed == nil && gpu.ResourceBurstable == nil {
					tryBestNode(int64((taskNum+1) * 100 + int(Balance) + 1000), nodeName, id)
				}
				findTheHole = true
			}
			if !findTheHole {
				if nodeRes.GpuFreeCount > 0 {
					tryBestNode(0, nodeName, gpusharev1.NewGPUID(5)) //选择并创建新的vGPU加入gpu pool，并赋予GPUID
				}
			}
		} else {
			tryBestNode(nodeRes.CpuFree-cpuReqTotal, nodeName, "")
		}
		wait.Done()
	}

	wait.Add(len(nodeResources))

	//从node资源列表中按照调度算法选择最合适的节点以及GPU，返回节点名称和GPUID
	for nodeName, nodeRes := range nodeResources { //此处的nodeResources中的GPUFree为经过过滤器过滤后的每个节点上的GPUID
		go scheduleNode(nodeName, nodeRes)
	}
	wait.Wait()

	return bestNode.NodeName, bestNode.GPUID

}