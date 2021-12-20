package vpod_manager

import (
	"container/list"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	gpusharev1 "gpushare/api/gpushare/v1"
	"gpushare/pkg/lib/bitmap"
)

var (
	ResourceQuantity1 = resource.MustParse("1")
)

type PodRequest struct {
	Key            string
	Request        float64
	Limit          float64
	Memory         int64
	Priority	   int64
	PodManagerPort int
}

type GPUInfo struct {
	UUID    string
	Usage   float64
	Mem     int64
	PodList *list.List
}

type NodeInfo struct {
	// GPUID -> GPU ， GPUID与真实GPU之间的映射
	GPUID2GPU map[string]*GPUInfo
	// UUID -> Port (string)
	UUID2Port map[string]string

	// port in use
	PodManagerPortBitmap *bitmap.RRBitmap
	PodIP                string
}

var (
	nodesInfo    map[string]*NodeInfo = make(map[string]*NodeInfo)
	nodesInfoMux sync.Mutex
)

func (c *Controller) initNodesInfo() error {
	var pods []*corev1.Pod
	var virtualpods []*gpusharev1.VirtualPod
	var err error

	clientPodsLabel := labels.SelectorFromSet(labels.Set{gpusharev1.GPUShareRole: "clientPod"})
	if pods, err = c.podsLister.Pods("kube-system").List(clientPodsLabel); err != nil {
		errrr := fmt.Errorf("Error when list Pods: %s", err)
		klog.Error(errrr)
		return errrr
	}
	if virtualpods, err = c.virtualpodsLister.List(labels.Everything()); err != nil {
		errrr := fmt.Errorf("Error when list virtualpods: %s", err)
		klog.Error(errrr)
		return errrr
	}

	nodesInfoMux.Lock()
	defer nodesInfoMux.Unlock()

	for _, pod := range pods {
		GPUID := ""
		if gpuid, ok := pod.ObjectMeta.Labels[gpusharev1.GPUShareResourceGPUID]; !ok {
			klog.Errorf("Error client Pod annotation: %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			continue
		} else {
			GPUID = gpuid
		}
		if node, ok := nodesInfo[pod.Spec.NodeName]; !ok {
			bm := bitmap.NewRRBitmap(512)
			bm.Mask(0)
			node = &NodeInfo{
				GPUID2GPU:            make(map[string]*GPUInfo),
				PodManagerPortBitmap: bm,
			}
			node.GPUID2GPU[GPUID] = &GPUInfo{
				UUID:    "",
				Usage:   0.0,
				Mem:     0,
				PodList: list.New(),
			}
			nodesInfo[pod.Spec.NodeName] = node
		} else {
			_, ok := node.GPUID2GPU[GPUID]
			if ok {
				klog.Errorf("Duplicated GPUID '%s' on node '%s'", GPUID, pod.Spec.NodeName)
				continue
			}
			node.GPUID2GPU[GPUID] = &GPUInfo{
				UUID:    "",
				Usage:   0.0,
				Mem:     0,
				PodList: list.New(),
			}
		}
	}

	type processClientPodLaterItem struct {
		NodeName string
		GPUID    string
	}
	var processClientPodLaterList []processClientPodLaterItem

	for _, virtualpod := range virtualpods {
		gpu_request := 0.0
		gpu_limit := 0.0
		gpu_mem := int64(0)
		sche_priority := int64(2)
		GPUID := ""

		var err error
		gpu_limit, err = strconv.ParseFloat(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit], 64)
		if err != nil || gpu_limit > 1.0 || gpu_limit < 0.0 {
			continue
		}
		gpu_request, err = strconv.ParseFloat(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest], 64)
		if err != nil || gpu_request > gpu_limit || gpu_request < 0.0 {
			continue
		}
		gpu_mem, err = strconv.ParseInt(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory], 10, 64)
		if err != nil || gpu_mem < 0 {
			continue
		}
		sche_priority, err = strconv.ParseInt(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority], 10, 64)
		if err != nil || sche_priority < 0 {
			continue
		}
		// after this line, virtualpod requires GPU

		// this virtualpod may not be scheduled yet
		if virtualpod.Spec.NodeName == "" {
			continue
		}
		// if Spec.NodeName is assigned but GPUID is empty, it's an error
		if gpuid, ok := virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID]; !ok {
			continue
		} else {
			GPUID = gpuid
		}

		node, ok := nodesInfo[virtualpod.Spec.NodeName]
		if !ok {
			klog.Errorf("virtualpod '%s/%s' doesn't have corresponding client Pod!", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name)
			continue
		}
		gpu, ok := node.GPUID2GPU[GPUID]
		if !ok {
			klog.Errorf("virtualpod '%s/%s' doesn't have corresponding client Pod!", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name)
			continue
		}

		gpu.Usage += gpu_request
		gpu.Mem += gpu_mem
		gpu.PodList.PushBack(&PodRequest{
			Key:            fmt.Sprintf("%s/%s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name),
			Request:        gpu_request,
			Limit:          gpu_limit,
			Memory:         gpu_mem,
			Priority:       sche_priority,
			PodManagerPort: virtualpod.Status.PodManagerPort,
		})
		node.PodManagerPortBitmap.Mask(virtualpod.Status.PodManagerPort - PodManagerPortStart)

		if virtualpod.Status.BoundDeviceID != "" {
			if gpu.UUID == "" {
				gpu.UUID = virtualpod.Status.BoundDeviceID
			}
		} else {
			if gpu.UUID != "" {
				c.workqueue.Add(fmt.Sprintf("%s/%s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name))
			} else {
				notFound := true
				for _, item := range processClientPodLaterList {
					if item.NodeName == virtualpod.Spec.NodeName && item.GPUID == GPUID {
						notFound = false
					}
				}
				if notFound {
					processClientPodLaterList = append(processClientPodLaterList, processClientPodLaterItem{
						NodeName: virtualpod.Spec.NodeName,
						GPUID:    GPUID,
					})
				}
			}
		}
	}

	for _, item := range processClientPodLaterList {
		go c.createClientPod(item.NodeName, item.GPUID)
	}

	return nil
}

func (c *Controller) cleanOrphanClientPod() {
	nodesInfoMux.Lock()
	defer nodesInfoMux.Unlock()

	for nodeName, node := range nodesInfo {
		for gpuid, gpu := range node.GPUID2GPU {
			if gpu.PodList.Len() == 0 {
				delete(node.GPUID2GPU, gpuid)
				c.deleteClientPod(nodeName, gpuid, gpu.UUID)
			}
		}
	}
}

func FindInQueue(key string, pl *list.List) (*PodRequest, bool) {
	for k := pl.Front(); k != nil; k = k.Next() {
		if k.Value.(*PodRequest).Key == key {
			return k.Value.(*PodRequest), true
		}
	}
	return nil, false
}

/* getPhysicalGPUuuid returns valid uuid if errCode==0
 * errCode 0: no error
 * errCode 1: need Client Pod
 * errCode 2: resource exceed
 * errCode 3: Pod manager port pool is full
 * errCode 255: other error
 */
func (c *Controller) getPhysicalGPUuuid(nodeName string, GPUID string, gpu_request, gpu_limit float64, gpu_mem int64, sche_priority int64, key string, port *int) (uuid string, errCode int) {

	nodesInfoMux.Lock()
	defer nodesInfoMux.Unlock()

	node, ok := nodesInfo[nodeName]  //获取nodeName上的node信息
	if !ok {
		msg := fmt.Sprintf("No client node: %s", nodeName)
		klog.Errorf(msg)
		return "", 255
	}

	//判断GPUID是否存在与vgpu pool中。如果不存在，则返回errCode=1，表示需要等待新创建的Client Pod成功创建
	if gpu, ok := node.GPUID2GPU[GPUID]; !ok { //GPU池中没有该GPUID
		gpu = &GPUInfo{
			UUID:    "",
			Usage:   gpu_request,
			Mem:     gpu_mem,
			PodList: list.New(),
		}
		tmp := node.PodManagerPortBitmap.FindNextFromCurrentAndSet() + PodManagerPortStart
		if tmp == -1 {
			klog.Errorf("Pod manager port pool is full!!!!!")
			return "", 3
		}
		*port = tmp
		gpu.PodList.PushBack(&PodRequest{
			Key:            key,
			Request:        gpu_request,
			Limit:          gpu_limit,
			Memory:         gpu_mem,
			Priority:       sche_priority,
			PodManagerPort: *port,
		})
		node.GPUID2GPU[GPUID] = gpu
		go c.createClientPod(nodeName, GPUID) //创建新的Client Pod，用于申请GPU并获取UUID，并将该GPU的UUID与GPUID进行映射。
		return "", 1

	} else { //GPU池中存在该GPUID：
		//检查podList中是否有请求的key(namespace/taskname)
		if podreq, isFound := FindInQueue(key, gpu.PodList); !isFound { //podList中没有请求的任务，更新GPU的usage、memory和port，并将该pod压入列表中
			if tmp := gpu.Usage + gpu_request; tmp > 1.0 {
				klog.Infof("Resource exceed, usage: %f, new_req: %f", gpu.Usage, gpu_request)
				return "", 2
			} else {
				gpu.Usage = tmp
			}
			gpu.Mem += gpu_mem
			tmp := node.PodManagerPortBitmap.FindNextFromCurrentAndSet() + PodManagerPortStart
			if tmp == -1 {
				klog.Errorf("Pod manager port pool is full!!!!!")
				return "", 3
			}
			*port = tmp
			//将该任务pod的信息压入该GPU下的PodList中
			gpu.PodList.PushBack(&PodRequest{
				Key:            key,
				Request:        gpu_request,
				Limit:          gpu_limit,
				Memory:         gpu_mem,
				Priority:       sche_priority,
				PodManagerPort: *port,
			})
		} else {
			*port = podreq.PodManagerPort
		}
		if gpu.UUID == "" {
			return "", 1
		} else {
			syncConfig(nodeName, gpu.UUID, gpu.PodList)
			return gpu.UUID, 0
		}
	}

	return "", 255
}

func (c *Controller) createClientPod(nodeName string, GPUID string) error {
	podName := fmt.Sprintf("%s-%s-%s", gpusharev1.GPUShareClientPodName, nodeName, GPUID) //client pod name: vgpu-nodeName-GPUID

	createit := func() error {
		klog.Infof("ERICYEH: creating client pod: %s", podName)
		// create a pod for client gpu then waiting for running, and get its gpu deviceID
		createdPod, err := c.kubeclientset.CoreV1().Pods("kube-system").Create(context.Background(),&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "kube-system",
				Labels: map[string]string{
					gpusharev1.GPUShareRole:          "clientPod",
					gpusharev1.GPUShareNodeName:      nodeName,
					gpusharev1.GPUShareResourceGPUID: GPUID,
				},
			},
			Spec: corev1.PodSpec{
				NodeName:                      nodeName,
				TerminationGracePeriodSeconds: new(int64),
				Containers: []corev1.Container{
					corev1.Container{
						Name:  "sleepforever",
						Image: "wangzhuang12306/gpushare-vgpupod:v1",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{gpusharev1.ResourceNVIDIAGPU: ResourceQuantity1},
							Limits:   corev1.ResourceList{gpusharev1.ResourceNVIDIAGPU: ResourceQuantity1},
						},
					},
				},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		},metav1.CreateOptions{})
		if err != nil {
			_, exists := c.kubeclientset.CoreV1().Pods("kube-system").Get(context.Background(), podName, metav1.GetOptions{})
			if exists != nil {
				klog.Errorf("Error when creating client pod: \nerror: '%s',\n podspec: %-v", err, createdPod)
				return err
			}
		}
		return nil
	}

	if clientpod, err := c.kubeclientset.CoreV1().Pods("kube-system").Get(context.Background(),podName, metav1.GetOptions{}); err != nil {
		//get该clientpod出现错误
		if errors.IsNotFound(err) {
			havetocreate := true
			nodesInfoMux.Lock()
			_, havetocreate = nodesInfo[nodeName].GPUID2GPU[GPUID] //正常时返回false，表示不需要再次创建clientpod
			nodesInfoMux.Unlock()
			if havetocreate {
				createit()
			}
		} else {
			msg := fmt.Sprintf("List Pods resource error! nodeName: %s", nodeName)
			klog.Errorf(msg)
			return err
		}
	} else {//clientpod正常创建
		if clientpod.ObjectMeta.DeletionTimestamp != nil {
			// TODO: If client Pod had been deleted, re-create it later
			klog.Warningf("Unhandled: client Pod %s is deleting! re-create it later!", podName)
		}
		if clientpod.Status.Phase == corev1.PodRunning || clientpod.Status.Phase == corev1.PodFailed {
			c.getAndSetUUIDFromClientPod(nodeName, GPUID, podName, clientpod) //从clientPod的环境变量中获取UUID，并将GPUID和UUID形成映射
		}
	}

	return nil
}

// clientPod status must be Running or Failed
// triggered from Pod event handler, preventing request throttling
func (c *Controller) getAndSetUUIDFromClientPod(nodeName, GPUID, podName string, clientPod *corev1.Pod) error {
	if clientPod.Status.Phase == corev1.PodFailed { //clientPod状态为失败时，删除该pod，并重新创建clientPod.
		c.kubeclientset.CoreV1().Pods("kube-system").Delete(context.Background(),podName, metav1.DeleteOptions{})
		time.Sleep(time.Second)
		c.createClientPod(nodeName, GPUID)
		err := fmt.Errorf("client Pod '%s' status failed, restart it.", podName)
		klog.Errorf(err.Error())
		return err
	}
	// clientPod.Status.Phase must be Running
	var uuid string
	rawlog, logerr := c.kubeclientset.CoreV1().Pods("kube-system").GetLogs(podName, &corev1.PodLogOptions{}).Do(context.Background()).Raw()
	if logerr != nil {
		err := fmt.Errorf("Error when get client pod's log! pod namespace/name: %s/%s, error: %s", "kube-system", podName, logerr)
		klog.Errorf(err.Error())
		return err
	}
	uuid = strings.Trim(string(rawlog), " \n\t")  //从clientPod的日志中获取GPU的UUID
	klog.Infof("client Pod %s get device ID: '%s'", podName, uuid)
	isFound := false
	for id := range nodesInfo[nodeName].UUID2Port {
		if id == uuid {
			isFound = true
		}
	}

	if !isFound {
		err := fmt.Errorf("Cannot find UUID '%s' from client Pod: '%s' in UUID database.", uuid, podName)
		klog.Errorf(err.Error())
		// possibly not print UUID yet, try again
		time.Sleep(time.Second)
		go c.createClientPod(nodeName, GPUID)
		return err
	}

	nodesInfoMux.Lock()
	defer nodesInfoMux.Unlock()

	nodesInfo[nodeName].GPUID2GPU[GPUID].UUID = uuid  //将GPUID与UUID形成映射

	PodList := nodesInfo[nodeName].GPUID2GPU[GPUID].PodList
	klog.Infof("After client Pod created, PodList Len: %d", PodList.Len())
	for k := PodList.Front(); k != nil; k = k.Next() {
		klog.Infof("Add MtgpuPod back to queue then process: %s", k.Value)
		c.workqueue.Add(k.Value.(*PodRequest).Key) //将该请求重新添加到工作队列中，重新处理
	}

	return nil
}

func (c *Controller) removeVirtualPodFromList(virtualpod *gpusharev1.VirtualPod) {
	nodeName := virtualpod.Spec.NodeName
	GPUID := virtualpod.Annotations[gpusharev1.GPUShareResourceGPUID]
	key := fmt.Sprintf("%s/%s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name)

	nodesInfoMux.Lock()

	if node, nodeOk := nodesInfo[nodeName]; nodeOk {
		if gpu, gpuOk := node.GPUID2GPU[GPUID]; gpuOk {
			podlist := gpu.PodList
			for pod := podlist.Front(); pod != nil; pod = pod.Next() {
				podRequest := pod.Value.(*PodRequest)
				if podRequest.Key == key {
					klog.Infof("Remove MtgpuPod %s from list, remaining %d MtgpuPod(s).", key, podlist.Len())
					podlist.Remove(pod)

					uuid := gpu.UUID
					remove := false

					if podlist.Len() == 0 {
						delete(node.GPUID2GPU, GPUID)
						remove = true
					} else {
						gpu.Usage -= podRequest.Request
						gpu.Mem -= podRequest.Memory
						syncConfig(nodeName, uuid, podlist)
					}
					node.PodManagerPortBitmap.Unmask(podRequest.PodManagerPort - PodManagerPortStart)

					nodesInfoMux.Unlock()

					if remove {
						c.deleteClientPod(nodeName, GPUID, uuid)
					}
					return
				}
			}
		}
	}
	nodesInfoMux.Unlock()
}

func (c *Controller) deleteClientPod(nodeName, GPUID, uuid string) {
	key := fmt.Sprintf("%s-%s-%s", gpusharev1.GPUShareClientPodName, nodeName, GPUID)
	klog.Infof("Deleting Client Pod: %s", key)
	c.kubeclientset.CoreV1().Pods("kube-system").Delete(context.Background(),key, metav1.DeleteOptions{})
	syncConfig(nodeName, uuid, nil)
}
