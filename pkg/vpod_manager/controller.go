package vpod_manager

import (
	"context"
	"fmt"
	"k8s.io/klog/v2"
	"strconv"
	"strings"
	"time"
	//appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	gpusharev1 "gpushare/api/gpushare/v1"
	clientset "gpushare/pkg/client/clientset/versioned"
	gpusharescheme "gpushare/pkg/client/clientset/versioned/scheme"
	informers "gpushare/pkg/client/informers/externalversions/gpushare/v1"
	listers "gpushare/pkg/client/listers/gpushare/v1"
	// "k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const controllerAgentName = "gpu-vpod-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a VirtualPod is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a VirtualPod fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	ErrValueError = "ErrValueError"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by VirtualPod"
	// MessageResourceSynced is the message used for an Event fired when a VirtualPod
	// is synced successfully
	MessageResourceSynced = "VirtualPod synced successfully"

	GPUShareLibraryPath = "/gpushare/library"
	SchedulerIpPath      = GPUShareLibraryPath + "/schedulerIP.txt"
	PodManagerPortStart  = 50050
)

type Controller struct {
	kubeclientset      kubernetes.Interface
	gpushareclientset clientset.Interface

	podsLister      corelisters.PodLister
	podsSynced      cache.InformerSynced
	virtualpodsLister listers.VirtualPodLister
	virtualpodsSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	//工作队列是一个速率受限的工作队列。 这用于将要处理的工作排队，而不是在发生更改时立即执行。
	//这意味着我们可以确保我们一次只处理固定数量的资源，并且可以轻松确保我们永远不会在两个不同的工作人员中同时处理相同的项目。
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	gpushareclientset clientset.Interface,
	podInformer coreinformers.PodInformer,
	gpushareInformer informers.VirtualPodInformer) *Controller {

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	utilruntime.Must(gpusharescheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:      kubeclientset,
		gpushareclientset:  gpushareclientset,
		podsLister:         podInformer.Lister(),
		podsSynced:         podInformer.Informer().HasSynced,
		virtualpodsLister:  gpushareInformer.Lister(),
		virtualpodsSynced:  gpushareInformer.Informer().HasSynced,
		workqueue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "VirtualPods"),
		recorder:           recorder,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when VirtualPod resources change
	gpushareInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{  //向informer添加eventhandler，控制器处理特定资源通知的地方
		AddFunc: controller.enqueueVirtualPod,    // 当资源第一次加入到Informer的缓存后调用
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueVirtualPod(new)
		},//当既有资源被修改时调用。oldObj是资源的上一个状态，newObj则是新状态，它获取资源的最终状态（如果能获取到的话），否则它会获取对象的一个
		DeleteFunc: controller.handleDeletedVirtualPod, // 当既有资源被删除时调用，obj是对象的最后状态
	})
	// Set up an event handler for when Deployment resources change. This
	// handler will lookup the owner of the given Deployment, and if it is
	// owned by a VirtualPod resource will enqueue that VirtualPod resource for
	// processing. This way, we don't need to implement custom logic for
	// handling Deployment resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			newDepl := new.(*corev1.Pod)
			oldDepl := old.(*corev1.Pod)
			if newDepl.ResourceVersion == oldDepl.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				return
			}
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})

	return controller
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()


	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting VirtualPod controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podsSynced, c.virtualpodsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}//等待 Informer 同步完成

	if err := c.initNodesInfo(); err != nil {
		return fmt.Errorf("failed to init NodeClient: %s", err)
	}
	c.cleanOrphanClientPod()
	// clientHandler in ConfigManager must have correct PodList of every VirtualPods,
	// so call it after initNode
	//ClientConfigManager 中的clientHandler必须有正确的每个 VirtualPods 的 PodList，所以在 initNodeClient 之后调用它

	go StartConfigManager(stopCh, c.kubeclientset)

	klog.Info("Starting workers")
	// Launch two workers to process VirtualPod resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		//我们在这里调用 Done，这样工作队列就知道我们已经完成了这个项目的处理。
		//如果我们不希望此工作项重新排队，我们还必须记住调用 Forget。
		//例如，如果发生暂时性错误，我们不会调用 Forget，而是将项目放回工作队列并在退避期后再次尝试。
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		//我们希望字符串脱离工作队列。 它们的形式为命名空间/名称。
		//我们这样做是因为工作队列的延迟性质意味着 Informer 缓存中的项目实际上可能比项目最初放入工作队列时更新。
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// VirtualPod resource to be synced.
		//运行 syncHandler，将要同步的 VirtualPod 资源的命名空间/名称字符串传递给它。
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}




// syncHandler returns error when we want to re-process the key, otherwise returns nil
// 当我们想要重新处理密钥时，syncHandler 返回错误，否则返回 nil
func (c *Controller) syncHandler(key string) error {

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	virtualpod, err := c.virtualpodsLister.VirtualPods(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("VirtualPod '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	if virtualpod.Spec.NodeName == "" {
		utilruntime.HandleError(fmt.Errorf("VirtualPod '%s' must be scheduled! Spec.NodeName is empty.", key))
		return nil
	}

	isGPUPod := false
	gpu_request := 0.0
	gpu_limit := 0.0
	gpu_mem := int64(0)
	sche_priority := int64(2)
	GPUID := ""
	physicalGPUuuid := ""
	physicalGPUport := 0

	// GPU Pod needs to be filled with request, limit, memory, and GPUID, or none of them.
	// If something weird, reject it (record the reason to user then return nil)

	if virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest] != "" ||
		virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit] != "" ||
		virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory] != "" ||
		virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID] != "" {
		var err error
		gpu_limit, err = strconv.ParseFloat(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit], 64)
		if err != nil || gpu_limit > 1.0 || gpu_limit < 0.0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s gpu_limit value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPULimit))
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Value error: "+gpusharev1.GPUShareResourceGPULimit)
			return nil
		}
		gpu_request, err = strconv.ParseFloat(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest], 64)
		if err != nil || gpu_request > gpu_limit || gpu_request < 0.0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s gpu_request value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPURequest))
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Value error: "+gpusharev1.GPUShareResourceGPURequest)
			return nil
		}
		gpu_mem, err = strconv.ParseInt(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory], 10, 64)
		if err != nil || gpu_mem < 0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s gpu_mem value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPUMemory))
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Value error: "+gpusharev1.GPUShareResourceGPUMemory)
			return nil
		}
		sche_priority, err = strconv.ParseInt(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority], 10, 64)
		if err != nil || gpu_mem < 0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s sche_priority value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourcePriority))
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Value error: "+gpusharev1.GPUShareResourcePriority)
			return nil
		}
		GPUID = virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID]
		if len(GPUID) == 0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s GPUID value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPUID))
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Value error: "+gpusharev1.GPUShareResourceGPUID)
			return nil
		}
		isGPUPod = true
	}

	// virtualpod.Print()
	//检查vgpu pool中是否有申请的GPUID的设备，如果有，则返回该GPUID的设备的uuid，并将该virtualpod的BoundDeviceID设置为该uuid。
	//如果该GPUID不存在于vgpu pool中，则创建一个client pod，该pod将申请GPU并返回该GPU的uuid，将此uuid与申请的GPUID绑定。
	if isGPUPod && virtualpod.Status.BoundDeviceID == "" {
		var errCode int
		physicalGPUuuid, errCode = c.getPhysicalGPUuuid(virtualpod.Spec.NodeName, GPUID, gpu_request, gpu_limit, gpu_mem, sche_priority, key, &physicalGPUport)
		switch errCode {
		case 0:
			klog.Infof("VirtualPod %s is bound to GPU uuid: %s", key, physicalGPUuuid)
		case 1:
			klog.Infof("VirtualPod %s/%s is waiting for client Pod", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name)
			return nil
		case 2:
			err := fmt.Errorf("Resource exceed!")
			utilruntime.HandleError(err)
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Resource exceed")
			return err
		case 3:
			err := fmt.Errorf("Pod manager port pool is full!")
			utilruntime.HandleError(err)
			return err
		default:
			utilruntime.HandleError(fmt.Errorf("Unknown Error"))
			c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrValueError, "Unknown Error")
			return nil
		}
		virtualpod.Status.BoundDeviceID = physicalGPUuuid
	}

	pod, err := c.podsLister.Pods(virtualpod.ObjectMeta.Namespace).Get(virtualpod.ObjectMeta.Name)

	// If the resource doesn't exist, we'll create it, but don't create when we knew that Pod will not restart forever
	if errors.IsNotFound(err) && (virtualpod.Status.PodStatus == nil ||
		virtualpod.Spec.RestartPolicy == corev1.RestartPolicyAlways ||
		(virtualpod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure && virtualpod.Status.PodStatus.Phase != corev1.PodSucceeded) ||
		(virtualpod.Spec.RestartPolicy == corev1.RestartPolicyNever && (virtualpod.Status.PodStatus.Phase != corev1.PodSucceeded && virtualpod.Status.PodStatus.Phase != corev1.PodFailed))) {
		if n, ok := nodesInfo[virtualpod.Spec.NodeName]; ok {
			pod, err = c.kubeclientset.CoreV1().Pods(virtualpod.ObjectMeta.Namespace).Create(context.Background(), newPod(virtualpod, isGPUPod, n.PodIP, physicalGPUport), metav1.CreateOptions{})
		} //为该virtualpod创建一个pod，并设置容器的环境变量，包括NVIDIA_VISIBLE_DEVICES、设备库路径、pod_manager IP和port、挂载等，并标记该pod属于此virtualpod。
	}

	if err != nil {
		return err
	}

	if !metav1.IsControlledBy(pod, virtualpod) {
		msg := fmt.Sprintf(MessageResourceExists, pod.Name)
		c.recorder.Event(virtualpod, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf(msg)
	}

	if (pod.Spec.RestartPolicy == corev1.RestartPolicyNever && (pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed)) ||
		(pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure && pod.Status.Phase == corev1.PodSucceeded) {
		go c.removeVirtualPodFromList(virtualpod)
	} //如果该virtualpod控制的pod已经完成或失败，且根据重启策略已经不需要重启了，则将该virtualpod从virtualpod列表中移除。

	err = c.updateVirtualPodStatus(virtualpod, pod, physicalGPUport)  //最后，更新virtualpod的状态，包括PodStatus、PodObjectMeta和PodManagerPort
	if err != nil {
		return err
	}

	c.recorder.Event(virtualpod, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (c *Controller) updateVirtualPodStatus(virtualpod *gpusharev1.VirtualPod, pod *corev1.Pod, port int) error {
	virtualpodCopy := virtualpod.DeepCopy()
	virtualpodCopy.Status.PodStatus = pod.Status.DeepCopy()
	virtualpodCopy.Status.PodObjectMeta = pod.ObjectMeta.DeepCopy()
	if port != 0 {
		virtualpodCopy.Status.PodManagerPort = port
	}

	_, err := c.gpushareclientset.GpushareV1().VirtualPods(virtualpodCopy.Namespace).Update(context.Background(), virtualpodCopy, metav1.UpdateOptions{}) //更新virtualpod的状态，包括PodStatus、PodObjectMeta和PodManagerPort
	return err
}


func (c *Controller) enqueueVirtualPod(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) handleDeletedVirtualPod(obj interface{}) {
	virtualpod, ok := obj.(*gpusharev1.VirtualPod)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("handleDeletedVirtualPod: cannot parse object"))
		return
	}
	go c.removeVirtualPodFromList(virtualpod)
}

func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	klog.V(4).Infof("Processing object: %s", object.GetName())

	// get physical GPU UUID from client Pod
	// get UUID here to prevent request throttling
	if pod, ok := obj.(*corev1.Pod); ok {
		if pod.ObjectMeta.Namespace == "kube-system" && strings.Contains(pod.ObjectMeta.Name, gpusharev1.GPUShareClientPodName) && (pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodFailed) {
			// TODO: change the method of getting GPUID from label to more reliable source
			// e.g. Pod name (gpushare-clientpod-{NodeName}-{GPUID})
			if pod.Spec.NodeName != "" {
				if gpuid, ok := pod.ObjectMeta.Labels[gpusharev1.GPUShareResourceGPUID]; ok {
					needSetUUID := false
					nodesInfoMux.Lock()
					// klog.Infof("ERICYEH1: %#v", nodeClients[pod.Spec.NodeName])
					if node, ok := nodesInfo[pod.Spec.NodeName]; ok {
						if _, ok := node.GPUID2GPU[gpuid]; ok && node.GPUID2GPU[gpuid].UUID == "" {
							needSetUUID = true
						}
					}
					nodesInfoMux.Unlock()
					if needSetUUID {
						klog.Infof("Start go routine to get UUID from client Pod")
						go c.getAndSetUUIDFromClientPod(pod.Spec.NodeName, gpuid, pod.ObjectMeta.Name, pod)
					}
				} else {
					klog.Errorf("Detect empty %s label from client Pod: %s", gpusharev1.GPUShareResourceGPUID, pod.ObjectMeta.Name)
				}
			} else {
				klog.Errorf("Detect empty NodeName from client Pod: %s", pod.ObjectMeta.Name)
			}
		}
	}

	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a virtualpod, we should not do anything more
		// with it.
		if ownerRef.Kind != "VirtualPod" {
			return
		}

		foo, err := c.virtualpodsLister.VirtualPods(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			klog.V(4).Infof("ignoring orphaned object '%s' of VirtualPod '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		c.enqueueVirtualPod(foo)
		return
	}
}

// newDeployment creates a new Deployment for a VirtualPod resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the VirtualPod resource that 'owns' it.
// newDeployment为VirtualPod资源创建一个新的Deployment。
// 它还在资源上设置适当的 OwnerReferences，以便handleObject可以发现“拥有”它的 VirtualPod 资源。
func newPod(virtualpod *gpusharev1.VirtualPod, isGPUPod bool, podManagerIP string, podManagerPort int) *corev1.Pod {
	specCopy := virtualpod.Spec.DeepCopy()
	labelCopy := make(map[string]string, len(virtualpod.ObjectMeta.Labels))
	for key, val := range virtualpod.ObjectMeta.Labels {
		labelCopy[key] = val
	}
	annotationCopy := make(map[string]string, len(virtualpod.ObjectMeta.Annotations)+5)
	for key, val := range virtualpod.ObjectMeta.Annotations {
		annotationCopy[key] = val
	}
	if isGPUPod {
		for i := range specCopy.Containers {
			c := &specCopy.Containers[i]
			c.Env = append(c.Env,
				corev1.EnvVar{
					Name:  "NVIDIA_VISIBLE_DEVICES",
					Value: virtualpod.Status.BoundDeviceID,
				},
				corev1.EnvVar{
					Name:  "NVIDIA_DRIVER_CAPABILITIES",
					Value: "compute,utility",
				},
				corev1.EnvVar{
					Name:  "LD_PRELOAD",
					Value: GPUShareLibraryPath + "/libgemhook.so.1",
				},
				corev1.EnvVar{
					Name:  "POD_MANAGER_IP",
					Value: podManagerIP,
				},
				corev1.EnvVar{
					Name:  "POD_MANAGER_PORT",
					Value: fmt.Sprintf("%d", podManagerPort),
				},
				corev1.EnvVar{
					Name:  "POD_NAME",
					Value: fmt.Sprintf("%s/%s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name),
				},
			)
			c.VolumeMounts = append(c.VolumeMounts,
				corev1.VolumeMount{
					Name:      "gpushare-lib",
					MountPath: GPUShareLibraryPath,
				},
			)
		}
		specCopy.Volumes = append(specCopy.Volumes,
			corev1.Volume{
				Name: "gpushare-lib",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: GPUShareLibraryPath,
					},
				},
			},
		)
		annotationCopy[gpusharev1.GPUShareResourceGPURequest] = virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest]
		annotationCopy[gpusharev1.GPUShareResourceGPULimit] = virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit]
		annotationCopy[gpusharev1.GPUShareResourceGPUMemory] = virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory]
		annotationCopy[gpusharev1.GPUShareResourceGPUID] = virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID]
		annotationCopy[gpusharev1.GPUShareResourcePriority] = virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority]
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      virtualpod.ObjectMeta.Name,
			Namespace: virtualpod.ObjectMeta.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(virtualpod, schema.GroupVersionKind{
					Group:   gpusharev1.SchemeGroupVersion.Group,
					Version: gpusharev1.SchemeGroupVersion.Version,
					Kind:    "VirtualPod",
				}),
			},
			Annotations: annotationCopy,
			Labels:      labelCopy,
		},
		Spec: *specCopy,
	}
}