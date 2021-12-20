package gpu_scheduler

import (
	"container/list"
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"strconv"
	"sync"
	"time"

	//appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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

	gpusharev1 "gpushare/api/gpushare/v1"
	clientset "gpushare/pkg/client/clientset/versioned"
	gpusharescheme "gpushare/pkg/client/clientset/versioned/scheme"
	informers "gpushare/pkg/client/informers/externalversions/gpushare/v1"
	listers "gpushare/pkg/client/listers/gpushare/v1"
)

const controllerAgentName = "gpushare-scheduler"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Foo fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by VirtualPod"
	// MessageResourceSynced is the message used for an Event fired when a Foo
	// is synced successfully
	MessageResourceSynced = "VirtualPod scheduled successfully"

	GpuShareResourceGuaranteed   = "gpushare/resource_guaranteed"
	GpuShareResourceBurstable = "gpushare/resource_burstable"
	GpuShareResourceBesteffort    = "gpushare/resource_besteffort"
)


// Controller is the controller implementation for GpuShare resources
type Controller struct {
	kubeclientset      kubernetes.Interface
	gpushareclientset clientset.Interface

	nodesLister     corelisters.NodeLister
	nodesSynced     cache.InformerSynced
	podsLister      corelisters.PodLister
	podsSynced      cache.InformerSynced
	virtualpodsLister listers.VirtualPodLister
	virtualpodsSynced cache.InformerSynced
	//Controller的关键成员即三个事件的Listener(corelisters.NodeLister   corelisters.PodLister   listers.VirtualPodLister)
	//这三个成员将由 main 函数传入参数进行初始化

	workqueue workqueue.RateLimitingInterface
	recorder  record.EventRecorder
	//此外，为了缓冲事件处理，这里使用队列暂存事件，相关成员即为 workqueue.RateLimitingInterface
	//record.EventRecorder 用于记录事件

	pendingList    *list.List
	pendingListMux *sync.Mutex
}

func NewController(
	kubeclientset kubernetes.Interface,
	gpushareclientset clientset.Interface,
	nodeInformer coreinformers.NodeInformer,
	podInformer coreinformers.PodInformer,
	gpushareInformer informers.VirtualPodInformer) *Controller {

	utilruntime.Must(gpusharescheme.AddToScheme(scheme.Scheme))
	//将 gpushare-controller 的类型信息（gpushare）添加到默认 Kubernetes Scheme，以便能够记录到其事件
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	//基于新 Scheme 创建一个事件记录 recorder ，用于记录来自 “kubeshare-controller” 的事件

	//基于函数入参及刚刚构造的 recorder，初始化 Controller
	controller := &Controller{
		kubeclientset:      kubeclientset,
		gpushareclientset: gpushareclientset,
		nodesLister:        nodeInformer.Lister(),
		nodesSynced:        nodeInformer.Informer().HasSynced,
		podsLister:         podInformer.Lister(),
		podsSynced:         podInformer.Informer().HasSynced,
		virtualpodsLister:    gpushareInformer.Lister(),
		virtualpodsSynced:    gpushareInformer.Informer().HasSynced,
		workqueue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "SharePods"),
		recorder:           recorder,
		pendingList:        list.New(),
		pendingListMux:     &sync.Mutex{},
	}

	klog.Info("Setting up event handlers")
	//设置对gpushare、pod、node资源变化的事件处理函数（Add、Update 均通过 enqueueFoo 处理）

	gpushareInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.enqueueVirtualPod,
		DeleteFunc: controller.resourceChanged,
	})

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: controller.resourceChanged,
	})

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: controller.resourceChanged,
	})

	return controller //返回初始化的Controller
}

//enqueueVirtualPod 就是解析 VirtualPod 资源为 namespace/name 形式的字符串，然后入队
func (c *Controller) enqueueVirtualPod(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

func (c *Controller) resourceChanged(obj interface{}) {
	// push pending SharePods into workqueue
	c.pendingListMux.Lock()
	for p := c.pendingList.Front(); p != nil; p = p.Next() {
		c.workqueue.Add(p.Value)
	}
	c.pendingList.Init()
	c.pendingListMux.Unlock()
}

func (c *Controller) pendingInsurance(ticker *time.Ticker, done *chan bool) {
	for {
		select {
		case <-(*done):
			return
		case <-ticker.C:
			c.resourceChanged(nil)
		}
	}
}
// handleObject will take any resource implementing metav1.Object and attempt
// to find the Foo resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Foo resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.

//handleObject 监听了所有实现了 metav1 的资源，但只过滤出 owner 是 gpushare 的，
//将其解析为 namespace/name 入队
//设置对 Deployment 资源变化的事件处理函数（Add、Update、Delete 均通过 handleObject 处理）
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
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Foo, we should not do anything more
		// with it.
		if ownerRef.Kind != "VirtualPod" {
			return
		}

		virtualpod, err := c.virtualpodsLister.VirtualPods(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			klog.V(4).Infof("ignoring orphaned object '%s' of foo '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		c.enqueueVirtualPod(virtualpod)
		return
	}
}

// Run
///*等待 Informer 同步完成; 并发 runWorker，处理队列内事件*/
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.Info("Starting gpushare controller")

	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podsSynced, c.virtualpodsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}//等待 Informer 同步完成

	klog.Info("Starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}//并发 runWorker，处理队列内事件

	pendingInsuranceTicker := time.NewTicker(5 * time.Second)
	pendingInsuranceDone := make(chan bool)
	go c.pendingInsurance(pendingInsuranceTicker, &pendingInsuranceDone)

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")
	pendingInsuranceTicker.Stop()
	pendingInsuranceDone <- true

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the workqueue.
// runWorker 是一个长时间运行的函数，它将不断调用
// processNextWorkItem 函数是为了读取和处理工作队列上的消息。
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
// 从队列取出待处理对象; 调用syncHandler处理。
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		//通过调用syncHandler()函数，来处理队列中的任务
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		// 最后，如果没有发生错误，我们就忘记这个任务，这样它就不会再次排队，直到发生另一次更改。
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

func (c *Controller) syncHandler(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key) //获取namespace和name
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	virtualpod, err := c.virtualpodsLister.VirtualPods(namespace).Get(name)
	if err!= nil {
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("VirtualPod '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	//Spec中的nodename不为空则表明已经被调度
	if virtualpod.Spec.NodeName != "" {
		utilruntime.HandleError(fmt.Errorf("VirtualPod '%s' NodeName had been scheduled.", key))
		return nil
	}

	isGPUPod := false
	gpu_request := 0.0
	gpu_limit := 0.0
	resource_priority := int64(0)
	gpu_memory := int64(0)


	//获取Spec中的上面三个参数，并将isGPUPod置为真
	if virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest] != "" ||
		virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit] != "" ||
		virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory] != "" ||
		virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority] != ""{
		var err error
		gpu_limit, err = strconv.ParseFloat(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPULimit], 64)
		if err != nil || gpu_limit > 1.0 || gpu_limit < 0.0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPULimit))
			return nil
		}
		gpu_request, err = strconv.ParseFloat(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPURequest], 64)
		if err != nil || gpu_request > gpu_limit || gpu_request < 0.0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPURequest))
			return nil
		}
		gpu_memory, err = strconv.ParseInt(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUMemory], 10, 64)
		if err != nil || gpu_memory < 0 {
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourceGPUMemory))
			return nil
		}
		resource_priority, err = strconv.ParseInt(virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourcePriority], 10, 64)
		if err != nil || resource_priority < 0 || resource_priority > 2{
			utilruntime.HandleError(fmt.Errorf("VirtualPod %s/%s value error: %s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name, gpusharev1.GPUShareResourcePriority))
			return nil
		}
		isGPUPod = true
	}

	if isGPUPod && virtualpod.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID] != "" {
		utilruntime.HandleError(fmt.Errorf("virtualpod '%s' GPUID had been scheduled.", key))
		return nil
	}

	nodeList, err := c.nodesLister.List(labels.Everything())
	if err != nil {
		return err
	}
	podList, err := c.podsLister.List(labels.Everything())
	if err != nil {
		return err
	}
	virtualPodList, err := c.virtualpodsLister.List(labels.Everything())
	if err != nil {
		return err
	}

	klog.Infof("VirtualPod '%s' , resource_priority '%d' ", key, resource_priority)
	//根据自定义调度规则对将pod调度到相应的node节点上，并获取要调度的NodeName和GPUID
	scheduled_Node, scheduled_GPUID := scheduleVirtualPod(isGPUPod, gpu_request, gpu_memory,resource_priority, virtualpod, nodeList, podList, virtualPodList)
	if scheduled_Node == "" {
		klog.Infof("No enough resources for VirtualPod: %s/%s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name)
		c.pendingListMux.Lock()
		c.pendingList.PushBack(key)
		c.pendingListMux.Unlock()
		return nil
	}
	klog.Infof("VirtualPod '%s' had been scheduled to node '%s' ", key, scheduled_Node, scheduled_GPUID)

	//绑定到要调度的节点上，创建新的virtualpod并更新virtualpod
	if err := c.bindVirtualPodToNode(virtualpod, scheduled_Node, scheduled_GPUID); err != nil {
		return err
	}

	c.recorder.Event(virtualpod, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (c *Controller) bindVirtualPodToNode(virtualpod *gpusharev1.VirtualPod, schedNode, schedGPUID string) error {
	virtualpodCopy := virtualpod.DeepCopy()  //DeepCopy是一个自动生成的deepcopy函数，复制接收者，创建一个新的SharePod。
	virtualpodCopy.Spec.NodeName = schedNode
	if schedGPUID != "" {
		if virtualpodCopy.ObjectMeta.Annotations != nil {
			virtualpodCopy.ObjectMeta.Annotations[gpusharev1.GPUShareResourceGPUID] = schedGPUID
		} else {
			virtualpodCopy.ObjectMeta.Annotations = map[string]string{gpusharev1.GPUShareResourceGPUID: schedGPUID}
		}
	}

	_, err := c.gpushareclientset.GpushareV1().VirtualPods(virtualpodCopy.Namespace).Update(context.Background(),virtualpodCopy,metav1.UpdateOptions{})
	return err
}


