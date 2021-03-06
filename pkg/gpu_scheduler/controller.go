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

	gpusharev1 "github.com/wangzhuang12306/GpuShare/api/gpushare/v1"
	clientset "github.com/wangzhuang12306/GpuShare/pkg/client/clientset/versioned"
	gpusharescheme "github.com/wangzhuang12306/GpuShare/pkg/client/clientset/versioned/scheme"
	informers "github.com/wangzhuang12306/GpuShare/pkg/client/informers/externalversions/gpushare/v1"
	listers "github.com/wangzhuang12306/GpuShare/pkg/client/listers/gpushare/v1"
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
	//Controller?????????????????????????????????Listener(corelisters.NodeLister   corelisters.PodLister   listers.VirtualPodLister)
	//????????????????????? main ?????????????????????????????????

	workqueue workqueue.RateLimitingInterface
	recorder  record.EventRecorder
	//??????????????????????????????????????????????????????????????????????????????????????? workqueue.RateLimitingInterface
	//record.EventRecorder ??????????????????

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
	//??? gpushare-controller ??????????????????gpushare?????????????????? Kubernetes Scheme?????????????????????????????????
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	//????????? Scheme ???????????????????????? recorder ????????????????????? ???kubeshare-controller??? ?????????

	//???????????????????????????????????? recorder???????????? Controller
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
	//?????????gpushare???pod???node????????????????????????????????????Add???Update ????????? enqueueFoo ?????????

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

	return controller //??????????????????Controller
}

//enqueueVirtualPod ???????????? VirtualPod ????????? namespace/name ?????????????????????????????????
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

//handleObject ???????????????????????? metav1 ??????????????????????????? owner ??? gpushare ??????
//??????????????? namespace/name ??????
//????????? Deployment ????????????????????????????????????Add???Update???Delete ????????? handleObject ?????????
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
///*?????? Informer ????????????; ?????? runWorker????????????????????????*/
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.Info("Starting gpushare controller")

	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podsSynced, c.virtualpodsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}//?????? Informer ????????????

	klog.Info("Starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}//?????? runWorker????????????????????????

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
// runWorker ??????????????????????????????????????????????????????
// processNextWorkItem ?????????????????????????????????????????????????????????
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
// ??????????????????????????????; ??????syncHandler?????????
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

		//????????????syncHandler()????????????????????????????????????
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		// ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
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
	namespace, name, err := cache.SplitMetaNamespaceKey(key) //??????namespace???name
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

	//Spec??????nodename?????????????????????????????????
	if virtualpod.Spec.NodeName != "" {
		utilruntime.HandleError(fmt.Errorf("VirtualPod '%s' NodeName had been scheduled.", key))
		return nil
	}

	isGPUPod := false
	gpu_request := 0.0
	gpu_limit := 0.0
	resource_priority := int64(0)
	gpu_memory := int64(0)


	//??????Spec?????????????????????????????????isGPUPod?????????
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
	//?????????????????????????????????pod??????????????????node?????????????????????????????????NodeName???GPUID
	scheduled_Node, scheduled_GPUID := scheduleVirtualPod(isGPUPod, gpu_request, gpu_memory,resource_priority, virtualpod, nodeList, podList, virtualPodList)
	if scheduled_Node == "" {
		klog.Infof("No enough resources for VirtualPod: %s/%s", virtualpod.ObjectMeta.Namespace, virtualpod.ObjectMeta.Name)
		c.pendingListMux.Lock()
		c.pendingList.PushBack(key)
		c.pendingListMux.Unlock()
		return nil
	}
	klog.Infof("VirtualPod '%s' had been scheduled to node '%s' ", key, scheduled_Node, scheduled_GPUID)

	//?????????????????????????????????????????????virtualpod?????????virtualpod
	if err := c.bindVirtualPodToNode(virtualpod, scheduled_Node, scheduled_GPUID); err != nil {
		return err
	}

	c.recorder.Event(virtualpod, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (c *Controller) bindVirtualPodToNode(virtualpod *gpusharev1.VirtualPod, schedNode, schedGPUID string) error {
	virtualpodCopy := virtualpod.DeepCopy()  //DeepCopy????????????????????????deepcopy?????????????????????????????????????????????SharePod???
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


