package main
import (
	"context"
	"flag"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"os"
	"time"

	"k8s.io/client-go/tools/clientcmd"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	clientset "gpushare/pkg/client/clientset/versioned"
	informers "gpushare/pkg/client/informers/externalversions"
	gpusharecontroller "gpushare/pkg/gpu_scheduler"
	"gpushare/pkg/signals"
)

var (
	masterURL  string
	kubeconfig string
)

func main() {

	//-  读取 kubeconfig 配置，构造用于事件监听的 Kubernetes Client
	//
	//这里创建了两个，一个监听普通事件，一个监听 gpusahre 事件
	//
	//-  基于 Client 构造监听相关的 informer
	//
	//-  基于 Client、Informer 初始化自定义 Controller，监听 Deployment 以及 Foos 资源变化
	//
	//-  开启 Controller

	klog.InitFlags(nil)
	flag.Parse()

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}
	gpushareClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building example clientset: %s", err.Error())
	} //读取 kubeconfig 配置，构造用于事件监听的 Kubernetes Client  这里创建了两个，一个监听普通事件，一个监听gpushare 事件

	if !checkCRD(gpushareClient) {
		klog.Error("CRD doesn't exist. Exiting")
		os.Exit(1)
	}
	//基于Client构造监听相关的informer
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	gpushareInformerFactory := informers.NewSharedInformerFactory(gpushareClient, time.Second*30)

	//基于Client、Informer初始化自定义Controller，监听Deployment以及gpushare资源变化
	controller := gpusharecontroller.NewController(kubeClient, gpushareClient,
		kubeInformerFactory.Core().V1().Nodes(),
		kubeInformerFactory.Core().V1().Pods(),
		gpushareInformerFactory.Gpushare().V1().VirtualPods())

	// notice that there is no need to run Start methods in a separate goroutine. (i.e. go kubeInformerFactory.Start(stopCh)
	// Start method is non-blocking and runs all registered informers in a dedicated goroutine.
	kubeInformerFactory.Start(stopCh)
	gpushareInformerFactory.Start(stopCh)

	//开启 Controller
	if err = controller.Run(1, stopCh); err != nil {
		klog.Fatalf("Error running controller: %s", err.Error())
	}
}

func init() {
	//flag.StringVar(&kubeconfig, "kubeconfig", "/home/wz123456/GpuShare/doc/yaml/sharepod1.yaml", "Path to a kubeconfig. Only required if out-of-cluster.")
	//flag.StringVar(&masterURL, "master", "219.245.186.38", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")


}

func checkCRD(gpushareClientSet *clientset.Clientset) bool {
	_, err := gpushareClientSet.GpushareV1().VirtualPods("").List(context.Background(),metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		if _, ok := err.(*errors.StatusError); ok {
			if errors.IsNotFound(err) {
				return false
			}
		}
	}
	return true
}