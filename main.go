package main

import (
	"flag"
	"fmt"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"path/filepath"
	"time"

	v1 "k8s.io/api/apps/v1"
	v2 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// pod控制器
type Controller struct {
	indexer  cache.Indexer
	queue    workqueue.RateLimitingInterface
	informer cache.Controller
}

func NewController(queue workqueue.RateLimitingInterface, indexer cache.Indexer, informer cache.Controller) *Controller {
	return &Controller{
		indexer:  indexer,
		queue:    queue,
		informer: informer,
	}
}

func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()
	// 停止控制器后关掉队列
	defer c.queue.ShutDown()

	// 启动控制器
	klog.Infof("staring pod controller")
	// 启动通用控制器框架
	go c.informer.Run(stopCh)

	// 等待同步缓存完成，然后开始处理队列中的数据
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timeout waiting for caches to sync"))
		return
	}
	for i := 0; i < threadiness; i++ {
		//队列中拿数据去处理
		go wait.Until(c.runWorker, time.Second, stopCh)

	}
	<-stopCh

	klog.Info("stopping pod controller")

}

// 处理元素
func (c *Controller) runWorker() {
	for c.processNextItem() {

	}
}
func (c *Controller) processNextItem() bool {
	// 从workqueue中取出元素
	key, quit := c.queue.Get()
	if !quit {
		return false
	}
	// 告诉队列我们处理了该key
	defer c.queue.Done(key)
	// 根据key去处理我们的业务逻辑
	err := c.syncToStdout(key.(string))
	c.handlerErr(err, key)
	return true
}

// 简单的业务处理逻辑
func (c *Controller) syncToStdout(key string) error {
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		klog.Errorf("Fetch obj with key %s from indexer failed with %v", key, err)
		return err
	}
	if !exists {
		klog.Info(fmt.Printf("Pod %s does not exists anymore \n", key))
	} else {
		klog.Info(fmt.Printf(
			"Sync/Add/Update for Pod %s\n",
			obj.(*v2.Pod).GetName(),
		))
	}
	return nil
}

// 检测逻辑，是否需要重试等
func (c *Controller) handlerErr(err error, key interface{}) {
	if err == nil { // 处理正常
		c.queue.Forget(key)
		return
	}
	// 如果出现了问题，我们运行当前控制器重试5次
	if c.queue.NumRequeues(key) < 5 {
		// 重新入队列
		c.queue.AddRateLimited(key)
		return
	}
	// 重试超过5次 不再重试
	c.queue.Forget(key)
	runtime.HandleError(err)
}

func main() {
	var err error
	var config *rest.Config

	var kubeconfig *string

	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "[可选] kubeconfig 绝对路径")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig 绝对路径")
	}
	// 初始化 rest.Config 对象
	if config, err = rest.InClusterConfig(); err != nil {
		if config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig); err != nil {
			panic(err.Error())
		}
	}
	// 创建 Clientset 对象
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// 创建pod的list watch
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "pods", v2.NamespaceDefault, fields.Everything())

	// 创建队列
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	indexer, informer := cache.NewIndexerInformer(podListWatcher, &v2.Pod{}, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key) // 入队workqueue
			}

		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				queue.Add(key) // 入队workqueue
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key) // 入队workqueue
			}
		},
	}, cache.Indexers{})
	// 实例化pod控制器
	controller := NewController(queue, indexer, informer)

	stopCh := make(chan struct{})
	defer close(stopCh)
	go controller.Run(1, stopCh)

	select {}

	//// 初始化 informer factory（为了测试方便这里设置每30s重新 List 一次）
	//informerFactory := informers.NewSharedInformerFactory(clientset, time.Second*30)
	//// 对 Deployment 监听
	//deployInformer := informerFactory.Apps().V1().Deployments()
	//// 创建 Informer（相当于注册到工厂中去，这样下面启动的时候就会去 List & Watch 对应的资源）
	//informer := deployInformer.Informer()
	//// 创建 Lister
	//deployLister := deployInformer.Lister()
	//// 注册事件处理程序
	//informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
	//	AddFunc:    onAdd,
	//	UpdateFunc: onUpdate,
	//	DeleteFunc: onDelete,
	//})
	//
	//stopper := make(chan struct{})
	//defer close(stopper)
	//
	//// 启动 informer，List & Watch
	//informerFactory.Start(stopper)
	//// 等待所有启动的 Informer 的缓存被同步
	//informerFactory.WaitForCacheSync(stopper)
	//
	//// 从本地缓存中获取 default 中的所有 deployment 列表
	//deployments, err := deployLister.Deployments("default").List(labels.Everything())
	//if err != nil {
	//	panic(err)
	//}
	//for idx, deploy := range deployments {
	//	fmt.Printf("%d -> %s\n", idx+1, deploy.Name)
	//}
	//<-stopper
}

func onAdd(obj interface{}) {
	deploy := obj.(*v1.Deployment)
	fmt.Println("add a deployment:", deploy.Name)
}

func onUpdate(old, new interface{}) {
	oldDeploy := old.(*v1.Deployment)
	newDeploy := new.(*v1.Deployment)
	fmt.Println("update deployment:", oldDeploy.Name, newDeploy.Name)
}

func onDelete(obj interface{}) {
	deploy := obj.(*v1.Deployment)
	fmt.Println("delete a deployment:", deploy.Name)
}
