package main

import (
	"flag"
	"os"

	"sbtg-operator/controllers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	var namespace string
	flag.StringVar(&namespace, "namespace", "default", "Namespace to deploy resources")
	flag.Parse()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		panic(err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}

	flowServer := &controllers.FlowServer{
		Clientset: clientset,
		Namespace: namespace,
	}
	flowServer.StartServer()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Namespace: namespace})
	if err != nil {
		panic(err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		panic(err)
	}
}

