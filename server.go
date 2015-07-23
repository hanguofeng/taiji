package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
	//"path/filepath"
	"runtime"
	"syscall"

	"github.com/golang/glog"
	"gopkg.in/Shopify/sarama.v1"
)

type HTTPServer struct {
	Addr            string
	Handler         http.Handler
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxHeaderBytes  int
	KeepAliveEnable bool
	RouterFunc      func(map[string]func(http.ResponseWriter, *http.Request))
	Wg              *sync.WaitGroup
	Mux             map[string]func(http.ResponseWriter, *http.Request)
}

type Server struct {
	managers []*Manager
	httpsvr  *http.Server
}

func NewServer() *Server {
	return &Server{}
}

func (this *Server) Init(configFile string) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	config, err := loadConfig(configFile)
	if err != nil {
		glog.Fatalf("[Pusher]Load Config err: %s", err.Error())
		return err
	}

	// init sarama logger
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	// init consumer managers
	for _, callbackConfig := range config.Callbacks {
		glog.Infoln(callbackConfig)
		manager := NewManager()
		if e := manager.Init(&callbackConfig); e != nil {
			glog.Fatalf("[Pusher]Init manager for url[%s] failed, %s", callbackConfig.Url, e.Error())
			return e
		}
		this.managers = append(this.managers, manager)
	}

	// init http service
	if statPort > 0 {
		hdl := NewHandler()
		hdl.AssignRouter()

		this.httpsvr = &http.Server{
			Addr:           fmt.Sprintf(":%d", statPort),
			Handler:        hdl,
			ReadTimeout:    1 * time.Second,
			WriteTimeout:   1 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
	}

	return nil
}

func (this *Server) Run() error {

	// run consumer managers
	for _, mgr := range this.managers {
		mgr.Work()
	}
	glog.V(2).Info("[Pusher]Managers get to work!")

	// run http service
	if statPort > 0 {
		if err := this.httpsvr.ListenAndServe(); err != nil {
			glog.Fatalln("[Pusher]Start admin http server failed.", err)
			return err
		}
		glog.V(2).Info("[Pusher]Start admin http server success.")
	}

	// register signal callback
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		glog.V(2).Info("[Pusher]Catch exit signal")
		for _, mgr := range this.managers {
			mgr.Close()
		}
		glog.V(2).Info("[Pusher]Exit done")
	}

	return nil
}
