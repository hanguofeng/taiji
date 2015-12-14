package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
)

type Server struct {
	callbackManagers      []*CallbackManager
	callbackManagerRunner *ServiceRunner
	adminServer           *http.Server
	handler               *HttpHandler
	httpTransport         http.RoundTripper
	config                *ServiceConfig
}

var serverInstance *Server

func GetServer() *Server {
	// singleton
	if serverInstance == nil {
		serverInstance = &Server{}
		serverInstance.handler = NewHttpHandler()
	}
	return serverInstance
}

func (this *Server) Init(configFileName string) error {
	config, err := LoadConfigFile(configFileName)

	if err != nil {
		seelog.Criticalf("Load Config err [err:%s]", err.Error())
		return err
	}

	// init admin server
	if config.StatServerPort > 0 {
		this.adminServer = &http.Server{
			Addr:           fmt.Sprintf(":%d", config.StatServerPort),
			Handler:        this.handler,
			ReadTimeout:    1 * time.Second,
			WriteTimeout:   1 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
	}

	// init sarama logger
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	// init http transport
	this.httpTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: config.ConnectionPoolSize,
	}

	// init callback managers
	for i, _ := range config.Callbacks {
		callbackConfig := &config.Callbacks[i]
		seelog.Debugf("Initialize CallbackManager [callbackConfig:%v]", callbackConfig)
		callbackManager := NewCallbackManager()
		if err := callbackManager.Init(callbackConfig); err != nil {
			seelog.Criticalf("Init CallbackManager failed [url:%s][err:%s]", callbackConfig.Url)
			return err
		}
		this.callbackManagers = append(this.callbackManagers, callbackManager)
	}

	this.callbackManagerRunner = NewServiceRunner()

	return nil
}

func (this *Server) Validate(configFileName string) error {
	_, err := LoadConfigFile(configFileName)
	return err
}

func (this *Server) Run() error {
	// run consumer managers
	this.callbackManagerRunner.Prepare()
	_, err := this.callbackManagerRunner.RunAsync(this.callbackManagers)

	if err != nil {
		seelog.Criticalf("Start CallbackManager failed, Pusher failed to start")
		return err
	}

	seelog.Debugf("Pusher server get to work")

	// run http service
	if this.adminServer != nil {
		if err := this.adminServer.ListenAndServe(); err != nil {
			seelog.Criticalf("Start admin http server failed [err:%s]", err.Error())
			return err
		}
		seelog.Info("Pusher start admin http server success")
	}

	// register signal callback
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-this.callbackManagerRunner.WaitForExitChannel():
		seelog.Critical("Pusher have one CallbackManager unexpected stopped, stopping server")
	case <-c:
		seelog.Info("Pusher catch exit signal")
		this.callbackManagerRunner.Close()
	}

	seelog.Infof("Pusher exit done")
	// adminServer would not close properly

	return nil
}

func (this *Server) Bind(uri string, callback func(w http.ResponseWriter, r *http.Request)) {
	this.handler.Mux[uri] = callback
}

func (this *Server) GetCallbackManagers() []*CallbackManager {
	return this.callbackManagers
}

func (this *Server) GetHttpTransport() http.RoundTripper {
	return this.httpTransport
}
