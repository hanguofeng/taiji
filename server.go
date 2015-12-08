package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cihub/seelog"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Server struct {
	callbackManagers []*CallbackManager
	adminServer      *http.Server
	handler          *HttpHandler
	httpTransport    http.RoundTripper
	config           *ServiceConfig
}

var _instance *Server

func GetServer() *Server {
	// singleton
	if _instance == nil {
		_instance = &Server{}

		// init http service
		if statPort > 0 {
			_instance.handler = NewHandler()

			_instance.adminServer = &http.Server{
				Addr:           fmt.Sprintf(":%d", statPort),
				Handler:        _instance.handler,
				ReadTimeout:    1 * time.Second,
				WriteTimeout:   1 * time.Second,
				MaxHeaderBytes: 1 << 20,
			}
		}
	}
	return _instance
}

func (this *Server) Init(config *ServiceConfig) error {
	config, err := LoadConfigFile(configFile)
	if err != nil {
		seelog.Criticalf("Load Config err [err:%s]", err.Error())
		return err
	}

	// check env var
	if commitInterval < 0 {
		seelog.Errorf("Invalid commit interval [commitInterval:%d]", commitInterval)
		return errors.New("Invalid param")
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
		seelog.Infof("%v", callbackConfig)
		callbackManager := NewCallbackManager()
		if e := callbackManager.Init(callbackConfig); e != nil {
			seelog.Criticalf("Init manager failed [url:%s][err:%s]", callbackConfig.Url)
			return e
		}
		this.callbackManagers = append(this.callbackManagers, callbackManager)
	}

	return nil
}

func (this *Server) Run() error {
	// run consumer managers
	for _, callbackManager := range this.callbackManagers {
		callbackManager.Run()
	}
	seelog.Infof("Pusher server get to work!")

	// run http service
	if statPort > 0 {
		if err := this.adminServer.ListenAndServe(); err != nil {
			seelog.Criticalf("Start admin http server failed [err:%s].", err.Error())
			return err
		}
		seelog.Infof("Pusher start admin http server success.")
	}

	// register signal callback
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-c:
		seelog.Infof("Pusher catch exit signal")
		this.Close()
		seelog.Infof("Pusher exit done")
	}

	return nil
}

func (this *Server) Bind(uri string, callback func(w http.ResponseWriter, r *http.Request)) {
	this.handler.Mux[uri] = callback
}

func (this *Server) Close() {
	// call httpServer Close
	// call callbackManager Close
	for _, callbackManager := range this.callbackManagers {
		callbackManager.Close()
	}
}

func (this *Server) GetCallbackManagers() []*CallbackManager {
	return this.callbackManagers
}

func (this *Server) GetHttpTransport() http.RoundTripper {
	return this.httpTransport
}
