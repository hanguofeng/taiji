package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
)

type Server struct {
	// callbackManager
	callbackManagers      []*CallbackManager
	callbackManagerRunner *ServiceRunner

	// adminServer
	adminServer       *http.Server
	adminServerRouter *mux.Router

	// global http connection pool
	httpTransport http.RoundTripper

	// config
	config *ServiceConfig
}

var serverInstance *Server

func GetServer() *Server {
	// singleton
	if serverInstance == nil {
		serverInstance = &Server{
			adminServerRouter: mux.NewRouter(),
		}
	}
	return serverInstance
}

func (s *Server) Init(configFileName string) error {
	config, err := LoadConfigFile(configFileName)

	if err != nil {
		glog.Fatalf("Load Config err [err:%s]", err.Error())
		return err
	}

	s.config = config

	// init admin server
	if config.StatServerPort > 0 {
		s.adminServer = &http.Server{
			Addr:           fmt.Sprintf(":%d", config.StatServerPort),
			Handler:        s.adminServerRouter,
			ReadTimeout:    1 * time.Second,
			WriteTimeout:   1 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
	}

	// init sarama logger
	// sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	// init kazoo logger
	// kazoo.Logger

	// init http transport
	s.httpTransport = &http.Transport{
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
		glog.V(1).Infof("Initialize CallbackManager [callbackConfig:%v]", callbackConfig)
		callbackManager := NewCallbackManager()
		if err := callbackManager.Init(callbackConfig); err != nil {
			glog.Fatalf("Init CallbackManager failed [url:%s][err:%s]", callbackConfig.Url, err)
			return err
		}
		s.callbackManagers = append(s.callbackManagers, callbackManager)
	}

	s.callbackManagerRunner = NewServiceRunner()

	return nil
}

func (s *Server) Validate(configFileName string) error {
	_, err := LoadConfigFile(configFileName)
	return err
}

func (s *Server) Run() error {
	// run consumer managers
	s.callbackManagerRunner.Prepare()
	_, err := s.callbackManagerRunner.RunAsync(s.callbackManagers)

	if err != nil {
		glog.Fatalf("Start CallbackManager failed, Pusher failed to start")
		return err
	}

	glog.V(1).Infof("Pusher server get to work")

	// run http service
	if s.adminServer != nil {
		if err := s.adminServer.ListenAndServe(); err != nil {
			glog.Fatalf("Start admin http server failed [err:%s]", err.Error())
			return err
		}
		glog.Info("Pusher start admin http server success")
	}

	// register signal callback
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM, syscall.SIGKILL)

	select {
	case <-s.callbackManagerRunner.WaitForExitChannel():
		glog.Fatal("Pusher have one CallbackManager unexpected stopped, stopping server")
	case <-c:
		glog.Info("Pusher catch exit signal")
		s.callbackManagerRunner.Close()
	}

	glog.Infof("Pusher exit done")
	// adminServer would not close properly

	return nil
}

func (s *Server) GetAdminServerRouter() *mux.Router {
	return s.adminServerRouter
}

func (s *Server) GetCallbackManagers() []*CallbackManager {
	return s.callbackManagers
}

func (s *Server) GetHttpTransport() http.RoundTripper {
	return s.httpTransport
}

func (s *Server) GetConfig() *ServiceConfig {
	return s.config
}
