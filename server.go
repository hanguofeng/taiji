package main

import "net/http"

type Server struct {
	callbackManagers []*CallbackManager
	adminServer      *http.Server
	httpTransport    http.RoundTripper
	config           *ServiceConfig
}

func GetServer() *Server {
	// singleton
	return nil
}

func (*Server) Init(config *ServiceConfig) {
	// init httpConnectionPool
	// new HttpHanlder
	// call callbackManager Init
}

func (*Server) Run() {
	// build httpserver listenAndServ
	// call callbackManager Run
}

func (*Server) Bind(uri string, callback func(w http.ResponseWriter, r *http.Request)) {
	// bind http handler
	// stat_consumer.go
	// func init() {
	//   GetServer().Bind("/stat/consumer", func(w http.ResponseWriter, r *http.Request){})
	// }
}

func (*Server) Close() {
	// call httpServer Close
	// call callbackManager Close
}

func (*Server) GetCallbackManagers() []*CallbackManager {
	return nil
}
