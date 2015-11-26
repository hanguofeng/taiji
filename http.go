package main

import (
	"io"
	"net/http"
)

// Http Handler
type HttpHandler struct {
	Mux map[string]func(http.ResponseWriter, *http.Request)
}

func NewHandler() *HttpHandler {
	return &HttpHandler{
		Mux: make(map[string]func(http.ResponseWriter, *http.Request)),
	}
}

func (h *HttpHandler) AssignRouter() {
	h.Mux["/stat/consumer"] = HttpStatConsumerAction
	h.Mux["/stat/worker"] = HttpStatWorkerAction
	h.Mux["/admin/skip"] = HttpAdminSkipAction

}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hdl, ok := h.Mux[r.URL.Path]; ok {
		hdl(w, r)
		return
	}

	//TODO: handle 404, just for debugging now
	io.WriteString(w, "My server: "+r.URL.Path)
}

//
