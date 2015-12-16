package main

import (
	"io"
	"net/http"
)

// Http Handler
type HttpHandler struct {
	Mux map[string]func(http.ResponseWriter, *http.Request)
}

func NewHttpHandler() *HttpHandler {
	return &HttpHandler{
		Mux: make(map[string]func(http.ResponseWriter, *http.Request)),
	}
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hdl, ok := h.Mux[r.URL.Path]; ok {
		hdl(w, r)
		return
	}

	w.WriteHeader(404)
	io.WriteString(w, "Path not exists: "+r.URL.Path)
}
