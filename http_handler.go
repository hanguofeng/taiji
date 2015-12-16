package main

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/golang/glog"
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
	io.WriteString(w, "Path not exists: "+r.URL.Path+"\n")
}

func jsonify(w http.ResponseWriter, r *http.Request, data interface{}, code int) {
	res := map[string]interface{}{
		"errno":  code,
		"errmsg": "success",
		"data":   data,
	}

	prettyPrint := false

	r.ParseForm()

	if r.Form["pretty"] != nil && len(r.Form["pretty"]) > 0 {
		prettyPrint = true
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	var buffer []byte
	var err error

	if prettyPrint {
		prefix := ""
		indent := "    "
		buffer, err = json.MarshalIndent(res, prefix, indent)
	} else {
		buffer, err = json.Marshal(res)
	}

	if err != nil {
		w.WriteHeader(500)
		glog.Errorf("Marshal http response error [err:%s]", err)
	} else {
		w.WriteHeader(200)
		w.Write(buffer)
		if prettyPrint {
			io.WriteString(w, "\n")
		}
	}

	return
}
