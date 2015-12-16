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
	io.WriteString(w, "Path not exists: "+r.URL.Path)
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
	w.WriteHeader(200)

	if prettyPrint {
		prefix := ""
		indent := "    "
		if buffer, err := json.MarshalIndent(res, prefix, indent); err != nil {
			glog.Errorf("Marshal http response error [err:%s]", err)
		} else {
			w.Write(buffer)
		}
	} else {
		if buffer, err := json.Marshal(res); err != nil {
			glog.Errorf("Marshal http response error [err:%s]", err)
		} else {
			w.Write(buffer)
		}
	}

	return
}
