package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func HttpStatConsumerAction(w http.ResponseWriter, r *http.Request) {
	echo2client(w, r)
}

func HttpStatWorkerAction(w http.ResponseWriter, r *http.Request) {
	echo2client(w, r)
}

func HttpAdminSkipAction(w http.ResponseWriter, r *http.Request) {
	echo2client(w, r)
}
func echo2client(w http.ResponseWriter, r *http.Request) {

	res := map[string]interface{}{
		"errno":  0,
		"errmsg": "success",
		"data":   r.URL.Path,
	}

	if b, e := json.Marshal(res); e != nil {
		log.Printf("marshal http response error, %v", e)
	} else {
		io.WriteString(w, string(b))
	}

	return
}
