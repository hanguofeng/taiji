package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func HttpStatConsumerAction(w http.ResponseWriter, r *http.Request) {
	type consumerStat struct {
		Topic string
		Url   string
	}

	managers := server.managers
	var res []consumerStat
	for _, mgr := range managers {
		res = append(res, consumerStat{Topic: mgr.Topic, Url: mgr.Url})
	}

	echo2client(w, r, res)
}

func HttpStatWorkerAction(w http.ResponseWriter, r *http.Request) {
	type workerStat struct {
		Topic     string
		Url       string
		Partition uint32
		UUID      string
		Offset    uint64
	}
	var res []workerStat
	for _, mgr := range server.managers {
		for _, worker := range mgr.workers {
			res = append(res, workerStat{
				Topic:     worker.Topics[0],
				Url:       worker.Callback.Url,
				Partition: 123,
			})
		}
	}

	echo2client(w, r, res)
}

func HttpAdminSkipAction(w http.ResponseWriter, r *http.Request) {
	echo2client(w, r, "")
}

func echo2client(w http.ResponseWriter, r *http.Request, data interface{}) {
	res := map[string]interface{}{
		"errno":  0,
		"errmsg": "success",
		"data":   data,
	}

	if b, e := json.Marshal(res); e != nil {
		log.Printf("marshal http response error, %v", e)
	} else {
		io.WriteString(w, string(b))
	}

	return
}
