package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/golang/glog"
)

func getGroupName(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func copyOffsetMap(offsetMap OffsetMap) OffsetMap {
	result := make(OffsetMap)

	for topic, partitionOffsets := range offsetMap {
		resultOffsets := make(map[int32]int64)

		for partition, offset := range partitionOffsets {
			resultOffsets[partition] = offset
		}

		result[topic] = resultOffsets
	}

	return result
}

func stringKeyOffsetMap(offsetMap OffsetMap) map[string]map[string]int64 {
	result := make(map[string]map[string]int64)

	for topic, partitionOffsets := range offsetMap {
		resultOffsets := make(map[string]int64)

		for partition, offset := range partitionOffsets {
			resultOffsets[strconv.FormatInt(int64(partition), 10)] = offset
		}

		result[topic] = resultOffsets
	}

	return result
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
