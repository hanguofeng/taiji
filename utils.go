package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sort"

	"github.com/golang/glog"
	"github.com/wvanbergen/kazoo-go"
)

func getGroupName(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
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

func retrievePartitionLeaders(partitions kazoo.PartitionList) (partitionLeaders, error) {

	pls := make(partitionLeaders, 0, len(partitions))
	for _, partition := range partitions {
		leader, err := partition.Leader()
		if err != nil {
			return nil, err
		}

		pl := partitionLeader{id: partition.ID, leader: leader, partition: partition}
		pls = append(pls, pl)
	}

	return pls, nil
}

// Divides a set of partitions between a set of consumers.
func dividePartitionsBetweenConsumers(consumers kazoo.ConsumergroupInstanceList, partitions partitionLeaders) map[string][]*kazoo.Partition {
	result := make(map[string][]*kazoo.Partition)

	plen := len(partitions)
	clen := len(consumers)
	if clen == 0 {
		return result
	}

	sort.Sort(partitions)
	sort.Sort(consumers)

	n := plen / clen
	m := plen % clen
	p := 0
	for i, consumer := range consumers {
		first := p
		last := first + n
		if m > 0 && i < m {
			last++
		}
		if last > plen {
			last = plen
		}

		for _, pl := range partitions[first:last] {
			result[consumer.ID] = append(result[consumer.ID], pl.partition)
		}
		p = last
	}

	return result
}

type partitionLeader struct {
	id        int32
	leader    int32
	partition *kazoo.Partition
}

// A sortable slice of PartitionLeader structs
type partitionLeaders []partitionLeader

func (pls partitionLeaders) Len() int {
	return len(pls)
}

func (pls partitionLeaders) Less(i, j int) bool {
	return pls[i].leader < pls[j].leader || (pls[i].leader == pls[j].leader && pls[i].id < pls[j].id)
}

func (s partitionLeaders) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
