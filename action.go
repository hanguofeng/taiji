package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"github.com/Shopify/sarama"
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

	echo2client(w, r, res, 0)
}

func HttpStatWorkerAction(w http.ResponseWriter, r *http.Request) {
	type OffsetData struct {
		UUID   string
		Offset int64
	}
	pusherDataMap := map[string]map[string]map[int32]OffsetData{}
	for _, mgr := range server.managers {
		// TODO, support multiple topic in one callback item
		topic := mgr.coordinator.Topics[0]
		status := mgr.coordinator.Closed()
		if status == true {
			continue
		}
		offset, err := mgr.coordinator.GetConsumer().OffsetManager().Offsets(topic)
		if err != nil {
			continue
		}
		instanceID := mgr.coordinator.GetConsumer().Instance().ID

		for k, v := range offset {
			g, ok := pusherDataMap[mgr.coordinator.CallbackUrl]
			if !ok {
				g = map[string]map[int32]OffsetData{}
				pusherDataMap[mgr.coordinator.CallbackUrl] = g
			}
			t, ok := g[topic]
			if !ok {
				t = map[int32]OffsetData{}
				g[topic] = t
			}
			value, ok := t[k]
			if !ok {
				t[k] = OffsetData{
					UUID:   instanceID,
					Offset: v,
				}
			} else {
				if v > value.Offset {
					t[k] = OffsetData{
						UUID:   instanceID,
						Offset: v,
					}
				}
			}
		}
	}
	type workerStat struct {
		Topic     string
		Url       string
		CgName    string
		Partition int32
		UUID      string
		Offset    int64
	}

	var res []workerStat
	for gUrl, group := range pusherDataMap {
		for topicKey, topic := range group {
			for partitionId, part := range topic {
				res = append(res, workerStat{
					Topic:     topicKey,
					Url:       gUrl,
					CgName:    getGroupName(gUrl),
					Partition: partitionId,
					UUID:      part.UUID,
					Offset:    part.Offset,
				})
			}
		}
	}
	code := 0

	echo2client(w, r, res, code)
}

func HttpAdminSkipAction(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	topic := query.Get("topic")
	partition := query.Get("partition")
	group := query.Get("group")
	offset := query.Get("offset")

	if topic == "" || partition == "" || group == "" || offset == "" {
		echo2client(w, r, "invalid param", 0)
		return
	}

	i_partition, _ := strconv.Atoi(partition)
	i_offset, _ := strconv.ParseInt(offset, 10, 64)

	mgr, err := server.Find(topic, group)
	if err != nil {
		echo2client(w, r, "invalid topic/group", 0)
		return
	}

	coordinator, err := mgr.Find(i_partition)
	if err != nil {
		echo2client(w, r, "invalid topic/partition", 0)
		return
	}

	msg := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: int32(i_partition),
		Offset:    i_offset,
	}
	coordinator.GetConsumer().CommitUpto(msg)
	// trigger supervise to reload Coordinator/Worker
	coordinator.Close()
	echo2client(w, r, "", 0)
}

func echo2client(w http.ResponseWriter, r *http.Request, data interface{}, code int) {
	res := map[string]interface{}{
		"errno":  code,
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
