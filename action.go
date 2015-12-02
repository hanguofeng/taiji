package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

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
		for _, worker := range mgr.workers {
			status := worker.Closed()
			if status == true {
				continue
			}
			offset, err := worker.Consumer.OffsetManager().Offsets(worker.Topics[0])
			if err != nil {
				continue
			}
			for k, v := range offset {
				g, ok := pusherDataMap[worker.Callback.Url]
				if !ok {
					g = map[string]map[int32]OffsetData{}
					pusherDataMap[worker.Callback.Url] = g
				}
				t, ok := g[worker.Topics[0]]
				if !ok {
					t = map[int32]OffsetData{}
					g[worker.Topics[0]] = t
				}
				value, ok := t[k]
				if !ok {
					t[k] = OffsetData{
						UUID:   worker.Consumer.Instance().ID,
						Offset: v,
					}
				} else {
					if v > value.Offset {
						t[k] = OffsetData{
							UUID:   worker.Consumer.Instance().ID,
							Offset: v,
						}
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

func HttpStatTrackerAction(w http.ResponseWriter, r *http.Request) {
	pusherDataMap := map[string]map[string]map[string]TrackerData{}
	for _, mgr := range server.managers {
		for _, worker := range mgr.workers {
			status := worker.Closed()
			if status == true {
				continue
			}
			tracker := worker.GetWorkerTracker()
			for k, v := range tracker {
				g, ok := pusherDataMap[worker.Callback.Url]
				if !ok {
					g = map[string]map[string]TrackerData{}
					pusherDataMap[worker.Callback.Url] = g
				}
				t, ok := g[worker.Topics[0]]
				if !ok {
					t = map[string]TrackerData{}
					g[worker.Topics[0]] = t
				}
				value, ok := t[k]
				if !ok {
					t[k] = TrackerData{
						LastRecordOpTime: v.LastRecordOpTime,
						CurrRecordOpTime: v.CurrRecordOpTime,
						LogId:            v.LogId,
						Offset:           v.Offset,
					}
				} else {
					if v.Offset > value.Offset {
						t[k] = TrackerData{
							LastRecordOpTime: v.LastRecordOpTime,
							CurrRecordOpTime: v.CurrRecordOpTime,
							LogId:            v.LogId,
							Offset:           v.Offset,
						}
					}
				}
			}
		}
	}

	type response struct {
		LastRecordOpTime     int64
		CurrRecordOpTime     int64
		LogId                string
		Offset               int64
		CurrRecordOpDateTime string
		TimeGap              int64
	}

	res := map[string]map[string]map[string]response{}
	for consumergroup, cg := range pusherDataMap {
		for topic, topicData := range cg {
			for partition, partitionData := range topicData {
				g, ok := res[consumergroup]
				if !ok {
					g = map[string]map[string]response{}
					res[consumergroup] = g
				}
				t, ok := g[topic]
				if !ok {
					t = map[string]response{}
					g[topic] = t
				}
				_, ok = t[partition]
				if !ok {
					t[partition] = response{
						LastRecordOpTime:     partitionData.LastRecordOpTime,
						CurrRecordOpTime:     partitionData.CurrRecordOpTime,
						LogId:                partitionData.LogId,
						Offset:               partitionData.Offset,
						CurrRecordOpDateTime: time.Unix(partitionData.CurrRecordOpTime/1000, 0).String(),
						TimeGap:              (partitionData.CurrRecordOpTime - partitionData.LastRecordOpTime) / 1000,
					}
				}
			}
		}
	}
	echo2client(w, r, res, 0)
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

	worker, err := mgr.Find(i_partition)
	if err != nil {
		echo2client(w, r, "invalid topic/partition", 0)
		return
	}

	msg := &sarama.ConsumerMessage{
		Topic:     topic,
		Partition: int32(i_partition),
		Offset:    i_offset,
	}
	worker.Consumer.CommitUpto(msg)
	worker.Close()
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
