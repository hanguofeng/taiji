package main

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

type trackerStat struct {
	LastRecordOpTime     int64
	CurrRecordOpTime     int64
	LogId                string
	Offset               int64
	CurrRecordOpDateTime string
	TimeGap              int64
}

func init() {
	GetServer().GetAdminServerRouter().HandleFunc("/stat/tracker", func(w http.ResponseWriter, r *http.Request) {
		// map[url]map[topic]map[partition]trackerStat
		res := make(map[string]map[string]map[string]trackerStat)

		for _, manager := range GetServer().GetCallbackManagers() {
			// only applicable to http transporter
			if strings.ToLower(manager.GetConfig().TransporterName) != "http" {
				continue
			}

			for _, partitionManager := range manager.GetPartitionManagers() {
				stat := partitionManager.GetTransporter().GetStat()
				mapStat, ok := stat.(map[string]interface{})

				if !ok {
					continue
				}

				lastRecordOptime, _ := mapStat["last_record_op_time"].(int64)
				currRecordOptime, _ := mapStat["current_record_op_time"].(int64)
				lastLogId, _ := mapStat["last_logid"].(string)
				lastOffset, _ := mapStat["last_offset"].(int64)

				url := manager.Url
				topic := partitionManager.Topic
				partition := strconv.FormatInt(int64(partitionManager.Partition), 10)

				if res[url] == nil {
					res[url] = make(map[string]map[string]trackerStat)
				}

				if res[url][topic] == nil {
					res[url][topic] = make(map[string]trackerStat)
				}

				res[url][topic][partition] = trackerStat{
					LastRecordOpTime:     lastRecordOptime,
					CurrRecordOpTime:     currRecordOptime,
					LogId:                lastLogId,
					Offset:               lastOffset,
					CurrRecordOpDateTime: time.Unix(currRecordOptime/1000, 0).String(),
					TimeGap:              (currRecordOptime - lastRecordOptime) / 1000,
				}
			}
		}

		code := 0
		jsonify(w, r, res, code)
	})
}
