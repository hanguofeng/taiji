package main

import "net/http"

type workerStat struct {
	Topic     string
	Url       string
	CgName    string
	Partition int32
	UUID      string
	Offset    int64
}

func init() {
	GetServer().GetAdminServerRouter().HandleFunc("/stat/worker", func(w http.ResponseWriter, r *http.Request) {
		var res []workerStat

		for _, manager := range GetServer().GetCallbackManagers() {
			offsets := manager.GetOffsetManager().GetOffsets()

			for _, partitionManager := range manager.GetPartitionManagers() {
				var offset int64 = -1

				topic := partitionManager.Topic
				partition := partitionManager.Partition

				if val, ok := offsets[topic][partition]; ok {
					offset = val
				}

				res = append(res, workerStat{
					Topic:     topic,
					Url:       manager.Url,
					CgName:    manager.GroupName,
					Partition: partition,
					UUID:      manager.GetKazooGroupInstance().ID,
					Offset:    offset,
				})
			}
		}

		code := 0
		jsonify(w, r, res, code)
	})
}
