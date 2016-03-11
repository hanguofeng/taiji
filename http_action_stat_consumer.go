package main

import "net/http"

type consumerStat struct {
	Topic string
	Url   string
}

func init() {
	GetServer().GetAdminServerRouter().HandleFunc("/stat/consumer", func(w http.ResponseWriter, r *http.Request) {
		var res []consumerStat

		for _, manager := range GetServer().GetCallbackManagers() {
			for _, topic := range manager.Topics {
				res = append(res, consumerStat{
					Topic: topic,
					Url:   manager.Url,
				})
			}
		}

		code := 0
		jsonify(w, r, res, code)
	})
}
