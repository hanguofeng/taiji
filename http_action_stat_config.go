package main

import "net/http"

func init() {
	GetServer().Bind("/stat/config", func(w http.ResponseWriter, r *http.Request) {
		code := 0
		jsonify(w, r, GetServer().GetConfig(), code)
	})
}
