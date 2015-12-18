package main

import (
	"flag"
	"net/http"
)

func init() {
	// get glog v/vmodule
	GetServer().GetAdminServerRouter().HandleFunc("/admin/glog", func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]interface{})

		if flagV := flag.Lookup("v"); flagV != nil {
			result["v"] = flagV.Value
		}
		if flagVModule := flag.Lookup("vmodule"); flagVModule != nil {
			result["vmodule"] = flagVModule.Value
		}

		code := 0
		jsonify(w, r, result, code)
	}).Methods("GET")

	// edit glog v/vmodule
	GetServer().GetAdminServerRouter().HandleFunc("/admin/glog", func(w http.ResponseWriter, r *http.Request) {
		v := r.FormValue("v")
		vmodule := r.FormValue("vmodule")

		var err error

		if v != "" {
			err = flag.Set("v", v)
		}

		if err == nil && vmodule != "" {
			err = flag.Set("vmodule", vmodule)
		}

		if err == nil {
			jsonify(w, r, nil, 0)
		} else {
			jsonify(w, r, nil, 500, err)
		}
	}).Methods("POST")
}
