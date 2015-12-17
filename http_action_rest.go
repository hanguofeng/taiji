package main

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type callbackManagerData struct {
	Url       string   `json:"url"`
	GroupName string   `json:"group"`
	Topics    []string `json:"topics"`
}

type topicPartitionsData struct {
	Url        string  `json:"url"`
	GroupName  string  `json:"group"`
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

type partitionManagerData struct {
	Url               string      `json:"url"`
	GroupName         string      `json:"group"`
	Topic             string      `json:"topic"`
	Partition         int32       `json:"partition"`
	PartitionConsumer interface{} `json:"partition_consumer"`
	Arbiter           interface{} `json:"arbiter"`
	Transporter       interface{} `json:"transporter"`
}

func findCallbackManagerByGroup(groupName string) *CallbackManager {
	if groupName == "" {
		return nil
	}

	groupName = strings.ToLower(groupName)
	for _, manager := range GetServer().GetCallbackManagers() {
		if manager.GroupName == groupName {
			return manager
		}
	}

	return nil
}

func findPartitionManagerByTopic(manager *CallbackManager, topic string) []*PartitionManager {
	if topic == "" || manager == nil {
		return nil
	}

	found := false
	var res []*PartitionManager

	for _, t := range manager.Topics {
		if t == topic {
			found = true
			break
		}
	}

	if found {
		for _, partitionManager := range manager.GetPartitionManagers() {
			if partitionManager.Topic == topic {
				res = append(res, partitionManager)
			}
		}
	}

	return res
}

func init() {
	// new REST interface

	// /config, Get config object
	GetServer().GetAdminServerRouter().HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		code := 0
		jsonify(w, r, GetServer().GetConfig(), code)
	})

	// /callbacks, Get CallbackManagers
	GetServer().GetAdminServerRouter().HandleFunc("/callbacks", func(w http.ResponseWriter, r *http.Request) {
		code := 0

		var res []callbackManagerData

		for _, manager := range GetServer().GetCallbackManagers() {
			res = append(res, callbackManagerData{
				Url:       manager.Url,
				GroupName: manager.GroupName,
				Topics:    manager.Topics,
			})
		}

		jsonify(w, r, res, code)
	})

	// /callbacks/{group}, Get specified CallbackManager
	GetServer().GetAdminServerRouter().HandleFunc("/callbacks/{group}", func(w http.ResponseWriter, r *http.Request) {
		if manager := findCallbackManagerByGroup(mux.Vars(r)["group"]); manager != nil {
			code := 0
			jsonify(w, r, callbackManagerData{
				Url:       manager.Url,
				GroupName: manager.GroupName,
				Topics:    manager.Topics,
			}, code)
			return
		}

		http.NotFound(w, r)
	})

	// /callbacks/{group}/topics/{topic}, Get subscribed partitions of specified topics
	GetServer().GetAdminServerRouter().HandleFunc("/callbacks/{group}/topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		muxVars := mux.Vars(r)

		if manager := findCallbackManagerByGroup(muxVars["group"]); manager != nil {
			var topics []string

			switch muxVars["topic"] {
			case "*", "":
				// get first topic
				topics = manager.Topics
			default:
				topics = append(topics, muxVars["topic"])
			}

			var result []topicPartitionsData
			code := 0

			for _, topic := range topics {
				if partitionManagers := findPartitionManagerByTopic(manager, topic); len(partitionManagers) > 0 {
					// found partitionManager matches this topic
					var res topicPartitionsData

					res.Url = manager.Url
					res.GroupName = manager.GroupName
					res.Topic = topic

					for _, partitionManager := range partitionManagers {
						res.Partitions = append(res.Partitions, partitionManager.Partition)
					}

					result = append(result, res)
				}
			}

			jsonify(w, r, result, code)
			return
		}

		http.NotFound(w, r)
	})

	// /callbacks/{group}/topics/{topic}/partitions/{partition}, Get PartitionManager
	GetServer().GetAdminServerRouter().HandleFunc("/callbacks/{group}/topics/{topic}/partitions/{partition}", func(w http.ResponseWriter, r *http.Request) {
		muxVars := mux.Vars(r)

		if manager := findCallbackManagerByGroup(muxVars["group"]); manager != nil {
			var topics []string

			switch muxVars["topic"] {
			case "*", "":
				// get first topic
				topics = manager.Topics
			default:
				topics = append(topics, muxVars["topic"])
			}

			var result []partitionManagerData
			code := 0

			for _, topic := range topics {
				if partitionManagers := findPartitionManagerByTopic(manager, topic); len(partitionManagers) > 0 {
					// found partitionManager matches this topic
					for _, partitionManager := range partitionManagers {
						var res partitionManagerData
						res.Url = manager.Url
						res.GroupName = manager.GroupName
						res.Topic = topic
						res.Partition = partitionManager.Partition
						res.Arbiter = partitionManager.GetArbiter().GetStat()
						res.PartitionConsumer = partitionManager.GetPartitionConsumer().GetStat()
						res.Transporter = partitionManager.GetTransporter().GetStat()

						result = append(result, res)
					}
				}
			}

			jsonify(w, r, result, code)
			return
		}

		http.NotFound(w, r)
	})

	// /callbacks/{group}/offsets, Get OffsetManager
	GetServer().GetAdminServerRouter().HandleFunc("/callbacks/{group}/offsets", func(w http.ResponseWriter, r *http.Request) {
		muxVars := mux.Vars(r)

		if manager := findCallbackManagerByGroup(muxVars["group"]); manager != nil {
			code := 0

			rawOffsets := manager.GetOffsetManager().GetOffsets()
			// convert OffsetMap keys to string
			offsets := make(map[string]map[string]int64)
			for topic, partitionOffsets := range rawOffsets {
				offsets[topic] = make(map[string]int64)
				for partition, offset := range partitionOffsets {
					offsets[topic][strconv.FormatInt(int64(partition), 10)] = offset
				}
			}

			jsonify(w, r, offsets, code)
			return
		}

		http.NotFound(w, r)
	})

	// /callbacks/{group}/offsets/storages, Get OffsetStorage
	GetServer().GetAdminServerRouter().HandleFunc("/callbacks/{group}/offsets/storages", func(w http.ResponseWriter, r *http.Request) {
		muxVars := mux.Vars(r)

		if manager := findCallbackManagerByGroup(muxVars["group"]); manager != nil {
			// TODO, add offset storage stats
			code := 0
			jsonify(w, r, nil, code)
			return
		}

		http.NotFound(w, r)
	})
}
