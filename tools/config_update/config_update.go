package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
)

var (
	configFile           string
	supervisorConfigFile string
	inplaceUpdate        bool
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "config.json file to upgrade")
	flag.StringVar(&supervisorConfigFile, "s", "supervisor.conf", "supervisor.conf file to upgrade")
	flag.BoolVar(&inplaceUpdate, "i", false, "update config file inplace")
}

func main() {
	flag.Parse()

	configJsonObject, err := os.Open(configFile)
	if err != nil {
		panic(fmt.Sprintf("could not open config.json file: %v", err))
	}

	configJsonBytes, err := ioutil.ReadAll(configJsonObject)
	if err != nil {
		panic(fmt.Sprintf("could not read config.json file: %v", err))
	}
	configJsonObject.Close()

	configJson := make(map[string]interface{})
	err = json.Unmarshal(configJsonBytes, &configJson)
	if err != nil {
		panic(fmt.Sprintf("could not parse config.json file: %v", err))
	}

	supervisorConfigObject, err := os.Open(supervisorConfigFile)
	if err != nil {
		panic(fmt.Sprintf("could not open supervisor.conf file: %v", err))
	}
	defer supervisorConfigObject.Close()

	supervisorConfigBytes, err := ioutil.ReadAll(supervisorConfigObject)
	if err != nil {
		panic(fmt.Sprintf("could not open supervisor.conf file: %v", err))
	}
	supervisorConfigObject.Close()

	port := 0
	portRegexp := regexp.MustCompile("\\s*-s\\s*(\\d+)")
	matchResult := portRegexp.FindSubmatch(supervisorConfigBytes)
	if len(matchResult) >= 2 {
		// matched
		port, err = strconv.Atoi(string(matchResult[1]))
		if err != nil {
			panic(fmt.Sprintf("decode port number failed: %v", err))
		}

		// edit supervisor.conf
		supervisorConfigBytes = portRegexp.ReplaceAllLiteral(supervisorConfigBytes, []byte{})
	}

	// add stat_server_port to config.json
	if port > 0 {
		configJson["stat_server_port"] = port
		configJsonBytes, err = json.MarshalIndent(configJson, "", "    ")
		if err != nil {
			panic(fmt.Sprintf("serialize config.json file content failed: %v", err))
		}
	}

	// print or inplace edit
	if inplaceUpdate {
		err = ioutil.WriteFile(configFile, configJsonBytes, 0600)
		if err != nil {
			panic(fmt.Sprintf("update config.json file failed: %v", err))
		}
		err = ioutil.WriteFile(supervisorConfigFile, supervisorConfigBytes, 0600)
		if err != nil {
			panic(fmt.Sprintf("update supervisor.conf file failed: %v", err))
		}
	} else {
		fmt.Println("config.json file content")
		fmt.Printf("%s\n\n", configJsonBytes)
		fmt.Println("supervisor.conf file content")
		fmt.Printf("%s\n\n", supervisorConfigBytes)
	}
}
