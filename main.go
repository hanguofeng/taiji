package main

import (
	"flag"
	"fmt"
	"log"
)

const (
	VERSION = "1.0.0"
)

var (
	configFile string
	version    bool
	testMode   bool
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.BoolVar(&version, "v", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
}

func getVersion() string {
	return VERSION
}

func showVersion() {
	fmt.Println(getVersion())
	flag.Usage()
}

func main() {
	var err error

	flag.Parse()

	if version {
		showVersion()
		return
	}

	if testMode {
		fmt.Println("config test ok")
		return
	}

	server := NewServer()
	if err = server.Init(configFile); err != nil {
		log.Fatalf("Init server failed, %s", err.Error())
		return
	}
	log.Println("Init server success")

	if err = server.Run(); err != nil {
		log.Fatalf("Run server failed, %s", err.Error())
		return
	}
}
