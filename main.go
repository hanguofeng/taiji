package main

import (
	"flag"
	"fmt"

	"github.com/golang/glog"
)

const (
	VERSION = "1.0.2"
)

var (
	configFile string
	version bool
	testMode bool
	server         *Server
	statPort int
	commitInterval int
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.BoolVar(&version, "V", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
	flag.IntVar(&statPort, "s", -1, "set stat server port")
	flag.IntVar(&commitInterval, "i", 10, "set stat server port")
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
	defer func() {
		glog.Flush()
	}()

	if version {
		showVersion()
		return
	}

	if testMode {
		fmt.Println("config test ok")
		return
	}

	server = NewServer()
	if err = server.Init(configFile); err != nil {
		glog.Errorf("[Pusher]Init server failed, %s", err.Error())
		return
	}
	glog.V(2).Info("[Pusher]Init server success")

	if err = server.Run(); err != nil {
		glog.Errorf("[Pusher]Run server failed, %s", err.Error())
		return
	}
}
