package main

import (
	"flag"
	"fmt"

	"github.com/cihub/seelog"
)

const (
	VERSION = "2.0.0"
)

var (
	configFile     string
	version        bool
	testMode       bool
	statPort       int
	commitInterval int
	gitCommit      string
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.BoolVar(&version, "V", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
	flag.IntVar(&statPort, "s", -1, "set stat server port")
	flag.IntVar(&commitInterval, "i", 10, "set offset commit interval")
}

func getVersion() string {
	return fmt.Sprintf("%s-%s", VERSION, gitCommit)
}

func showVersion() {
	fmt.Println(getVersion())
	flag.Usage()
}

func main() {
	flag.Parse()
	defer seelog.Flush()

	if version {
		showVersion()
		return
	}

	seelog.Info("Dummy main stub")
}
