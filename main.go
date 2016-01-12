package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/golang/glog"
)

const (
	VERSION = "2.0.0"
)

var (
	configFile  string
	version     bool
	testMode    bool
	gitCommit   string
	profileFile string
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.BoolVar(&version, "V", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
	flag.StringVar(&profileFile, "p", "", "log profile file")
}

func getVersion() string {
	return fmt.Sprintf("%s-%s", VERSION, gitCommit)
}

func showVersion() {
	fmt.Println(getVersion())
}

func main() {
	// set default stderrthreshold to FATAL
	flag.Set("stderrthreshold", "FATAL")
	flag.Parse()
	defer glog.Flush()

	if profileFile != "" {
		f, err := os.Create(profileFile)
		if err != nil {
			glog.Fatalf("Cannot start profiler: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if version {
		showVersion()
		return
	}

	server := GetServer()

	if testMode {
		if err := server.Validate(configFile); err != nil {
			fmt.Println("Invalid config, err: %v", err)
			os.Exit(-1)
			return
		} else {
			fmt.Println("Config is valid")
			return
		}
	}

	// server Init
	if err := server.Init(configFile); err != nil {
		fmt.Println("Invalid config, err: %v", err)
		os.Exit(-1)
		return
	}

	// server Run
	if err := server.Run(); err != nil {
		fmt.Println("Pusher exit unexpectedly, err: %v", err)
		os.Exit(-1)
		return
	}
}
