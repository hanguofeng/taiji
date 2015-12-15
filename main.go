package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/cihub/seelog"
)

const (
	VERSION = "2.0.0"
)

var (
	configFile  string
	logConfig   string
	version     bool
	testMode    bool
	gitCommit   string
	profileFile string
)

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.StringVar(&logConfig, "l", "logging.cfg", "log config file")
	flag.StringVar(&profileFile, "p", "", "log profile file")
	flag.BoolVar(&version, "V", false, "show version")
	flag.BoolVar(&testMode, "t", false, "test config")
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

	if profileFile != "" {
		f, err := os.Create(profileFile)
		if err != nil {
			seelog.Criticalf("Cannot start profiler: %v", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if logConfig != "" {
		logger, err := seelog.LoggerFromConfigAsFile(logConfig)
		if err != nil {
			seelog.Criticalf("Cannot start logger: %v", err)
			os.Exit(-1)
		}
		seelog.ReplaceLogger(logger)
	}
	defer seelog.Flush()

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
