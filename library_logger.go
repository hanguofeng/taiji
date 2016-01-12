package main

import (
	"fmt"

	"github.com/golang/glog"
)

type LibraryLogger struct {
	V      glog.Level
	Prefix string
}

func NewLibraryLogger(level int32, prefix string) *LibraryLogger {
	return &LibraryLogger{
		V:      glog.Level(level),
		Prefix: prefix,
	}
}

func (ll *LibraryLogger) Print(v ...interface{}) {
	if glog.V(ll.V) {
		glog.InfoDepth(1, ll.Prefix+fmt.Sprint(v...))
	}
}

func (ll *LibraryLogger) Printf(format string, v ...interface{}) {
	if glog.V(ll.V) {
		glog.InfoDepth(1, ll.Prefix+fmt.Sprintf(format, v...))
	}
}

func (ll *LibraryLogger) Println(v ...interface{}) {
	if glog.V(ll.V) {
		glog.InfoDepth(1, ll.Prefix+fmt.Sprintln(v...))
	}
}
