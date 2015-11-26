package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
)

func getGroupName(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func discardBody(r io.Reader) {
	tempBuf := make([]byte, 4096)
	for {
		_, e := r.Read(tempBuf)
		if e != nil {
			break
		}
	}
}
