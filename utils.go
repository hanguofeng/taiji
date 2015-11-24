package main

import (
	"crypto/md5"
	"encoding/hex"
)

func getGroupName(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}
