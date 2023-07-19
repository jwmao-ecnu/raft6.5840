package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func max(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
