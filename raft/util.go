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

func cloneLogs(orig []Entry) []Entry {
	x := make([]Entry, len(orig))
	copy(x, orig)
	return x
}
