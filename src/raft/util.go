package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

// Min returns minimum of two ints
func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// Max returns the maximum of two ints
func Max(a, b int) int {
	if a <= b {
		return b
	}
	return a
}
