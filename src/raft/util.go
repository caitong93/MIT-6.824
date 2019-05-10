package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = 1

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var rng = struct {
	sync.Mutex
	rand *rand.Rand
}{
	rand: rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
}

// Int63nRange generates an int64 integer in range [min,max).
// By design this should panic if input is invalid, <= 0.
func Int63nRange(min, max int64) int64 {
	rng.Lock()
	defer rng.Unlock()
	return rng.rand.Int63n(max-min) + min
}

func durationRange(min, max time.Duration) time.Duration {
	return time.Duration(Int63nRange(int64(min), int64(max)))
}

func maxInt32(a, b int32) int32 {
	if a >= b {
		return a
	}
	return b
}

func minInt32(a, b int32) int32 {
	if a <= b {
		return a
	}
	return b
}
