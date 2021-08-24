package main

import (
	"fmt"
	"sync"
	"time"
)

// 到底锁应该加指针吗
func main() {
	cond := sync.NewCond(&sync.Mutex{})
	var done bool = true
	go func() {
		cond.L.Lock()
		done = false
		for done {
			cond.Wait()
		}

		fmt.Println("free")
		cond.L.Unlock()
	}()
	time.Sleep(time.Second)
	// done = false

	cond.Signal()

	time.Sleep(time.Second)
}
