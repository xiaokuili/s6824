package main

import (
	"fmt"
	"sync"
	"time"
)

var locker = new(sync.Mutex)
var cond = sync.NewCond(locker)

func apply() {
	cond.L.Lock()
	defer cond.L.Unlock()

	cond.Wait()
	fmt.Println("begin doing sth")
}

func commit() {
	cond.Signal()
}

func main() {
	for i := 0; i < 10; i++ {
		go apply()
	}

	for i := 0; i < 10; i++ {
		go commit()
	}
	time.Sleep(time.Second * 10)
}
