package main

import "fmt"

type Raft struct {
	name string
}

// golang slice Copy

func main() {
	r := make([]*Raft, 0)
	r = append(r, &Raft{"1"})

	r = append(r, &Raft{"2"})

	r = append(r, &Raft{"3"})

	r2 := make([]*Raft, len(r))
	copy(r2, r)
	fmt.Println(r)
	fmt.Println(r2)
}
