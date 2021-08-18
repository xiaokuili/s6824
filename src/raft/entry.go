package raft

func copyEntries(e []*Entry, begin, end int) []*Entry {
	c := make([]*Entry, end-begin)
	for i, v := range e[begin:end] {
		tmp := *v
		c[i] = &tmp
	}
	return c
}
