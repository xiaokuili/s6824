package raft

import (
	"log"
	"sync/atomic"
)

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).

	rf.rw.Lock()
	role := rf.role
	if role == leader {
		log.Printf("节点%d调用start", rf.me)
		term = rf.currentTerm
		index = rf.addEntries(&Entry{Term: term, Commond: command})
		rf.notifyReplicator()
	}
	rf.rw.Unlock()
	return index, term, role == leader
}

func (rf *Raft) notifyReplicator() {
	// 这里可以添加缓存处理并发
	atomic.StoreInt64(&rf.replicateNum, 1)
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.replicatorCond[i].Signal()
		}
	}
}

func (rf *Raft) addEntries(e *Entry) int {
	last := rf.LastEntry()
	e.Index = last.Index + 1
	rf.log = append(rf.log, e)

	return rf.LastEntry().Index
}

func (rf *Raft) needReplicate(peer int) bool {
	rf.rw.RLock()
	defer rf.rw.RUnlock()

	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	return rf.role == leader && rf.LastEntry().Index >= rf.nextIndex[peer]
}

func (rf *Raft) replicator(peer int) {
	for {

		rf.replicatorCond[peer].L.Lock()

		// 除了leader其他角色启动都会被阻塞
		for !rf.needReplicate(peer) {
			rf.replicatorCond[peer].Wait()
		}

		rf.replicateOneRound(peer)
		rf.replicatorCond[peer].L.Unlock()

	}
}

func (rf *Raft) EntryCopy(e []*Entry) []*Entry {
	entry := make([]*Entry, len(e))
	for k, v := range e {
		tmp := *v
		entry[k] = &tmp
	}
	return entry
}

func (rf *Raft) refreshCommitIndex() int {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N
	log.Printf("leader %d rf.log %d, rf.commitIndex %d", rf.me, rf.LastEntry().Index, rf.commitIndex)

	nodeC := 0

	for i := rf.LastEntry().Index; i > rf.commitIndex; i-- {
		for j := 0; j < len(rf.matchIndex); j++ {

			if rf.matchIndex[j] >= i {
				nodeC = nodeC + 1
				if nodeC > len(rf.peers)/2 {

					return i
				}
			}
		}
	}

	return rf.commitIndex

}
func (rf *Raft) genAppendEntriesArgs(peer int) *AppendEntriesArgs {
	rf.rw.Lock()

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.log[rf.nextIndex[peer]-1].Index,
		PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
		Entries:      rf.log[rf.nextIndex[peer]:],
	}

	rf.rw.Unlock()
	return args
}

func (rf *Raft) replicateOneRound(peer int) {

	reply := &AppendEntriesReply{}
	args := rf.genAppendEntriesArgs(peer)

	num := -1
	for ok := rf.sendAppendEntries(peer, args, reply); ok; {
		rf.rw.Lock()
		num++
		if num == 0 {
			if reply.Success {
				rf.matchIndex[peer] = reply.Index
				rf.nextIndex[peer] = reply.Index + 1

				atomic.AddInt64(&rf.replicateNum, 1)
				n := atomic.LoadInt64(&rf.replicateNum)

				if n > int64(len(rf.peers)/2) {

					rf.notifyApplicer()
				}
			} else {
				i := rf.nextIndex[peer]
				if i > 1 {
					rf.nextIndex[peer] = i - 1
				}
			}
		}
		rf.rw.Unlock()

	}

}

func (rf *Raft) notifyApplicer() {

	// 触发Signal

	// rf.refreshCommitIndex()
	rf.applierCond.Signal()
}

func (rf *Raft) needApplier() bool {

	return rf.commitIndex > rf.lastApplied
}

func (rf *Raft) applier() {

	for {
		rf.rw.Lock()

		// leader 触发开始提
		// if rf.role == leader {
		// 	rf.commitIndex = rf.refreshCommitIndex()
		// }
		log.Printf("节点%d, 角色%d", rf.me, rf.role)
		for !rf.needApplier() {
			rf.applierCond.Wait()
		}

		log.Printf("%d节点开始提交", rf.me)
		// for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {

		// 	rf.applyCh <- ApplyMsg{
		// 		CommandValid: true,
		// 		Command:      rf.log[i].Commond,
		// 		CommandIndex: rf.log[i].Index,
		// 	}
		// }
		// rf.lastApplied = rf.commitIndex
		// // 提交完毕, 发送新的commitIndex follower需要通过心跳调用rpc来重置自己的选举时间
		// rf.BroadcastHeartBeat(false)

		rf.rw.Unlock()
	}

}
