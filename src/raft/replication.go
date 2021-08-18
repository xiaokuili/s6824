package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) leaderCheckApply(apply AppendEntriesApply) {
	// 考虑多并发情况下, 大量非同一批次的请求返回
	// 只计算最新的term
	// 当commit之后term ++
	term := rf.term
	if apply.Term == term {
		rf.replicaNum = rf.replicaNum + 1
		if rf.replicaNum > len(rf.peers)/2 {

			rf.leaderApply(apply.Index)

			rf.TermUpdate(term + 1)
			rf.commitChan <- &commitInfo{
				term: term,
				node: rf.me,
			}
		}
	}

}

func (rf *Raft) followerApply(commitIndex int) {
	if commitIndex == rf.commitIndex {
		return
	}
	for i, v := range rf.entries[rf.applyIndex:commitIndex] {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i + rf.applyIndex + 1,
			Command:      v.Command,
		}

	}
	term := rf.GetTerm()
	rf.applyIndex = rf.commitIndex
	rf.commitIndex = rf.applyIndex + 1
	rf.TermUpdate(term + 1)

}

func (rf *Raft) leaderApply(commitIndex int) {
	if commitIndex == rf.commitIndex {
		return
	}
	for i, v := range rf.entries[rf.applyIndex:commitIndex] {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i + rf.applyIndex + 1,
			Command:      v.Command,
		}

	}

	rf.applyIndex = rf.commitIndex
	rf.commitIndex = rf.applyIndex + 1
	rf.reqCommitEntries()
}

func (rf *Raft) getFollowerAppendIndex(client int) (int, int) {

	return rf.nextIndex[client], len(rf.entries)

}

func (rf *Raft) reqAppendEntries() {
	term := rf.GetTerm()

	for i, _ := range rf.peers {
		if i != rf.me {
			begin, end := rf.getFollowerAppendIndex(i)
			go rf.sendAppendEntries(i, &AppendEntriesArgs{
				Term:            term,
				Me:              rf.me,
				PrevLogIndex:    begin,
				Entries:         copyEntries(rf.entries, begin, end),
				LeaderCommitter: rf.commitIndex,
			}, &AppendEntriesApply{})
		}
	}

}

func (rf *Raft) reqCommitEntries() {
	term := rf.GetTerm()

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendCommitEntries(i, &AppendEntriesArgs{
				Term:            term,
				Me:              rf.me,
				LeaderCommitter: rf.commitIndex,
			}, &AppendEntriesApply{})

		}
	}

}

func (rf *Raft) sendCommitEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesApply) bool {
	// 判断是否成功写入log
	ok := true
	for {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// if crash, send util node restart
		if !ok {
			time.Sleep(time.Second * 1)
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
			break
		}

		if reply.Succeeded {
			rf.replicaChan <- *reply
		}
		break
	}
	rf.commitChan <- &commitInfo{
		term: rf.term,
		node: server,
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesApply) bool {
	// 判断是否成功写入log
	ok := true
	for {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// if crash, send util node restart
		if !ok {
			time.Sleep(time.Second * 1)
			rf.nextIndex[server] = rf.nextIndex[server] - 1
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
			break
		}

		if reply.Succeeded {
			rf.nextIndex[server] = reply.Index
			rf.replicaChan <- AppendEntriesApply{
				Term:      reply.Term,
				Index:     reply.Index + 1,
				Succeeded: reply.Succeeded,
			}
			break
		}
	}

	return ok
}

func (rf *Raft) leaderCopyEntries(command interface{}) {
	term := rf.GetTerm()

	rf.entries = append(rf.entries, &Entry{Command: command, Term: term})
}

// 基于term进行通讯c
func (rf *Raft) isCommit(c chan *commitInfo) {
	count := 0
	for {

		fmt.Println(<-c)
		count++

		if count >= len(rf.peers) {
			return
		}
	}
}

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
// 第一个参数返回 command 索引, 曾经被提交
// 第二个参数返回目前的term
// 第三个参数返回true， 如果这个服务相信他是leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	// leader

	if rf.GetRole() == leader {
		rf.leaderCopyEntries(command)
		rf.reqAppendEntries()
		rf.isCommit(rf.commitChan)
		return len(rf.entries), rf.GetTerm(), true
	}

	return -1, -1, false
}

func (rf *Raft) LeaderCommit() {
	// 自动提交
	for {
		if rf.GetRole() == leader {

			v := <-rf.replicaChan
			// fmt.Println(v)
			rf.leaderCheckApply(v)
			// time.Sleep(time.Millisecond * 100)

		} else {
			time.Sleep(time.Second * 10)
		}
	}

}
