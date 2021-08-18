package raft

import (
	"fmt"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Me   int
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term int
	Me   int
	// follow index match leader
	PrevLogIndex int
	// 可以用指针, 因为不会有修改操作
	Entries         []*Entry
	LeaderCommitter int
}

type AppendEntriesApply struct {
	Term      int
	Index     int
	Succeeded bool
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
// 这里直接把args投递到自己的channel
// 所以后续基于这个channel被通知
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	t := time.Now()
	clientTerm := args.Term
	clientMe := args.Me
	term := rf.GetTerm()
	voteFor := rf.GetVoteFor()
	switch true {
	case clientTerm > term:
		rf.TermUpdate(clientTerm)
		rf.VoteForUpdate(rf.me)
		reply.Term = rf.term
		reply.VoteGranted = true
	case clientTerm == term:
		if voteFor != -1 {
			reply.VoteGranted = false
		} else {
			rf.VoteForUpdate(clientMe)
			reply.VoteGranted = true
			reply.Term = clientTerm
		}
	case clientTerm < term:
		reply.VoteGranted = false
	}
	fmt.Printf(time.Now().Sub(t).String()+"node: %d, term: %d, voteFor: %d, clientme: %d, clientterm:%d, replyterm:%d, replyGrand: %t\n", rf.me, term, voteFor, clientMe, clientTerm, reply.Term, reply.VoteGranted)

}

func (rf *Raft) ruleTerm(term, leaderTerm int) bool {
	return term <= leaderTerm
}

func (rf *Raft) ruleSameEntries(index int, logs []*Entry) bool {
	//
	if index == 0 && len(rf.entries) == 0 {
		return true
	}
	// 如果这个位置还没有entries
	if len(rf.entries) < index {
		return true
	}
	return rf.entries[index].Command == logs[0].Command
}

func (rf *Raft) followerMergeEntries(index int, logs []*Entry) {
	if len(logs) == 0 {
		return
	}
	rf.entries = append(rf.entries[:index], logs...)

}

func (rf *Raft) HeardBeat(args *AppendEntriesArgs, reply *AppendEntriesApply) {
	rf.heardbeatChan <- true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesApply) {
	// Your code here (2A, 2B).
	if args.LeaderCommitter != 0 {
		rf.followerApply(args.LeaderCommitter)
	}
	if len(args.Entries) == 0 {

		return
	}
	term := rf.GetTerm()
	// follow copy entries
	if !rf.ruleTerm(term, args.Term) {
		return
	}
	if !rf.ruleSameEntries(args.PrevLogIndex, args.Entries) {
		return
	}

	rf.followerMergeEntries(args.PrevLogIndex, args.Entries)

	reply.Index = len(rf.entries) - 1
	reply.Succeeded = true
	reply.Term = args.Term

	// // follow apply

}
