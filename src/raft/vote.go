package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) reqHeardbeatRPC() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendHeardBeat(i, &AppendEntriesArgs{}, &AppendEntriesApply{})
		}
	}
}

func (rf *Raft) sendHeardBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesApply) bool {
	ok := rf.peers[server].Call("Raft.HeardBeat", args, reply)

	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if reply.VoteGranted {

		rf.voteForChan <- reply
	}
	return ok
}

func (rf *Raft) reqVoteForRPC() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &RequestVoteArgs{rf.term, rf.me}, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) becomeCandidater() {
	rf.VoteForUpdate(rf.me)
	rf.roleUpdate(candidater)
}

func (rf *Raft) becomeLeader(reply *RequestVoteReply) {
	rf.termVoteNUM = rf.termVoteNUM + 1
	if rf.termVoteNUM >= int(len(rf.peers)/2) {
		rf.logger("变为leader")
		rf.roleUpdate(leader)
	}
}

func (rf *Raft) becomeFollow() {
	rf.roleUpdate(follower)
}

func (rf *Raft) beginElection() {
	rf.VoteForUpdate(rf.me)
	rf.termVoteNUM = 1
	rf.TermUpdate(rf.term + 1)
}

func (rf *Raft) getElectionTimeout() time.Duration {
	rand.Seed(int64(rf.me))
	return time.Duration(ElectionTimeout+rand.Intn(ElectionTimeout)) * time.Millisecond

}

func (rf *Raft) runServer() {

	for {

		switch rf.role {
		case follower:
			select {
			case <-rf.heardbeatChan:
				time.Sleep(120 * time.Millisecond)
				// rf.logger("heardbeat...")
			case <-time.After(rf.getElectionTimeout()):
				rf.becomeCandidater()
			}
		case candidater:

			select {
			// 如果leader横空出世, 就发送心跳
			case <-rf.heardbeatChan:
				rf.roleUpdate(follower)

			// 旧的选举周期处理
			case v := <-rf.voteForChan:
				t := rf.GetTerm()

				if v.Term > t {

					rf.becomeFollow()
				}
				if v.Term == t {
					rf.becomeLeader(v)
				}
			// 如果新的选举周期启动
			case <-time.After(rf.getElectionTimeout()):

				rf.beginElection()
				rf.reqVoteForRPC()

			}
		case leader:

			rf.reqHeardbeatRPC()
			time.Sleep(100 * time.Millisecond)

		}
	}
	// Each server stores a current term number, which increases monotonically over time .

}
