package raft

import "log"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatID   int
	LastLogIndex int
	LastLogTerm  int
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
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.rw.Lock()
	defer rf.rw.Unlock()
	term := args.Term

	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	// log.Printf("[vote]节点:%d,term:%d请求节点:%d,term: %d投票", args.CandidatID, args.Term, rf.me, rf.currentTerm)
	// // 只有follower进行投票
	if rf.role == leader {
		reply.VoteGranted = false

		return
	}

	// 比较term
	switch {
	// 请求term 小直接返回
	case term < currentTerm:
		reply.VoteGranted = false

		rf.role = follower

		return
	// 请求term大, 重置选举
	case term > currentTerm:

		rf.currentTerm = term
		rf.voteFor = -1
		reply.Term = rf.currentTerm

	}

	voteFor := rf.voteFor

	if voteFor == -1 || voteFor == args.CandidatID {
		if len(rf.log) == 0 {
			reply.VoteGranted = true
			return
		}
		if args.LastLogIndex >= rf.LastEntry().Index {
			if args.LastLogTerm >= rf.LastEntry().Term {
				reply.VoteGranted = true
				return
			}
		}

	}
	// log.Printf("rpc fail, %d-%d req %d-%d vote", args.CandidatID, args.Term, rf.me, term)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 多添加一个Index, 记录节点日志的长度
	Index int
}

func (rf *Raft) min(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	if i2 < i1 {
		return i2
	}
	return 0
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 每次提交都是一个个复制
	rf.rw.Lock()
	defer rf.rw.Unlock()

	rf.role = follower
	rf.heartbeatT.Reset(0)
	rf.electionT.Reset(rf.randomElectionTimeout())
	rf.currentTerm = args.Term
	// 心跳检测目的是确定权威, 所以这里直接把term变更为leader的term
	reply.Term = rf.currentTerm
	reply.Index = rf.LastEntry().Index
	// applier
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = rf.min(args.LeaderCommit, len(rf.log))
		log.Printf("[RPC-Append-Applier] %d 请求节点 %d, commit: %d, args %#v, reply %#v", args.LeaderID, rf.me, rf.commitIndex, args, reply)

		rf.applierCond.Signal()
	}

	// copy
	if len(args.Entries) == 0 {
		reply.Success = true

		// log.Printf("[RPC-Append-Heartbeat] %d 请求节点 %d, args %#v, reply %#v", args.LeaderID, rf.me, args, reply)
		return
	}
	//  Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if len(rf.log) > args.PrevLogIndex {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			log.Printf("[RPC-Append-Replicator-Fail] %d 请求节点 %d, args %#v, reply %#v", args.LeaderID, rf.me, args, reply)

			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it

	// Append any new entries not already in the log
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	reply.Index = rf.LastEntry().Index
	reply.Success = true
	log.Printf("[RPC-Append-Replicator-Succeed] %d 请求节点 %d, args %#v, reply %#v, log %#v", args.LeaderID, rf.me, args, reply, rf.log)

	// log.Printf("leader %d 复制日志到节点%d, reply: %#v", args.LeaderID, rf.me, reply)

}
