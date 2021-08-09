package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	follower Role = iota
	candidater
	leader
)
const (
	CandidateTimeout = 120
	ElectionTimeout  = 1000
	heardbeatTimeout = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term              int
	voteFor           int
	role              Role
	heardbeatChan     chan bool
	voteForChan       chan *RequestVoteReply
	nextEelectionChan chan bool
	termChan          chan bool
	termVoteNUM       int
	logchan           chan rpcFormat
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	return rf.GetTerm(), rf.GetRole() == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Me   int
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

type rpcFormat struct {
	node     int
	voteFor  int
	term     int
	argsnode int
	argsterm int
}

//
// example RequestVote RPC handler.
//
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

func (rf *Raft) AppendEntries(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.heardbeatChan <- true
}

func (rf *Raft) logger(s string) {
	buf := &bytes.Buffer{}
	l := log.New(buf, "raft-"+strconv.Itoa(rf.me)+";role-"+strconv.Itoa(int(rf.role))+";term-"+strconv.Itoa(rf.term)+";", log.Lshortfile)
	l.Print(s)
	fmt.Print(buf)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if reply.VoteGranted {

		rf.voteForChan <- reply
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

func (rf *Raft) TermUpdate(t int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term = t
}

func (rf *Raft) GetTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term
}

func (rf *Raft) VoteForUpdate(t int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.voteFor = t
}

func (rf *Raft) GetVoteFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.voteFor
}

func (rf *Raft) roleUpdate(r Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = r
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) reqVoteForRPC() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &RequestVoteArgs{rf.term, rf.me}, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) reqHeardbeatRPC() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &RequestVoteArgs{}, &RequestVoteReply{})
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
				rf.logger("become candidate, become 120 ms can't heard heardbeat")
				rf.becomeCandidater()
			}
		case candidater:

			select {
			// 如果leader横空出世, 就发送心跳
			case <-rf.heardbeatChan:
				rf.role = follower

			// 旧的选举周期处理
			case v := <-rf.voteForChan:
				t := rf.GetTerm()
				rf.logger("获取投票日志")
				if v.Term > t {
					rf.logger("变为follow")
					rf.becomeFollow()
				}
				if v.Term == t {
					rf.becomeLeader(v)
				}
			// 如果新的选举周期启动
			case <-time.After(rf.getElectionTimeout()):
				rf.logger("新的选举周期")
				rf.beginElection()
				rf.reqVoteForRPC()

			}
		case leader:
			rf.logger("heardbeat")
			rf.reqHeardbeatRPC()
			time.Sleep(100 * time.Millisecond)

		}
	}
	// Each server stores a current term number, which increases monotonically over time .

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg
// 因为单节点模拟多节点, 所以是port

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		heardbeatChan:     make(chan bool),
		voteForChan:       make(chan *RequestVoteReply),
		nextEelectionChan: make(chan bool),
		termChan:          make(chan bool),
		logchan:           make(chan rpcFormat),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voteFor = -1
	rf.termVoteNUM = 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	go rf.runServer()

	return rf
}
