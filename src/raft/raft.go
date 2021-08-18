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
	"strconv"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type commitInfo struct {
	term int
	node int
}

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

type Entry struct {
	Command interface{}
	Term    int
}
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
	nextIndex         []int
	entries           []*Entry
	commitIndex       int
	applyIndex        int
	heardbeatChan     chan bool
	voteForChan       chan *RequestVoteReply
	nextEelectionChan chan bool
	termChan          chan bool
	applyChan         chan ApplyMsg
	termVoteNUM       int
	logchan           chan rpcFormat
	replicaChan       chan AppendEntriesApply
	replicaNum        int
	wg                sync.WaitGroup
	commitChan        chan *commitInfo
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

type rpcFormat struct {
	node     int
	voteFor  int
	term     int
	argsnode int
	argsterm int
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
		replicaChan:       make(chan AppendEntriesApply),
		peers:             peers,
		persister:         persister,
		me:                me,
		voteFor:           -1,
		termVoteNUM:       1,
		nextIndex:         make([]int, len(peers)),
		entries:           make([]*Entry, 0),
		commitIndex:       0,
		applyIndex:        0,
		applyChan:         applyCh,
		commitChan:        make(chan *commitInfo, 0),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	go rf.runServer()
	go rf.LeaderCommit()

	return rf
}
