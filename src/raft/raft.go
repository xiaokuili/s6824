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

	"log"
	"math/rand"
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

type Entry struct {
	Term    int
	Commond interface{}
	Index   int
}

type Role int

const (
	follower   Role = 0
	candidater Role = 1
	leader     Role = 2
)
const (
	ElectionTimeout  = 200
	HeartBeatTimeout = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 控制进行append
	replicatorCond []*sync.Cond
	applierCond    *sync.Cond
	applyCh        chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	rw   sync.RWMutex
	role Role

	// vote
	currentTerm int
	voteFor     int
	// command
	log []*Entry

	// append
	nextIndex  []int
	matchIndex []int

	// apply
	commitIndex int
	lastApplied int

	heartbeatT *time.Ticker
	electionT  *time.Ticker

	voteNum      int64
	replicateNum int64
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	rand.Seed(time.Now().Unix() + int64(rf.me))
	t := ElectionTimeout + ElectionTimeout*rand.Float64()
	return time.Duration(t) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.rw.RLock()
	term := rf.currentTerm
	role := rf.role
	rf.rw.RUnlock()
	return term, role == leader
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

func (rf *Raft) WakeUp() {
	atomic.StoreInt32(&rf.dead, 0)
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
		select {
		case <-rf.electionT.C:
			rf.rw.Lock()

			// become candidate
			rf.becomeCandidater()
			// start election
			rf.electionStart()
			rf.rw.Unlock()

		case <-rf.heartbeatT.C:
			rf.rw.Lock()
			role := rf.role

			if role == leader {
				// log.Printf("节点%d发送心跳检测", rf.me)
				rf.BroadcastHeartBeat(true)
				// 只有leader才能重置自己的选举时间
				// follower需要通过心跳调用rpc来重置自己的选举时间
				et := rf.randomElectionTimeout()
				// log.Printf("server %d heardbeat ,reset election timeout %s\n", rf.me, et.String())
				rf.electionT.Reset(et)
			}
			rf.rw.Unlock()

		}
	}
}

func (rf *Raft) electionStart() {
	atomic.StoreInt64(&rf.voteNum, 1)
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.lauchVote(i)
		}
	}
}

func (rf *Raft) lauchVote(peer int) {

	rf.rw.Lock()
	reply := &RequestVoteReply{}

	lastLogIndex := rf.log[rf.commitIndex].Index
	lastLogTerm := rf.log[rf.commitIndex].Term
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidatID:   rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.rw.Unlock()
	ok := rf.sendRequestVote(peer, args, reply)

	if ok {

		rf.rw.Lock()
		defer rf.rw.Unlock()
		role := rf.role
		term := rf.currentTerm
		me := rf.me
		if role == candidater {
			// -> leader
			if reply.VoteGranted {
				atomic.AddInt64(&rf.voteNum, 1)
				voteNum := atomic.LoadInt64(&rf.voteNum)
				if voteNum >= int64(len(rf.peers)/2) {
					t := rf.randomElectionTimeout()
					log.Printf("节点%d成为leader, 选举时长推迟%s\n", rf.me, t.String())
					rf.role = leader
					rf.electionT.Reset(t)
					rf.BroadcastHeartBeat(true)
					return
				}
			}

			//  -> follower
			if reply.Term > term {
				log.Printf("%d请求%d节点投票, 节点退回到follower", me, peer)
				rf.currentTerm = reply.Term
				rf.role = follower
				return
			}

		}

	}

}

func (rf *Raft) becomeCandidater() {

	rf.currentTerm = rf.currentTerm + 1
	rf.voteFor = rf.me
	rf.role = candidater
	t := rf.randomElectionTimeout()
	rf.electionT.Reset(t)

}

func (rf *Raft) becomeFollower() {
	rf.rw.Lock()
	defer rf.rw.Unlock()
	rf.role = follower
}

func (rf *Raft) BroadcastHeartBeat(t bool) {
	if rf.role != leader {
		return
	}

	if t {
		for i, _ := range rf.peers {
			if i != rf.me {

				go rf.sendAppendEntries(i, &AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
				}, &AppendEntriesReply{})
			}
		}
	} else {
		for i, _ := range rf.peers {
			if i != rf.me {
				args := rf.genAppendEntriesArgs(i)
				go rf.sendAppendEntries(i, args, &AppendEntriesReply{})

			}
		}
	}

}

func (rf *Raft) LastEntry() *Entry {
	return rf.log[len(rf.log)-1]
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voteFor = -1
	rf.log = make([]*Entry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	et := rf.randomElectionTimeout()
	rf.heartbeatT = time.NewTicker(HeartBeatTimeout * time.Millisecond)
	rf.electionT = time.NewTicker(et)
	rf.replicatorCond = make([]*sync.Cond, len(peers))
	rf.applierCond = sync.NewCond(&rf.rw)
	rf.applyCh = applyCh
	log.Printf("server %d init, heartbeatTimeout: %d, electionTimeout:%s", rf.me, HeartBeatTimeout, et.String())

	rf.log = append(rf.log, &Entry{Index: 0})
	for i, _ := range rf.peers {
		// 由于lastIndex从1开始，0可以不用相同， 空的command也没有影响
		rf.nextIndex[i] = rf.LastEntry().Index + 1
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if i != rf.me {
				rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
				go rf.replicator(i)
			}
		}

	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	log.Printf("%drun applier ", me)
	go rf.applier()

	return rf
}
