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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	StateFollower int = iota
	StateCandidate
	StateLeader
)

var HeartBeatTimeout = 50 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
const (
	HEARTBEAT = 50 * time.Millisecond
)

// type Entry struct {
// 	Command interface{}
// 	Term    int
// 	Index   int
// }

type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	state       int
	currentTerm int
	votedfor    int
	logIndex    int
	log         []LogEntry

	electionTime  time.Time
	heartbeatTime time.Duration

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	applyCond *sync.Cond
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader := rf.state == StateLeader
	// Your code here (2A).
	return int(rf.currentTerm), isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedfor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogterm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) leaderAppendEntries(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if ok := rf.sendAppendEntries(peer, args, &reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}

	if reply.Term == rf.currentTerm {
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[peer] = max(next, rf.nextIndex[peer])
			rf.matchIndex[peer] = max(match, rf.matchIndex[peer])
			DPrintf("[%v]: %v append success next %v match %v", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])

		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v", rf.me, peer, reply)
			//没有存储该leader日志
			if reply.XTerm == -1 {
				rf.nextIndex[peer] = reply.XLen
			} else {
				lastLogIdxXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogIdxXTerm)
				if lastLogIdxXTerm > 0 {
					rf.nextIndex[peer] = lastLogIdxXTerm
				} else {
					rf.nextIndex[peer] = reply.XIndex
				}
			}

		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}
		rf.LeaderCommit()
	}

}

func (rf *Raft) LeaderCommit() {
	if rf.state != StateLeader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log[len(rf.log)-1].Index; n++ {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		matched := 1

		for peer, _ := range rf.peers {
			if peer == rf.me {
				continue
			}

			if rf.matchIndex[peer] >= n {
				matched++
			}

			if matched >= len(rf.peers)/2 {
				// rf.apply()
				rf.commitIndex = n
				rf.apply()
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				break
			}
		}
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.log[len(rf.log)-1].Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintVerbose("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}

}

func (rf *Raft) commits() string {
	nums := []string{}
	for i := 0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.log[i].Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log[len(rf.log)-1].Index; i > 0; i-- {
		term := rf.log[i].Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (rf *Raft) appendEntries() {
	lastLog := rf.log[len(rf.log)-1]

	for peer, _ := range rf.peers {
		nextIndex := rf.nextIndex[peer]
		if lastLog.Index >= nextIndex {
			prevLog := rf.log[nextIndex-1]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]LogEntry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log[nextIndex:])
			go rf.leaderAppendEntries(peer, &args)

		}
	}

}

//handle appendentries rpc
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		fmt.Printf("node %d【receive appendtries from %d ret false\n", rf.me, args.LeaderId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	rf.resetElectionTimeout()
	if rf.state == StateCandidate {
		rf.state = StateFollower
	}

	//append entries rule 2
	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XIndex = -1
		reply.Term = -1
		reply.XLen = len(rf.log)
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	for idx, entry := range args.Entries {
		//append entry rule 3
		if entry.Index <= rf.log[len(rf.log)-1].Index && rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
			rf.persist()
		}

		if entry.Index > rf.LastLog().Index {
			rf.log = append(rf.log, args.Entries[idx:]...)
			rf.persist()
			break
		}
	}

	//append entries rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.LastLog().Index)
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) LastLog() *LogEntry {
	return &rf.log[len(rf.log)-1]
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = StateFollower
		rf.currentTerm = term
		rf.votedfor = -1
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// rf.elecitonTimer.Reset(RandomElectionTime())
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	if args.Term < rf.currentTerm {
		fmt.Printf("node %d 【reject vote】due to term\n", rf.me)
		reply.VoteGranted = false
		return
	}

	if rf.votedfor == -1 || rf.votedfor == args.CandidateId {
		rf.votedfor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionTimeout()
		fmt.Printf("node %d 【grant vote】 to %d \n", rf.me, args.CandidateId)
	}

	// if rf.votedfor == -1 {
	// 	if args.LastLogterm > rf.currentTerm {
	// 		rf.votedfor = args.CandidateId
	// 		rf.state = StateFollower
	// 		fmt.Printf("node %d 【grant vote】 to %d \n", rf.me, args.CandidateId)
	// 		reply.VoteGranted = true
	// 	} else if args.LastLogterm == rf.currentTerm {
	// 		if args.LastLogIndex >= rf.logIndex {
	// 			rf.state = StateFollower
	// 			fmt.Println("grant vote")
	// 			reply.VoteGranted = true
	// 			rf.votedfor = args.CandidateId
	// 		}
	// 	}
	// } else {
	// 	// fmt.Printf("node %d receive request vote 【voteder for】is %d\n", rf.me, rf.votedfor)
	// }
	// Your code here (2A, 2B).
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("node %d【send request vote】to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("node %d【send heart beat】to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := len(rf.log) + 1
	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	log := LogEntry{
		Command: command,
		Index:   int(index),
		Term:    rf.currentTerm,
	}

	rf.log = append(rf.log, log)
	rf.persist()

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) SetState(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) setVoted(votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedfor = votedFor
}

func (rf *Raft) getQuormCount() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.peers) / 2
}

func (rf *Raft) resetElectionTimeout() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) SendHeartBeat() {
	if rf.state != StateLeader {
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				Entries:  nil,
				LeaderId: rf.me,
			}

			reply := new(AppendEntriesReply)
			rf.sendAppendEntries(i, &args, reply)
			if reply.Term > rf.currentTerm {
				rf.state = StateFollower
				rf.votedfor = -1
				return
			}
		}(i)
	}
}

func (rf *Raft) runCandidate() {
	rf.currentTerm += 1
	rf.votedfor = rf.me
	rf.state = StateCandidate
	quormSize := len(rf.peers) / 2
	rf.resetElectionTimeout()
	grantVote := 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogterm:  rf.currentTerm,
		LastLogIndex: 10,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			resp := &RequestVoteReply{}
			if rf.state != StateCandidate {
				fmt.Printf("【not candidate】return \n")
				return
			}
			ok := rf.sendRequestVote(i, args, resp)
			if !ok {
				resp.VoteGranted = false
				// fmt.Printf("node %d【send request vote fail】to %d peers %d\n", rf.me, i, len(rf.peers))
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != StateCandidate {
				fmt.Printf("【not candidate】return \n")
				return
			}
			if resp.Term > rf.currentTerm {
				fmt.Println("follower term greater than candidate")
				rf.state = StateFollower
				rf.currentTerm = resp.Term
				return
			}
			if resp.VoteGranted {
				grantVote += 1
				if grantVote >= quormSize {
					rf.state = StateLeader
					rf.SendHeartBeat()
					fmt.Printf("node %d change state 【to leader】 on %d\n", rf.me, rf.currentTerm)
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) RunLeader() {

}

func (rf *Raft) ticker() {
	fmt.Println("cluster node num ", len(rf.peers))
	for !rf.killed() {
		time.Sleep(HEARTBEAT)
		rf.mu.Lock()
		if rf.state == StateLeader {
			rf.SendHeartBeat()
		}
		if time.Now().After(rf.electionTime) {
			fmt.Printf("node %d 【election out】 state change to candidate\n", rf.me)
			rf.runCandidate()
		}
		rf.mu.Unlock()

		// select {
		// case <-rf.elecitonTimer.C:
		// 	switch rf.state {
		// 	case StateCandidate:
		// 	case StateFollower:
		// 		fmt.Printf("node %d 【election out】 state change to candidate\n", rf.me)
		// 		rf.runCandidate()
		// 	}
		// case <-rf.heartbeatTimer.C:
		// 	if rf.state == StateLeader {
		// 		rf.mu.Lock()
		// 		defer rf.mu.Unlock()
		// 		fmt.Printf("node %d send heart beat on %d\n", rf.me, rf.currentTerm)
		// 		rf.SendHeartBeat()

		// 	}
		// default:
		// 	fmt.Printf("not timer\n")
		// }

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       StateFollower,
		currentTerm: 0,
		votedfor:    -1,
	}
	rf.resetElectionTimeout()
	rf.heartbeatTime = HEARTBEAT
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
