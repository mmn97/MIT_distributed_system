package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.attempt(command interface{}) (index, term, isleader)
//   attempt agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

var (
	TimeoutMIN        int           = 210
	TimeoutMAX        int           = 350
	HeartbeatDuration time.Duration = time.Duration(110) * time.Millisecond
)

type State string

const (
	Candidate State = "candidate"
	Leader    State = "leader"
	Follower  State = "follower"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

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
	currentTerm     int           // current term of this peer
	votedFor        int           // the id of the candidate this peer votes for in current term
	logs            []Log         // log entries of this peer
	state           State         // current state
	lastcalled      time.Time     // the time when this peer received last message
	electionTimeout time.Duration // current randomized election timeout
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	cond            *sync.Cond
}

// A struct to hold information about each log entry
type Log struct {
	Command interface{}
	Term    int
	Index   int
}

func randomTimeDuration() time.Duration {
	min := TimeoutMIN
	max := TimeoutMAX
	randN := min + rand.Intn(max-min)
	return time.Duration(randN) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and reattempt.
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
// example RequestVote RPC arguments structure.
// field names must attempt with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must attempt with capital letters!
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
	// 2A
	rf.mu.Lock()
	rf.lastcalled = time.Now()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm && rf.votedFor != args.CandidateID && rf.votedFor != -1 {
		reply.VoteGranted = false
	} else if len(rf.logs) >= 1 && args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		reply.VoteGranted = false
	} else if len(rf.logs) >= 1 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)-1 {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = args.Term
		}
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
	return ok
}

type AppendEntiresAgrs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RPC handler of append log entires
func (rf *Raft) AppendEntries(args *AppendEntiresAgrs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.lastcalled = time.Now()
	prevLogIndex := args.PrevLogIndex
	if args.Term < rf.currentTerm ||
		(prevLogIndex >= 0 && rf.logs[prevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
	} else {
		reply.Success = true
		if args.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = args.Term
		}
		if len(args.Entries) > 0 {
			DPrintf("lens of entry for empty heart beat is %v.\n", len(args.Entries))
			idx := prevLogIndex + 1
			i := 0
			for ; i < len(args.Entries); i++ {
				if rf.logs[idx] == args.Entries[i] {
					idx++
					continue
				} else {
					break
				}
			}
			for j := i; j < len(args.Entries); j++ {
				rf.logs[idx] = args.Entries[j]
				idx++
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if len(rf.logs)-1 < rf.commitIndex {
					rf.commitIndex = len(rf.logs) - 1
				}
			}
		}

	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntiresAgrs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to attempt
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise attempt the
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
	rf.mu.Lock()
	index = rf.commitIndex + 1
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()
	// if isLeader {
	// 	rf.mu.Lock()
	// 	rf.logs = append(rf.logs, Log{command, term, index})
	// 	rf.mu.Unlock()
	// 	// ask other servers to append the new log entry
	// 	go sendLog(rf)
	// }
	return index, term, isLeader
}

func sendLog(rf *Raft) {
	cnt := 1
	cond := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// append entry if last log index >= nextIndex
		go func(i int) {
			for len(rf.logs)-1 >= rf.nextIndex[i] {
				rf.mu.Lock()
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.logs[prevLogIndex].Term
				}
				args := AppendEntiresAgrs{rf.currentTerm, rf.me, prevLogIndex,
					prevLogTerm, rf.logs[prevLogIndex+1 : len(rf.logs)], rf.commitIndex}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if !rf.sendAppendEntries(i, &args, &reply) {
					continue
				}
				rf.mu.Lock()
				if reply.Success {
					cnt++
					rf.nextIndex[i] = len(rf.logs)
					rf.matchIndex[i] = len(rf.logs) - 1
					cond.Broadcast()
				} else {
					rf.nextIndex[i]--
				}
				rf.mu.Unlock()
			}
		}(i)
	}
	rf.mu.Lock()
	for cnt < len(rf.peers)/2 {
		cond.Wait()
	}
	rf.commitIndex++
	rf.cond.Broadcast()
	rf.mu.Unlock()
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should attempt goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.cond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1           // Raft peer has not voted for any servers at first
	rf.state = Follower        // Raft peer begins as a follower.
	rf.lastcalled = time.Now() // use attempting time as the initial value
	rf.electionTimeout = randomTimeDuration()
	//fmt.Printf("Election timeout duration is %v.\n", rf.electionTimeout)
	rf.currentTerm = 0
	rf.logs = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// attempt a background goroutine to kick off leader-election periodcally
	go work(rf)
	//go apply(rf, applyCh)
	// Follower will transfer to a candidate if he does not receive message within election timeout.
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
func apply(rf *Raft, applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		len := len(rf.logs)
		rf.mu.Unlock()
		if len == 0 {
			time.Sleep(5 * time.Millisecond)
		} else {
			rf.mu.Lock()
			for rf.lastApplied >= rf.commitIndex {
				rf.cond.Wait()
			}
			rf.lastApplied++
			msg := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.logs[rf.lastApplied].Index}
			rf.mu.Unlock()
			applyCh <- msg
		}

	}
}

func work(rf *Raft) {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == Follower {
			rf.mu.Lock()
			if time.Since(rf.lastcalled) > rf.electionTimeout {
				rf.state = Candidate
			} else {
				time.Sleep(5 * time.Millisecond)
			}
			rf.mu.Unlock()
		} else if state == Candidate {
			attemptElection(rf)
		} else {
			for state == Leader {
				sendHeartBeat(rf)
				rf.mu.Lock()
				curState := rf.state
				rf.mu.Unlock()
				if curState == Leader {
					time.Sleep(HeartbeatDuration)
				}
			}

		}
	}

}

// send heartbeat(empyt AppendEntries RPC) to all other peers to maintain authority as a leader
func sendHeartBeat(rf *Raft) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.logs[prevLogIndex].Term
		}
		args := AppendEntiresAgrs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		//DPrintf("Send heart beats. \n")
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		go func(i int) {
			if !rf.sendAppendEntries(i, &args, &reply) {
				return
			}
			rf.mu.Lock()
			curTerm := rf.currentTerm
			defer rf.mu.Unlock()
			if reply.Term > curTerm {
				rf.state = Follower
				rf.currentTerm = reply.Term
			}
		}(i)
	}
}

func attemptElection(rf *Raft) {
	rf.mu.Lock()
	// Increment current term; reset the election time out; vote for itself
	rf.currentTerm++
	curTerm := rf.currentTerm
	rf.electionTimeout = randomTimeDuration()
	rf.votedFor = rf.me
	lastLogIndex := len(rf.logs) - 1
	rf.mu.Unlock()
	//DPrintf("The initial length of log is %v. \n", len(rf.logs))
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	t := time.Now()
	cnt := 1
	// Send requestVote to all other peers in parallel
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Candidate {
			break
		}
		go func(i int) {
			rf.mu.Lock()
			state := rf.state
			args := RequestVoteArgs{rf.me, rf.currentTerm, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			if state != Candidate {
				return
			}
			if !rf.sendRequestVote(i, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				DPrintf("%v gets vote! \n", rf.me)
				cnt++
				if cnt > len(rf.peers)/2 {
					rf.state = Leader
				}
			} else {
				if reply.Term > curTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
				}
			}
		}(i)
	}

	for time.Since(t) < rf.electionTimeout {
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}
