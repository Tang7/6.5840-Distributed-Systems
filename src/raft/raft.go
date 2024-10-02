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

	"math/rand"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"

	"github.com/sasha-s/go-deadlock"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu deadlock.Mutex
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role Role

	lastHeartbeat time.Time

	// Persistent state on all servers:
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be on server

	appendEntriesCh chan *AppendEntriesArgs
}

type LogEntry struct {
	Term    int         // term when entry was received by leader
	Command interface{} // command for state machine
}

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.role == Leader
	DPrintf("GetSate Server %d, term: %d, isLeader: %v", rf.me, term, isLeader)
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("RequestVote received from Server %d to Server %d, term: %d, currentTerm: %d", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	rf.lastHeartbeat = time.Now()

	rf.mu.Unlock()

	if rf.isStaleTerm(args.Term) {
		return
	}

	rf.becomeFollower(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// votes for at most one candidate per term (first-come-first-served)
	DPrintf("RequestVote received from Server %d to Server %d, current server vote for server %d", args.CandidateId, rf.me, rf.votedFor)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		DPrintf("Server %d voted for server %d at term %d", rf.me, args.CandidateId, rf.currentTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.Success = false

	rf.lastHeartbeat = time.Now()

	rf.mu.Unlock()

	if rf.isStaleTerm(args.Term) {
		return
	}

	rf.becomeFollower(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("AppendEntries sent via channel from Leader %d to Server %d at term %d", args.LeaderId, rf.me, args.Term)

	if rf.role != Leader {
		go func() {
			rf.appendEntriesCh <- args
		}()
	}

	// treat the sender as a leader, and change the state to follower upon receiving the request
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.role = Follower

	reply.Success = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

// To begin an election, a follower increments its current term and transitions to candidate state.
// It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
// A candidate continues in this state until one of three things happens:
// (a) it wins the election,
// (b) another server establishes itself as leader,
// (c) a period of time goes by with no winner.

// Election Process:
// 1. Winning condition:
//    - Candidate receives votes from a majority of servers in the cluster for the same term
//    - Each server votes for at most one candidate per term (first-come-first-served)
//    - Majority rule ensures only one winner per term (Election Safety Property)
//
// 2. Upon winning:
//    - Candidate becomes leader
//    - Increase term and Sends heartbeat messages to all other servers to establish authority and prevent new elections
//
// 3. While waiting for votes:
//    - If AppendEntries RPC received from another server claiming leadership:
//      a. If RPC term >= candidate's term: recognize as leader and become follower
//      b. If RPC term < candidate's term: reject RPC and continue as candidate
//
// 4. Split vote scenario:
//    - Multiple candidates may split the votes, preventing a majority
//    - In this case, candidates timeout and start new election:
//      a. Increment term
//      b. Initiate new round of RequestVote RPCs
//    - Note: Without additional measures, split votes could repeat indefinitely
//
// 5. Leader election timeout:
//    - If no leader is elected within a timeout period, a new election is triggered

// Follower:
//
// Followers only respond to requests from other servers:
// 	1. AppendEntries from leader.
//  2. RequestVote from candidate.
//
// If a follower receives no communication over a period of time called the election timeout,
//  1. No AppendEntries from leader.
//  2. No RequestVote from candidate. Split vote, no more RequestVote come.
// then it assumes there is no viable leader and begins an election to choose a new leader.
//
// Follower -> Candidate:
//  1. F.1 && F.2

// Candidate:
//
// Candidate -> Leader:
//  1. when receives votes from a majority of servers.(term is the same as lastTerm)
//
// Candidate -> Follower:
//  1. when receives AppendEntries from leader. And leader's term >= candidate's term.
//  2. receives message with higher term.
//
// Candidate -> Candidate:
//  1. election timeout (split vote, no AppendEntries come), start a new election.
//  2. when receives AppendEntries from leader. And leader's term < candidate's term.

// Leader:
//
// Leader -> Follower:
//  1. Receive message with higher term.
//  2. Dead.

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// each term begins with an election, make sure startElection is needed.
	// don't start arbitrary election! e.g., when there is already a leader and it is not dead.
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	lastTerm := rf.currentTerm

	// election timeout:
	// 1. Random time between 150ms and 300ms
	// 2. Each candidate restarts its randomized election timeout at the start of an election
	timeout := rf.getElectionTimeout()

	DPrintf("startElection Server %d, term: %d", rf.me, rf.currentTerm)

	rf.mu.Unlock()

	votes := 1
	peersNum := len(rf.peers)
	voteCh := make(chan int, peersNum-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.getVote(i, lastTerm, voteCh)
	}

	// Finish this election and handle election restart to ticker() if:
	// 1. timeout: no communication from other servers.
	// 2. election done:
	//    - Become Leader: Receive votes from a majority of servers.
	//    - Become Follower: Another server establishes itself as leader.
	for !rf.killed() {
		rf.mu.Lock()
		timeoutDone := time.Since(rf.lastHeartbeat) > timeout
		rf.mu.Unlock()

		if timeoutDone {
			break
		}

		// TODO: better approach to handle election with voteCh and appendEntriesCh?
		done := false
		votes, done = rf.handleElection(voteCh, rf.appendEntriesCh, votes, peersNum, lastTerm)
		if done {
			break
		}
	}
}

func (rf *Raft) getVote(server int, lastTerm int, voteChan chan int) {
	args := &RequestVoteArgs{
		Term:        lastTerm,
		CandidateId: rf.me,
	}
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(server, args, reply) {
		if rf.ignoreVote(lastTerm) {
			return
		}
		if rf.becomeFollower(reply.Term) {
			return
		}

		if reply.VoteGranted {
			rf.mu.Lock()
			DPrintf("Server %d received vote from server %d at term %d, vote granted: %v", rf.me, server, rf.currentTerm, reply.VoteGranted)
			voteChan <- server
			rf.mu.Unlock()
		}
	}
}

// if the server is not a candidate or the term is not the same, ignore the vote
func (rf *Raft) ignoreVote(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role != Candidate || rf.currentTerm != term
}

func (rf *Raft) handleElection(voteCh <-chan int, appendEntriesCh <-chan *AppendEntriesArgs, votes, peersNum, lastTerm int) (int, bool) {
	electionDone := false
	// TODO: should check if current server is a candidate? Otherwise, no need return from this function!
	select {
	case id := <-voteCh:
		votes++
		rf.mu.Lock()
		DPrintf("Server %d received vote from server %d, current votes: %d, limit votes: %d", rf.me, id, votes, peersNum/2)
		rf.mu.Unlock()
		if votes > peersNum/2 {
			if rf.becomeLeader(lastTerm) {
				go rf.sendHeartbeats()
				electionDone = true
			}
		}
	case args := <-appendEntriesCh:
		rf.mu.Lock()
		DPrintf("AppendEntries acknowledged from Leader %d to Server %d at term %d", args.LeaderId, rf.me, args.Term)
		rf.mu.Unlock()
		if rf.becomeFollower(args.Term) {
			electionDone = true
		}
	default:
		electionDone = false
	}
	return votes, electionDone
}

func (rf *Raft) getElectionTimeout() time.Duration {
	timeout := 200 + rand.Intn(200)
	return time.Duration(timeout) * time.Millisecond
}

// Increase term and Sends heartbeat messages to all other servers to establish authority and prevent new elections
func (rf *Raft) becomeLeader(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Candidate && rf.currentTerm == term {
		DPrintf("Leader is Server %d at term %d", rf.me, term)
		// Increase term upon on winning election
		rf.role = Leader
		rf.currentTerm++

		return true
	}
	return false
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		// lastTerm is the term when leader sends the request
		lastTerm, isLeader := rf.GetState()
		if !isLeader {
			return
		}
		// prepare the AppendEntriesArgs which will be sent to each follower from this rf
		rf.mu.Lock()
		args := make([]*AppendEntriesArgs, len(rf.peers))
		for i := range rf.peers {
			args[i] = &AppendEntriesArgs{
				Term:     lastTerm,
				LeaderId: rf.me,
				Entries:  []LogEntry{}, // empty for heartbeat
			}
		}
		rf.mu.Unlock()

		for i, arg := range args {
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			if i != rf.me {
				go func(server int, arg *AppendEntriesArgs) {
					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(server, arg, reply) {
						// always compare with the lastTerm when leader sends the request
						if reply.Term > lastTerm {
							rf.becomeFollower(reply.Term)
							return
						}
					}
				}(i, arg)
			}
		}

		time.Sleep(HeartbeatInterval)
	}
}

const HeartbeatInterval = 100 * time.Millisecond

// becomeFollower changes the role of the server to follower if the term is greater than the current term
func (rf *Raft) becomeFollower(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		DPrintf("Server %d becomeFollower term: %d, currentTerm: %d", rf.me, term, rf.currentTerm)
		// sync the term with the leader
		rf.currentTerm = term
		rf.role = Follower
		rf.votedFor = -1
		return true
	}
	return false
}

// If a server receives a request with a stale term number, it rejects the request.
func (rf *Raft) isStaleTerm(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return term < rf.currentTerm
}

// start election:
// 1. Follower: does not receive message from peer for a period of time (election timeout).
// 2. Candidate: vote split, no more RequestVote come.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		if role != Leader {
			rf.startElection()
		}

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// server starts as a follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{{Term: 0, Command: nil}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.appendEntriesCh = make(chan *AppendEntriesArgs)

	rf.role = Follower
	rf.lastHeartbeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
