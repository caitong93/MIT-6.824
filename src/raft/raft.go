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
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/caitong93/MIT-6.824/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int32
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int32
	votedFor    int32
	logs        []LogEntry

	// Volatile state on all servers.
	commitIndex int32
	lastApplied int32
	role        int32

	// Volatile state on leaders
	leader *leaderNode

	readyCh         chan struct{}
	appendEntriesCh chan Operation
	requestVoteCh   chan Operation
	resetElectionCh chan time.Time
	electionTimeout time.Duration
	applyCh         chan<- ApplyMsg

	ctx    context.Context
	cancel func()
}

type Operation struct {
	call  func()
	errCh chan error
}

const (
	RaftRoleFollower int32 = iota
	RaftRoleCandidate
	RaftRoleLeader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)
	isleader = rf.isLeader()

	return term, isleader
}

func (rf *Raft) isLeader() bool {
	return rf.role == RaftRoleLeader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int32
	CandidateID  int32
	LastLogIndex int32
	LastLogTerm  int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	op := Operation{
		call: func() {
			rf.handleRequestVote(args, reply)
		},
		errCh: make(chan error),
	}

	rf.requestVoteCh <- op
	err := <-op.errCh
	if err != nil {
		fmt.Println("Err handle RequestVote:", err)
	}
}

func (rf *Raft) requestVoteLoop() {
	select {
	case <-rf.ctx.Done():
		return
	case <-rf.readyCh:
	}

	for {
		select {
		case <-rf.ctx.Done():
			return
		case op := <-rf.requestVoteCh:
			op.call()
			if op.errCh != nil {
				op.errCh <- nil
			}
		}
	}
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("RequestVote done. node=%v,term=%v,role=%v,votedFor=%v. Args: candidate=%v,term=%v,lastLogIndex=%v,lastLogTerm=%v. success=%v", rf.me, rf.currentTerm, rf.role, rf.votedFor, args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm, reply.VoteGranted)
	}()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.logs[lastLogIndex].Term
	if !atLeastUpToDate(args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex) {
		return
	}

	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.resetElectionTimeout()

	return
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = -1
	term = int(rf.currentTerm)
	isLeader = rf.role == RaftRoleLeader
	if !isLeader {
		return index, term, isLeader
	}

	// Append the log to memory
	rf.appendLogs(LogEntry{Command: command, Term: rf.currentTerm})
	index = int(rf.getLastLogIndex())

	// DPrintf("Add %v, node=%v,index=%v,term=%v,logs=%v", command, rf.me, index, term, rf.logs)
	DPrintf("Add %v, node=%v,index=%v,term=%v,lastLogIndex=%v", command, rf.me, index, term, rf.getLastLogIndex())

	// Replicate to followers
	rf.leader.Sync()

	return index, term, isLeader
}

func (rf *Raft) appendLogs(logs ...LogEntry) {
	rf.logs = append(rf.logs, logs...)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.cancel()
	// Your code here, if desired.
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderID     int32
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	op := Operation{
		call: func() {
			rf.handleAppendEntries(args, reply)
		},
		errCh: make(chan error),
	}
	rf.appendEntriesCh <- op
	err := <-op.errCh
	if err != nil {
		fmt.Println("Err handle AppendEntries:", err)
	}
}

func (rf *Raft) appendEntriesLoop() {
	select {
	case <-rf.ctx.Done():
		return
	case <-rf.readyCh:
	}

	for {
		select {
		case <-rf.ctx.Done():
			return
		case op := <-rf.appendEntriesCh:
			op.call()
			if op.errCh != nil {
				op.errCh <- nil
			}
		}
	}
}

func (rf *Raft) getLastLogIndex() int32 {
	return int32(len(rf.logs)) - 1
}

func atLeastUpToDate(lastLogTerm, lastLogIndex, localLastLogTerm, localLastLogIndex int32) bool {
	if lastLogTerm != localLastLogTerm {
		return lastLogTerm >= localLastLogTerm
	}
	return lastLogIndex >= localLastLogIndex
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		defer func() {
			//DPrintf("AppendEntries node=%v,term=%v. Args: leader=%v,commit=%v,term=%v,prevLogIndex=%v,prevLogTerm=%v,entries=%v. success=%v,logs=%v", rf.me, rf.currentTerm, args.LeaderID, args.LeaderCommit, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, reply.Success, rf.logs)
			DPrintf("AppendEntries node=%v,term=%v. Args: leader=%v,commit=%v,term=%v,prevLogIndex=%v,prevLogTerm=%v,entries=%v. success=%v", rf.me, rf.currentTerm, args.LeaderID, args.LeaderCommit, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.Success)
		}()
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.role == RaftRoleLeader && rf.currentTerm == args.Term {
		// If me is leader, then leader ID in args must be greater to make me a follower
		return
	}

	// Become follower if not
	rf.becomeFollower(args.Term)
	reply.Term = rf.currentTerm

	containPrevLog := false
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.logs[lastLogIndex].Term
	if args.PrevLogIndex <= lastLogIndex && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
		containPrevLog = true
	}

	if !containPrevLog {
		return
	}

	if len(args.Entries) > 0 {
		// Check conflict
		prevIndex := args.PrevLogIndex
		for i := args.PrevLogIndex + 1; i <= minInt32(lastLogIndex, args.PrevLogIndex+int32(len(args.Entries))); i++ {
			if !reflect.DeepEqual(rf.logs[i], args.Entries[i-args.PrevLogIndex-1]) {
				DPrintf("Delete conflict logs. node=%v,before=%v,after=%v", rf.me, rf.logs, rf.logs[:i])
				// Delete conflict entries
				rf.logs = rf.logs[:i]
				lastLogIndex = rf.getLastLogIndex()
				lastLogTerm = rf.logs[lastLogIndex].Term
				break
			}
			prevIndex = i
		}

		argsLastLogIndex := args.PrevLogIndex + int32(len(args.Entries))
		argsLastLogTerm := args.Entries[len(args.Entries)-1].Term
		if !atLeastUpToDate(argsLastLogTerm, argsLastLogIndex, lastLogTerm, lastLogIndex) {
			DPrintf("Reject AppendEntries due to too old,node=%v,peer=%v,lastLogTerm=%v,lastLogIndex=%v,localLogs=%#v,logs=%#v", rf.me, args.LeaderID, lastLogTerm, lastLogIndex, rf.logs, args.Entries)
			return
		}

		// Add logs
		rf.appendLogs(args.Entries[prevIndex-args.PrevLogIndex:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		// DPrintf("AppendEntries.commit node=%v,term=%v. Args: leader=%v,commit=%v,term=%v,prevLogIndex=%v,prevLogTerm=%v,entries=%v. success=%v,logs=%v", rf.me, rf.currentTerm, args.LeaderID, args.LeaderCommit, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, reply.Success, rf.logs)
		newCommitIndex := minInt32(rf.getLastLogIndex(), args.LeaderCommit)
		newCommitIndex = minInt32(newCommitIndex, args.PrevLogIndex+int32(len(args.Entries)))
		rf.commit(newCommitIndex)
	}

	reply.Success = containPrevLog
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) commit(commitIndex int32) {
	if commitIndex <= rf.commitIndex {
		return
	}
	originalIndex := rf.commitIndex
	rf.commitIndex = commitIndex
	// DPrintf("Commit. node=%v,from=%v,to=%v,logs=%v", rf.me, originalIndex+1, rf.commitIndex, rf.logs)
	DPrintf("Commit. node=%v,from=%v,to=%v", rf.me, originalIndex+1, rf.commitIndex)
}

func (rf *Raft) applyCommit() {
	<-rf.readyCh
	nextIndex := 1
	for {
		startTime := time.Now()
		var logs []LogEntry
		rf.mu.Lock()
		commitIndex := int(rf.commitIndex)
		count := commitIndex - nextIndex + 1
		if count > 0 {
			logs = make([]LogEntry, count, count)
			copy(logs, rf.logs[nextIndex:commitIndex+1])
			// DPrintf("ApplyMsg %v. node=%v,from=%v,to=%v", logs, rf.me, nextIndex, commitIndex)
		}
		rf.mu.Unlock()

		if count > 0 {
			for i := nextIndex; i <= commitIndex; i++ {
				msg := ApplyMsg{
					Index:   i,
					Command: logs[i-nextIndex].Command,
				}
				rf.applyCh <- msg
			}
			nextIndex = commitIndex + 1
		}

		if time.Since(startTime) > 100*time.Millisecond {
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) becomeFollower(term int32) {
	isLeader := rf.role == RaftRoleLeader
	if rf.role != RaftRoleFollower {
		DPrintf("Node %v become follower, term=%v, role=%v", rf.me, rf.currentTerm, rf.role)
		rf.role = RaftRoleFollower
	}
	rf.updateTerm(term)

	if isLeader {
		rf.leader.Stop()
		rf.leader = nil
	}

	rf.resetElectionTimeout()
}

func (rf *Raft) updateTerm(term int32) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func (rf *Raft) gatherVotes() {
	rf.mu.Lock()
	me, term, peerNum := rf.me, rf.currentTerm, len(rf.peers)

	if rf.role != RaftRoleCandidate {
		rf.mu.Unlock()
		return
	}
	if rf.votedFor != -1 && rf.votedFor != rf.me {
		rf.mu.Unlock()
		return
	}

	DPrintf("Gather votes, node=%v, term=%v, votedFor=%v, role=%v", rf.me, rf.currentTerm, rf.votedFor, rf.role)

	resultCh := make(chan *RequestVoteReply, len(rf.peers)-1)
	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}

		node := i
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.logs[lastLogIndex].Term
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go func() {
			// fmt.Println("ReqeustVote from", me, "to", node, "start")
			reply := &RequestVoteReply{}
			rf.sendRequestVote(node, args, reply)
			resultCh <- reply
			// fmt.Println("ReqeustVote from", me, "to", node, "end")
		}()
	}
	rf.mu.Unlock()

	grantedVotes := 1
	defer func() {
		DPrintf("Got %v votes, node=%v, term=%v", grantedVotes, me, term)
	}()

	for reply := range resultCh {
		rf.mu.Lock()
		if rf.currentTerm != term || rf.role != RaftRoleCandidate {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			grantedVotes++
			if grantedVotes*2 > peerNum {
				rf.becomeLeader()
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
	}
	return
}

func (rf *Raft) becomeLeader() {
	DPrintf("Node %v become leader", rf.me)
	rf.role = RaftRoleLeader
	rf.leader = newLeaderNode(rf)
	rf.resetElectionTimeout()
}

func (rf *Raft) resetElectionTimeout() {
	now := time.Now()
	select {
	case <-rf.resetElectionCh:
	default:
	}
	rf.resetElectionCh <- now
}

func (rf *Raft) election() {
	var electionCh <-chan time.Time
	for {
		select {
		case <-rf.ctx.Done():
			return
		default:
		}

		rf.mu.Lock()
		if rf.role == RaftRoleLeader {
			electionCh = nil
		} else {
			rf.electionTimeout = randElectionTimeout()
			electionCh = time.After(rf.electionTimeout)
		}
		rf.mu.Unlock()

		select {
		case <-rf.ctx.Done():
			return
		case <-rf.resetElectionCh:
			// DPrintf("Election reset, node=%v", rf.me)
			continue
		case <-electionCh:
			DPrintf("Election timeout, node=%v", rf.me)

			select {
			case <-rf.resetElectionCh:
				// DPrintf("Election reset, node=%v", rf.me)
				continue
			default:
			}

			rf.mu.Lock()
			if rf.role == RaftRoleFollower {
				// Become cadidate and vote for self
				rf.role = RaftRoleCandidate
				rf.votedFor = rf.me
			}
			if rf.role == RaftRoleCandidate {
				rf.currentTerm++
				go rf.gatherVotes()
			}
			rf.mu.Unlock()
		}
	}
}

func randElectionTimeout() time.Duration {
	return durationRange(150*time.Millisecond, 300*time.Millisecond)
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
// Notes(Tong): test multiple times to find every possible bug, go test -race -failfast  -count 50   -run TestInitialElection2A
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)

	// Your initialization code here (2A, 2B, 2C).
	rf.role = RaftRoleFollower
	rf.electionTimeout = randElectionTimeout()
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{})
	rf.resetElectionCh = make(chan time.Time, 1)
	rf.appendEntriesCh = make(chan Operation, 100)
	rf.requestVoteCh = make(chan Operation, 100)
	rf.readyCh = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	rf.ctx = ctx
	rf.cancel = cancel
	rf.electionTimeout = randElectionTimeout()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.election()
	go rf.requestVoteLoop()
	go rf.appendEntriesLoop()
	go rf.applyCommit()
	close(rf.readyCh)

	return rf
}
