package raft

import (
	"context"
	"sync"
	"time"
)

type leaderNode struct {
	mu           sync.RWMutex
	me           int32
	rf           *Raft
	nextIndex    []int32
	matchIndex   []int32
	followers    []*followerManager
	syncResultCh chan syncResult
	ctx          context.Context
	cancel       func()
}

type syncResult struct {
	server       int32
	term         int32
	lastLogIndex int32
	success      bool
}

// rf should be locked to call newLeaderNode
func newLeaderNode(rf *Raft) *leaderNode {
	peerNum := len(rf.peers)
	ctx, cancel := context.WithCancel(rf.ctx)
	leader := &leaderNode{
		me:        rf.me,
		rf:        rf,
		ctx:       ctx,
		cancel:    cancel,
		nextIndex: make([]int32, peerNum, peerNum),
		// matchIndex is initialized to 0, increases monotonically
		matchIndex:   make([]int32, peerNum, peerNum),
		syncResultCh: make(chan syncResult, peerNum),
	}

	// nextIndex is initialized to leader last log index + 1
	nextIndex := int32(len(rf.logs))
	for i := 0; i < peerNum; i++ {
		leader.nextIndex[i] = nextIndex
	}

	// TODO: configurable
	syncInterval := 500 * time.Millisecond
	for i := 0; i < peerNum; i++ {
		var fm *followerManager
		if i != int(rf.me) {
			fm = &followerManager{
				ctx:          ctx,
				server:       i,
				syncInterval: syncInterval,
				notifyCh:     make(chan chan<- syncResult),
				rf:           rf,
				leader:       leader,
			}
			fm.run()
		}
		leader.followers = append(leader.followers, fm)
	}

	go leader.handleResults()

	return leader
}

func (l *leaderNode) getMatchIndex(server int) int32 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.matchIndex[server]
}

// Sync starts sync log to all followers and return immediately
func (l *leaderNode) Sync() {
	for i := range l.followers {
		if i == int(l.me) {
			continue
		}
		l.followers[i].Sync(l.syncResultCh)
	}
}

func (l *leaderNode) handleResults() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case r := <-l.syncResultCh:
			if r.success {
				l.onSyncSuccess(&r)
			} else {
				l.onSyncFailure(&r)
			}
		}
	}
}

func (l *leaderNode) onSyncFailure(r *syncResult) {
	rf := l.rf
	rf.mu.Lock()
	defer rf.mu.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-l.ctx.Done():
		return
	default:
	}

	server := r.server
	// After a rejection, the leader decrements nextIndex and reties the AppendEntries RPC
	if l.nextIndex[server] > 1 {
		l.nextIndex[server]--
	}
	DPrintf("On failure, update nextIndex. node=%v,follower=%v,nextIndex=%v,followerTerm=%v", l.me, server, l.nextIndex[server], r.term)
	if r.term > rf.currentTerm {
		DPrintf("Follower have higher term than leader. leader=%v,term=%v,follower=%v,followerTerm=%v", rf.me, rf.currentTerm, server, r.term)
		// Leader update term and become follower, then start election
		rf.becomeFollower(r.term)
	} else {
		// TODO: rate limit on failure retry
		l.followers[r.server].Sync(l.syncResultCh)
	}
}

func (l *leaderNode) onSyncSuccess(r *syncResult) {
	server := r.server
	lastLogIndex := r.lastLogIndex
	rf := l.rf

	rf.mu.Lock()
	defer rf.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	select {
	case <-l.ctx.Done():
		return
	default:
	}

	peers := len(l.followers)
	commitIndex := rf.commitIndex
	totalLogs := int32(len(rf.logs))

	l.nextIndex[server] = maxInt32(l.nextIndex[server], lastLogIndex+1)
	l.matchIndex[server] = maxInt32(l.matchIndex[server], lastLogIndex)
	for i := commitIndex + 1; i < totalLogs; i++ {
		cnt := 1
		for j := 0; j < peers; j++ {
			if j == int(rf.me) {
				continue
			}
			if l.matchIndex[j] >= i {
				cnt++
			}
		}
		if cnt*2 > peers {
			commitIndex = i
		} else {
			break
		}
	}

	rf.commit(commitIndex)
	DPrintf("On success, update nextIndex. leader=%v,commitIndex=%v,follower=%v,nextIndex=%v,followeTerm=%v", l.me, rf.commitIndex, server, l.nextIndex[server], r.term)
}

func (l *leaderNode) Stop() {
	l.cancel()
}
