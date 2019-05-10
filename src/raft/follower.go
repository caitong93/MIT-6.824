package raft

import (
	"context"
	"time"
)

type followerManager struct {
	ctx          context.Context
	server       int
	syncInterval time.Duration
	notifyCh     chan chan<- syncResult
	rf           *Raft
	leader       *leaderNode
	// syncing      int32
}

func (fm *followerManager) Sync(resultCh chan syncResult) {
	go func() {
		select {
		case <-fm.ctx.Done():
			return
		case fm.notifyCh <- resultCh:
		}
	}()
}

func (fm *followerManager) run() {
	go fm.loop()
}

// TODO: should backoff when sync failed too many times ?
func (fm *followerManager) loop() {
	// TODO: configurable
	heartbeatInterval := 50 * time.Millisecond
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	syncCh := time.After(fm.syncInterval)

	// Send heartbeat immediately
	go fm.syncLogs(fm.ctx, fm.server, true, nil)

	for {
		var resultCh chan<- syncResult
		select {
		case <-fm.ctx.Done():
			DPrintf("Follower manager %v quit", fm.server)
			return
		case <-heartbeatTicker.C:
			go fm.syncLogs(fm.ctx, fm.server, true, nil)
			continue
		case resultCh = <-fm.notifyCh:
		case <-syncCh:
		}

		go fm.syncLogs(fm.ctx, fm.server, false, resultCh)

		// Reset next sync time
		syncCh = time.After(fm.syncInterval)
	}
}

/*
func (fm *followerManager) isSyncing() bool {
	return atomic.LoadInt32(&fm.syncing) == 1
}

func (fm *followerManager) setIsSyncing(val int32) bool {
	return atomic.CompareAndSwapInt32(&fm.syncing, 1-val, val)
}
*/

// syncLogs is a long running goroutine which syncs leader logs to a follower
func (fm *followerManager) syncLogs(ctx context.Context, server int, heartbeat bool, resultCh chan<- syncResult) {
	fm.rf.mu.Lock()
	rf := fm.rf
	leader := fm.leader

	select {
	case <-ctx.Done():
		rf.mu.Unlock()
		return
	default:
	}

	leader.mu.RLock()
	prevLogIndex := leader.nextIndex[server] - 1
	lastLogIndex := int32(len(rf.logs) - 1)
	me := int32(rf.me)

	args := &AppendEntriesArgs{
		Term:         int32(rf.currentTerm),
		LeaderID:     me,
		PrevLogIndex: int32(prevLogIndex),
		PrevLogTerm:  int32(rf.logs[prevLogIndex].Term),
		LeaderCommit: rf.commitIndex,
	}
	if !heartbeat {
		args.Entries = rf.logs[prevLogIndex+1:]
	}

	leader.mu.RUnlock()
	rf.mu.Unlock()

	if !heartbeat && len(args.Entries) == 0 {
		return
	}

	// DPrintf("Sync from %v to %v", rf.me, server)
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	if heartbeat {
		// Not handled for now.
		// TODO: heartbeat response can be used to check follower healthy.
		return
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	if resultCh != nil {
		resultCh <- syncResult{
			server:       int32(server),
			term:         reply.Term,
			lastLogIndex: lastLogIndex,
			success:      reply.Success,
		}
	}

	return
}
