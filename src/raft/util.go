package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 最小值min
func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func GetRandomExpireTime(left, right int32) time.Time {
	t := rand.Int31n(right - left)
	return time.Now().Add(time.Duration(t+left) * time.Millisecond)
}

// UpToDate paper中投票RPC的rule2
func (rf *Raft) UpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) ChangeToFollower(new_term int, vote_for int) {
	rf.status = Follower
	rf.votedFor = vote_for
	rf.currentTerm = new_term
	rf.persist()
}

func (rf *Raft) ChangeToCandidate() {
	rf.status = Candidate
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.currentTerm += 1
	rf.persist()
}

func (rf *Raft) changeToLeader() {
	rf.status = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
}

// get the first dummy log index
func (rf *Raft) getFirstIndex() int {
	return rf.logs[0].Index
}

// get the first dummy log term
func (rf *Raft) getFirstTerm() int {
	return rf.logs[0].Term
}

// get the last log term
func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

// get the last log index
func (rf *Raft) getLastIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

// get the Term of index
// compute the location in log and return the result
func (rf *Raft) getTermForIndex(index int) int {
	return rf.logs[index-rf.getFirstIndex()].Term
}

// get the command of index
// compute the location in log and return the result
func (rf *Raft) getCommand(index int) interface{} {
	return rf.logs[index-rf.getFirstIndex()].Command
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
