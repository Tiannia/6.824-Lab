package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"time"
)

//
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyChan passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyChan, but set CommandValid to false for these
// other uses.
//

// Status 节点的角色
type Status int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

// InstallSnapshotState 安装快照的状态
type InstallSnapshotState int

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

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

const (

	// 定义随机生成投票过期时间范围:(ElectionExpireLeft ~ ElectionExpireRight)
	ElectionExpireLeft  = 200
	ElectionExpireRight = 350

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	// ElectionTimerResolution 每隔一个此时间段就会判断是否需要发起选主投票
	// AppendTimerResolution 每隔一个此时间段就会尝试发送AppendEntries
	HeartbeatSleep          = 120
	ElectionTimerResolution = 5
	AppendTimerResolution   = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's status
	cv        *sync.Cond          // condition variable for applych
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted status
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有的servers需要持久化的变量:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	// 对于正常流程来说应该先applyLog进chan里，然后更新commit，最后两者应该是相同的，只是先后更新顺序不同
	commitIndex int
	lastApplied int

	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	applyChan chan ApplyMsg // 用来写入通道

	// paper外自己追加的
	status             Status
	voteNum            int
	AppendExpireTime   []time.Time // next append time
	ElectionExpireTime time.Time   // election expire time
	commitQueue        []ApplyMsg
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// --------------------------------------------------------RPC参数部分----------------------------------------------------

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引(2D包含快照
	LastLogTerm  int // 候选人最后日志条目的任期号(2D包含快照
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool // 是否投票给了该竞选人
}

// AppendEntriesArgs Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int        // 用于匹配日志的任期是否是合适的是，是否有冲突
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool // 如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	UpNextIndex int  // 如果发生conflict时reply传过来的正确的下标用于更新nextIndex[i]
	// 用于快速回滚，简单说明：如果发送AppEntry的时候Follower拒绝了，
	// 这个ConflictTerm可以记录冲突的Term，可以让Leader快速更新NextIndex[i]，具体情况请看:
	// https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt (how to roll back quickly)
	// https://pdos.csail.mit.edu/6.824/papers/raft2-faq.txt (Q: I'm a little confused by the "how to roll back quickly"...)
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	Term              int    // 发送请求方的任期
	LeaderId          int    // 请求方的LeaderId
	LastIncludedIndex int    // 快照最后applied的日志下标
	LastIncludedTerm  int    // 快照最后applied时的当前任期
	Data              []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent status, and also initially holds the most
// recent saved status, if any. applyChan is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	serverNum := len(peers)

	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		currentTerm:      0,
		status:           Follower,
		votedFor:         -1,
		nextIndex:        make([]int, serverNum),
		matchIndex:       make([]int, serverNum),
		commitQueue:      make([]ApplyMsg, 0),
		applyChan:        applyCh,
		AppendExpireTime: make([]time.Time, serverNum),
		logs:             []LogEntry{},
	}

	// Your initialization code here (2A, 2B, 2C).
	// 注意：日志索引为0存储快照元数据，即Index = lastIncludedIndex， Term = lastIncludedTerm
	rf.logs = append(rf.logs, LogEntry{
		Index:   0,
		Term:    0,
		Command: nil,
	})

	rf.cv = sync.NewCond(&rf.mu)

	rf.ResetElectionTimer()
	for i := 0; i < len(peers); i++ {
		rf.ResetAppendTimer(i, false)
	}

	// initialize from status persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.getFirstIndex()
	rf.commitIndex = rf.getFirstIndex()

	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

// --------------------------------------------------------ticker部分----------------------------------------------------

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// 时间过期发起选举
		time.Sleep(ElectionTimerResolution * time.Millisecond)
		// 此处的流程为每次每次votedTimer如果小于在sleep睡眠之前定义的时间，就代表没有votedTimer被更新为最新的时间，则发起选举
		rf.mu.Lock()
		if time.Now().After(rf.ElectionExpireTime) && rf.status != Leader {
			// 转变状态为候选者
			rf.ChangeToCandidate()
			rf.sendElection()
			rf.ResetElectionTimer()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(AppendTimerResolution * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {
			rf.leaderAppendEntries()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for !rf.killed() {
		rf.mu.Lock()
		for len(rf.commitQueue) == 0 {
			rf.cv.Wait()
		}
		Messages := rf.commitQueue
		rf.commitQueue = make([]ApplyMsg, 0)
		rf.mu.Unlock()

		for _, message := range Messages {
			rf.applyChan <- message
		}
	}

}

//----------------------------------------------leader选举部分------------------------------------------------------------

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			rf.getLastIndex(),
			rf.getLastTerm(),
		}
		// 开启协程对各个节点发起选举
		go func(server int, args RequestVoteArgs) {

			reply := RequestVoteReply{}
			res := rf.sendRequestVote(server, &args, &reply)

			if res {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term != rf.currentTerm {
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > rf.currentTerm {
					rf.ChangeToFollower(reply.Term, -1)
					rf.ResetElectionTimer()
					return
				}

				// reply.Term == rf.currentTerm == args.Term
				// 返回结果正确就去判断是否大于一半节点同意
				if reply.VoteGranted {
					rf.voteNum += 1
					if rf.voteNum > len(rf.peers)/2 {

						DPrintf("[Election]-> S[%v] to be leader, Term is: %v\n", rf.me, rf.currentTerm)
						rf.changeToLeader()

						// 立即向Follower发送心跳包
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
							rf.ResetAppendTimer(i, true)
						}
					}
				}
			}

		}(i, args)

	}

}

// RequestVote
// example RequestVote RPC handler.
// 个人认为定时刷新的地方应该是别的节点与当前节点在数据上不冲突时才要刷新
// 因为如果不是数据冲突那么定时相当于防止自身去选举的一个心跳
// 如果是因为数据冲突，那么这个节点不用刷新定时是为了当前整个raft能尽快有个正确的leader
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[SendElection-S(%v)-To(%v)] args:%+v, curStatus%v\n", args.CandidateId, rf.me, args, rf.status)

	// 由于网络分区或者是节点crash，导致的任期比接收者还小
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if args.Term == rf.currentTerm {
		reply.Term = args.Term
		// 任期相等，检查日志，根据Paper：
		// "If votedFor is null or candidateId, and candidate’s logs is at
		// least as up-to-date as receiver’s logs, grant vote."
		// 1. 日志是否有冲突  2. votedFor是否为空，如果不为空，是否投票给自己（可能是重复的rpc请求）
		if rf.UpToDate(args.LastLogIndex, args.LastLogTerm) && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
			reply.VoteGranted = true
			rf.ChangeToFollower(args.Term, args.CandidateId)
			rf.ResetElectionTimer()
		} else {
			reply.VoteGranted = false
		}
	} else {
		// args.Term > rf.currentTerm
		// check log
		reply.Term = args.Term
		if rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			rf.ChangeToFollower(args.Term, args.CandidateId)
		} else {
			reply.VoteGranted = false
			rf.ChangeToFollower(args.Term, -1)
		}
		rf.ResetElectionTimer()
	}
}

// reset Election
func (rf *Raft) ResetElectionTimer() {
	rf.ElectionExpireTime = GetRandomExpireTime(ElectionExpireLeft, ElectionExpireRight)
}

//----------------------------------------------日志增量部分------------------------------------------------------------

func (rf *Raft) leaderAppendEntries() {

	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		if !time.Now().After(rf.AppendExpireTime[index]) {
			continue
		}
		// installSnapshot，如果rf.nextIndex[i]-1小于等lastIncludeIndex,
		// 说明followers的日志小于自身的快照状态，将自己的快照发过去
		// 同时要注意的是比快照还小时，已经算是比较落后
		if rf.nextIndex[index]-1 < rf.getFirstIndex() {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.getFirstIndex(),
				LastIncludedTerm:  rf.getFirstTerm(),
				Data:              rf.persister.ReadSnapshot(),
			}
			go rf.leaderSendSnapShot(index, args)
		} else {
			// 开启协程并发的进行日志增量
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[index] - 1,
				PrevLogTerm:  rf.getTermForIndex(rf.nextIndex[index] - 1),
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[index] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[index]-rf.getFirstIndex():]...)
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{}
			}

			go func(server int, args AppendEntriesArgs) {

				// DPrintf("[TIKER-SendHeart-Rf(%v)-To(%v)] args:%+v, curStatus%v\n", rf.me, server, args, rf.status)
				reply := AppendEntriesReply{}
				res := rf.sendAppendEntries(server, &args, &reply)

				if res {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 不是领导者直接返回，因为日志增量必须从Leader单向流向Follower
					if rf.status != Leader {
						return
					}

					if reply.Term < rf.currentTerm {
						// Maybe: the prevLog is in the snapshot of this peer.
						return
					}

					// 返回的任期更高说明自己可能是处于一个过时的分区，直接恢复为Follower并返回
					if reply.Term > rf.currentTerm {
						rf.ChangeToFollower(reply.Term, -1)
						rf.ResetElectionTimer()
						return
					}

					// 过滤过期的RPC回复
					// 1. when this leader sent x in term i, after that this leader become leader of term i + 2.
					// other server at term i + 2 receive this message and reply false, but it was a out of date reply
					// 2. nextIndex has been changed, ignore this reply.
					if args.Term != rf.currentTerm || rf.nextIndex[server] != args.PrevLogIndex+1 {
						return
					}

					if reply.Success {
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1

						diff := make([]int, rf.getLastIndex()+5)
						for idx := 0; idx < len(rf.peers); idx++ {
							if idx == rf.me {
								continue
							}
							diff[0] += 1
							diff[rf.matchIndex[idx]+1] -= 1
						}
						ok_idx := 0
						for idx := 1; idx < len(diff); idx++ {
							diff[idx] += diff[idx-1]
							if diff[idx]+1 > len(rf.peers)/2 {
								ok_idx = idx
							}
						}
						if ok_idx > rf.commitIndex && rf.getTermForIndex(ok_idx) == rf.currentTerm {
							rf.commitIndex = ok_idx
							for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
								rf.lastApplied++
								rf.commitQueue = append(rf.commitQueue, ApplyMsg{
									SnapshotValid: false,
									CommandValid:  true,
									CommandIndex:  rf.lastApplied,
									Command:       rf.getCommand(rf.lastApplied),
								})
							}
							rf.cv.Broadcast()
						}

					} else {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[server] = reply.UpNextIndex
						} else { // Case 2: nextIndex = leader's last entry for XTerm
							findIdx := -1
							for index := rf.getLastIndex(); index > rf.getFirstIndex(); index-- {
								if rf.getTermForIndex(index) == reply.ConflictTerm {
									findIdx = index
									break
								}
							}
							if findIdx != -1 {
								rf.nextIndex[server] = findIdx
							} else {
								rf.nextIndex[server] = reply.UpNextIndex
							}
						}
					}
					// whether success or not, if the nextIndex[idx] is not the len(rf.log)
					// means this follower log is not matched, seend appendEntry to this
					// peer immediately
					if rf.nextIndex[server] != rf.getLastIndex()+1 {
						rf.ResetAppendTimer(server, true)
					}
				}
			}(index, args)
		}

		rf.ResetAppendTimer(index, false)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if args.Term >= curTerm, check and sync the log.
	curLastLogIndex := rf.getLastIndex()
	curFirstLogIndex := rf.getFirstIndex()

	if args.PrevLogIndex < curFirstLogIndex { // the prevLog is in the snapshot of this peer.
		// fmt.Printf("S%d, PrevlogIndex %d is in the snapshot! %d\n", rf.me, args.PrevLogIndex, curFirstLogIndex)
		reply.Term = args.Term
		reply.UpNextIndex = curFirstLogIndex + 1
		reply.ConflictTerm = -1
		reply.Success = false
		// check prevIndex and prevTerm
	} else if args.PrevLogIndex > curLastLogIndex || rf.getTermForIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.Term = args.Term

		if args.PrevLogIndex > curLastLogIndex { // Case 3: nextIndex = log length
			reply.UpNextIndex = rf.getLastIndex() + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.getTermForIndex(args.PrevLogIndex)
			findIdx := args.PrevLogIndex
			// find the first index of conflictTerm in the logs
			for index := args.PrevLogIndex; index > rf.getFirstIndex(); index-- {
				if rf.getTermForIndex(index) != reply.ConflictTerm {
					findIdx = index + 1
					break
				}
			}
			reply.UpNextIndex = findIdx // Case 1: nextIndex = index of first entry with that term
		}
	} else {
		// append leader's log to follower's and apply command to channel.
		reply.Success = true
		reply.Term = args.Term

		// check whether match all logs between args and current raft server.
		last_match_idx := args.PrevLogIndex
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+1+i > curLastLogIndex {
				break
			}
			if rf.getTermForIndex(args.PrevLogIndex+1+i) != args.Entries[i].Term {
				break
			}
			last_match_idx = args.PrevLogIndex + 1 + i
		}

		if last_match_idx-args.PrevLogIndex != len(args.Entries) {
			// partially match
			rf.logs = rf.logs[:last_match_idx-rf.getFirstIndex()+1]
			rf.logs = append(rf.logs, args.Entries[last_match_idx-args.PrevLogIndex:]...)
			rf.persist()
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		}

		if rf.lastApplied < rf.commitIndex {
			for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
				rf.lastApplied++
				rf.commitQueue = append(rf.commitQueue, ApplyMsg{
					CommandValid:  true,
					SnapshotValid: false,
					CommandIndex:  rf.lastApplied,
					Command:       rf.getCommand(rf.lastApplied),
				})
			}
			rf.cv.Broadcast()
		}
	}

	// if args.Term larger than rf.currTerm or term equal but not a follower,
	// change itself to follower
	if args.Term > rf.currentTerm || rf.status != Follower {
		rf.ChangeToFollower(args.Term, -1)
	}
	// reset vote expire time
	rf.ResetElectionTimer()
}

// reset heartBeat, imme mean whether send immediately
//
func (rf *Raft) ResetAppendTimer(idx int, imme bool) {
	t := time.Now()
	if !imme {
		t = t.Add(HeartbeatSleep * time.Millisecond)
	}
	rf.AppendExpireTime[idx] = t
}

//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------

func (rf *Raft) leaderSendSnapShot(server int, args InstallSnapshotArgs) {

	reply := InstallSnapshotReply{}
	res := rf.sendSnapShot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// 检查是否为领导者，并检查raft的任期，如果不等于快照参数的任期，说明这是一个过期的回复
		if rf.status != Leader || rf.currentTerm != args.Term {
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.ChangeToFollower(reply.Term, -1)
			rf.ResetElectionTimer()
			return
		}

		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}
}

// InstallSnapShot RPC Handler
//
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	DPrintf("S%d Receive Snapshot From S%d T%d, LII: %d, LIT:%d, Snap:%v\n",
		rf.me, args.LeaderId, args.Term,
		args.LastIncludedIndex, args.LastIncludedTerm,
		args.Data)
	DPrintf("S%d Beform Process, Log is: %v\n", rf.me, rf.logs)

	curSnapLastIndex := rf.getFirstIndex()
	curLogLastIndex := rf.getLastIndex()

	if curSnapLastIndex >= args.LastIncludedIndex {
		// peer's snapshot contain more data, ignore this rpc
		reply.Term = args.Term
	} else {
		reply.Term = args.Term
		rf.commitQueue = append(rf.commitQueue, ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		})

		if args.LastIncludedIndex < curLogLastIndex {
			tempLog := rf.logs
			//       |----------------------|----------------------|
			//curSnapLastIndex    args.LastIncludedIndex   curLogLastIndex
			rf.logs = make([]LogEntry, 0)
			rf.logs = append(rf.logs, tempLog[args.LastIncludedIndex-curSnapLastIndex:]...)
			rf.logs[0].Command = nil

			// recommit lastIncludeIndex ~ commitIndex
			if args.LastIncludedIndex < rf.commitIndex {
				rf.lastApplied = args.LastIncludedIndex
				for rf.lastApplied < rf.commitIndex && rf.lastApplied < curLogLastIndex {
					rf.lastApplied++
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						SnapshotValid: false,
						CommandValid:  true,
						CommandIndex:  rf.lastApplied,
						Command:       rf.getCommand(rf.lastApplied),
					})
				}
			} else {
				rf.commitIndex = args.LastIncludedIndex
				rf.lastApplied = args.LastIncludedIndex
			}
		} else {
			// add a dummy entry
			rf.logs = make([]LogEntry, 0)
			rf.logs = append(rf.logs, LogEntry{
				Index:   args.LastIncludedIndex,
				Term:    args.LastIncludedTerm,
				Command: nil,
			})
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex
		}
		rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
		rf.cv.Broadcast()
	}

	if args.Term > rf.currentTerm || rf.status != Follower {
		rf.ChangeToFollower(reply.Term, -1)
	}

	rf.ResetElectionTimer()

	DPrintf("S%d After Process, Log is: %v\n", rf.me, rf.logs)
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
//
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d at T%d GetAPP IDX:%d, SnapShot: %v", rf.me, rf.currentTerm, index, snapshot)

	if rf.killed() {
		return
	}

	// 如果自身快照点大于index说明不需要安装
	if rf.getFirstIndex() >= index {
		DPrintf("S%d Application Set out of date snapshot!, Index: %d, FirstIndex: %d", rf.me, index, rf.getFirstIndex())
		return
	}

	// 更新快照日志
	firstLogIndex := rf.getFirstIndex()
	tempLog := rf.logs
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, tempLog[index-firstLogIndex:]...)
	rf.logs[0].Command = nil // delete the first dummy command

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
	DPrintf("S%d Persiste Snapshot Before Index: %d\n", rf.me, index)
	DPrintf("S%d at T%d After GetAPP, log is %v\n", rf.me, rf.currentTerm, rf.logs)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyChan.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//----------------------------------------------持久化（persist)部分---------------------------------------------------------
//
// save Raft's persistent status to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted status.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any status?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.status == Leader
	return term, isleader
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, -1, false
	}
	if rf.status != Leader {
		return -1, -1, false
	}

	// append this command to its log and return
	// HeartBeat ticker will sync this log to other peers
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Index: rf.getLastIndex() + 1, Command: command})
	rf.persist()
	// send appendEntry immediately
	for idx := 0; idx < len(rf.peers); idx++ {
		if idx == rf.me {
			continue
		}
		rf.ResetAppendTimer(idx, true)
	}
	return rf.getLastIndex(), rf.currentTerm, true
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
