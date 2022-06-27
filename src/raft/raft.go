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

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 200
	MinVoteTime  = 150

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	// AppendTimerResolution 每隔一个此时间段就会尝试发送AppendEntries
	HeartbeatSleep        = 120
	AppendTimerResolution = 5
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
	status           Status
	voteNum          int
	votedTimer       time.Time
	AppendExpireTime []time.Time
	commitQueue      []ApplyMsg

	// 2D中用于传入快照点
	lastIncludeIndex int
	lastIncludeTerm  int
}

type LogEntry struct {
	Term    int
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
	Term         int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success      bool // 如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	UpNextIndex  int  // 如果发生conflict时reply传过来的正确的下标用于更新nextIndex[i]
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh
	rf.commitQueue = make([]ApplyMsg, 0)
	rf.cv = sync.NewCond(&rf.mu)
	rf.AppendExpireTime = make([]time.Time, len(peers))

	rf.votedTimer = time.Now()
	for i := 0; i < len(peers); i++ {
		rf.ResetAppendTimer(i, false)
	}

	// initialize from status persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex
	}

	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

// --------------------------------------------------------ticker部分----------------------------------------------------

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		// 时间过期发起选举
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)
		// 此处的流程为每次每次votedTimer如果小于在sleep睡眠之前定义的时间，就代表没有votedTimer被更新为最新的时间，则发起选举
		rf.mu.Lock()
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			// 转变状态为候选者
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.currentTerm += 1
			rf.persist()

			rf.sendElection()
			rf.votedTimer = time.Now()
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
			// // For Debug:
			// if message.CommandValid {
			// 	DPrintf("S%d[%v] Apply Command. index:%d, cmd:%v\n", rf.me, rf.status, message.CommandIndex, message.Command)
			// } else {
			// 	DPrintf("S%d[%v] Apply Snapshot. index:%d, term:%d, snapshot:%v\n", rf.me, rf.status, message.SnapshotIndex, message.SnapshotTerm, message.Snapshot)
			// }
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

			// DPrintf("[TIKER-SendElection-Rf(%v)-To(%v)] args:%+v, curStatus%v\n", rf.me, server, args, rf.status)
			reply := RequestVoteReply{}
			res := rf.sendRequestVote(server, &args, &reply)

			if res {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term < rf.currentTerm {
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()

					rf.votedTimer = time.Now()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted && rf.currentTerm == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {

						DPrintf("[elect]-> Rf[%v] to be leader, term is : %v\n", rf.me, rf.currentTerm)
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
						}

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()

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

	// 由于网络分区或者是节点crash，导致的任期比接收者还小，直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 预期的结果:任期大于当前节点，进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	// If votedFor is null or candidateId, and candidate’s logs is at
	// least as up-to-date as receiver’s logs, grant vote
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || // 第一个条件：votedFor is null，需要判断日志是否conflict
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { // 第二个条件：votedFor is candidateID
		// 满足以上两个其中一个都返回false，不给予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}

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
		if rf.nextIndex[index]-1 < rf.lastIncludeIndex {
			go rf.leaderSendSnapShot(index)

		} else {
			// 开启协程并发的进行日志增量
			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(index)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}

			if rf.getLastIndex() >= rf.nextIndex[index] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[index]-rf.lastIncludeIndex:]...)
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

					if rf.status != Leader {
						return
					}

					// 返回的任期更高说明自己可能是处于一个过时的分区，直接恢复为Follower并返回
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.votedFor = -1
						rf.voteNum = 0
						rf.votedTimer = time.Now()
						rf.persist()
						return
					}

					// filter out of date rpc reply
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
						if ok_idx > rf.commitIndex && rf.restoreLogTerm(ok_idx) == rf.currentTerm {
							rf.commitIndex = ok_idx
							for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
								rf.lastApplied++
								rf.commitQueue = append(rf.commitQueue, ApplyMsg{
									CommandValid: true,
									CommandIndex: rf.lastApplied,
									Command:      rf.restoreLog(rf.lastApplied).Command,
								})
							}
							rf.cv.Broadcast()
						}

					} else {
						if reply.ConflictTerm == -1 {
							rf.nextIndex[server] = reply.UpNextIndex
						} else { // Case 2: nextIndex = leader's last entry for XTerm
							findIdx := -1
							for index := rf.getLastIndex(); index >= rf.lastIncludeIndex; index-- {
								if rf.restoreLogTerm(index) == reply.ConflictTerm {
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
	defer DPrintf("[AppendEntries--Return-Rf(%v)] arg:%+v, reply:%+v\n", rf.me, args, reply)

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if args.Term >= curTerm, check and sync the log.
	if args.PrevLogIndex < rf.lastIncludeIndex {
		// the prevLog is in the snapshot of this peer.
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	} else if args.PrevLogIndex > rf.getLastIndex() || rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.Term = args.Term

		if args.PrevLogIndex > rf.getLastIndex() { // Case 3: nextIndex = log length
			reply.UpNextIndex = rf.getLastIndex()
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.restoreLogTerm(args.PrevLogIndex)
			findIdx := args.PrevLogIndex
			// find the index of the log of conflictTerm
			for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- {
				if rf.restoreLogTerm(index) != reply.ConflictTerm {
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

		rf.logs = append(rf.logs[:args.PrevLogIndex+1-rf.lastIncludeIndex], args.Entries...)
		rf.persist()

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
		}

		if rf.lastApplied < rf.commitIndex {
			for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
				rf.lastApplied++
				rf.commitQueue = append(rf.commitQueue, ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.lastApplied,
					Command:      rf.restoreLog(rf.lastApplied).Command,
				})
			}
			rf.cv.Broadcast()
		}
	}

	// if args.Term larger than rf.currTerm or term equal but not a follower,
	// change itself to follower
	if args.Term > rf.currentTerm || rf.status != Follower {
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.currentTerm = args.Term
		rf.persist()
	}
	// reset vote expire time
	rf.votedTimer = time.Now()
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

func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.status != Leader || rf.currentTerm != args.Term {
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.status = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.votedTimer = time.Now()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1
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

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		reply.Term = args.Term
	} else {
		reply.Term = args.Term
		index := args.LastIncludeIndex
		tempLog := make([]LogEntry, 0)
		tempLog = append(tempLog, LogEntry{})

		for i := index + 1; i <= rf.getLastIndex(); i++ {
			tempLog = append(tempLog, rf.restoreLog(i))
		}

		rf.lastIncludeTerm = args.LastIncludeTerm
		rf.lastIncludeIndex = args.LastIncludeIndex

		rf.logs = tempLog
		if index > rf.commitIndex {
			rf.commitIndex = index
		} else {
			// recommit lastIncludeIndex ~ commitIndex
			// I dont know why it is serviceable for lab 3B
			if index < rf.commitIndex {
				rf.lastApplied = index
				for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
					rf.lastApplied++
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.lastApplied,
						Command:      rf.restoreLog(rf.lastApplied).Command,
					})
				}
			}
		}

		// I think lastApplied will lower than commitIndex(contrary to the paper)
		// So if a failure occurs before the transfer is completed(lastApplied catch commitIndex)
		// We shall check it alone.
		if index > rf.lastApplied {
			rf.lastApplied = index
		}

		rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

		rf.commitQueue = append(rf.commitQueue, ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  rf.lastIncludeTerm,
			SnapshotIndex: rf.lastIncludeIndex,
		})
		rf.cv.Broadcast()
	}

	if args.Term > rf.currentTerm || rf.status != Follower {
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.votedTimer = time.Now()
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

	if rf.killed() {
		return
	}
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}
	// 更新快照日志
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	DPrintf("[Snapshot-Rf(%v)]rf.commitIndex:%v, index:%v\n", rf.me, rf.commitIndex, index)

	// 更新快照下标/任期
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.restoreLogTerm(index)
	rf.logs = sLogs

	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
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
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
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
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
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

	// this is a live leader,
	// append this command to its log and return
	// the HBT timer will sync this log to other peers
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
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
