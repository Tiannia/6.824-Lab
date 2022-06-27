package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	QUERY   = "QUERY"
	JOIN    = "JOIN"
	LEAVE   = "LEAVE"
	MOVE    = "MOVE"
	TIMEOUT = 100 // set time out to 100 millsecond.
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs   []Config                // indexed by config num
	seqMap    map[int64]int           // clientId -> seqId
	resMap    map[int64]OpResult      // seqId -> OpResult
	waitChMap map[int64]chan OpResult // ServerSeq -> chan(OpResult)
}

type Op struct {
	// Your data here.
	GIDs      []int
	Shard     int
	Num       int
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64
	SeqId     int
	ServerSeq int64
	OpType    string
}

type OpResult struct {
	Config Config
	Error  Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	lastSeq, ok := sc.seqMap[args.ClientId]
	if ok {
		if lastSeq == args.SeqId {
			reply.Err = sc.resMap[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if lastSeq > args.SeqId {
			// Return immediately if recieve an out of date request.
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    JOIN,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		ServerSeq: nrand(),
	}

	if _, _, ok := sc.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}

	ch := sc.getWaitCh(op.ServerSeq)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.ServerSeq)
		sc.mu.Unlock()
	}()

	timer := time.After(TIMEOUT * time.Millisecond)

	select {
	case res := <-ch:
		reply.Err = res.Error
		reply.WrongLeader = false
	case <-timer:
		reply.Err = "TIMEOUT"
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	lastSeq, ok := sc.seqMap[args.ClientId]
	if ok {
		if lastSeq == args.SeqId {
			reply.Err = sc.resMap[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if lastSeq > args.SeqId {
			// Return immediately if recieve an out of date request.
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    LEAVE,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		ServerSeq: nrand(),
	}

	if _, _, ok := sc.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}

	ch := sc.getWaitCh(op.ServerSeq)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.ServerSeq)
		sc.mu.Unlock()
	}()

	timer := time.After(TIMEOUT * time.Millisecond)

	select {
	case res := <-ch:
		reply.Err = res.Error
		reply.WrongLeader = false
	case <-timer:
		reply.Err = "TIMEOUT"
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	lastSeq, ok := sc.seqMap[args.ClientId]
	if ok {
		if lastSeq == args.SeqId {
			reply.Err = sc.resMap[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if lastSeq > args.SeqId {
			// Return immediately if recieve an out of date request.
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    MOVE,
		Shard:     args.Shard,
		GIDs:      make([]int, 1),
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		ServerSeq: nrand(),
	}
	op.GIDs[0] = args.GID

	if _, _, ok := sc.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}

	ch := sc.getWaitCh(op.ServerSeq)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.ServerSeq)
		sc.mu.Unlock()
	}()

	timer := time.After(TIMEOUT * time.Millisecond)

	select {
	case res := <-ch:
		reply.Err = res.Error
		reply.WrongLeader = false
	case <-timer:
		reply.Err = "TIMEOUT"
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	lastSeq, ok := sc.seqMap[args.ClientId]
	if ok {
		if lastSeq == args.SeqId {
			reply.Err = sc.resMap[args.ClientId].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if lastSeq > args.SeqId {
			// Return immediately if recieve an out of date request.
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		OpType:    QUERY,
		Num:       args.Num,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		ServerSeq: nrand(),
	}

	if _, _, ok := sc.rf.Start(op); !ok {
		reply.WrongLeader = true
		return
	}

	ch := sc.getWaitCh(op.ServerSeq)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, op.ServerSeq)
		sc.mu.Unlock()
	}()

	timer := time.After(TIMEOUT * time.Millisecond)

	select {
	case res := <-ch:
		reply.Err = res.Error
		reply.WrongLeader = false
		reply.Config = res.Config
	case <-timer:
		reply.Err = "TIMEOUT"
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitChMap = make(map[int64]chan OpResult)
	sc.seqMap = make(map[int64]int)
	sc.resMap = make(map[int64]OpResult)

	go sc.applyMsgHandlerLoop()
	return sc
}

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for command := range sc.applyCh {
		if command.CommandValid {

			op := command.Command.(Op)

			isduplicated, res := sc.isDuplicated(op.ClientId, op.SeqId)
			if isduplicated {
				sc.getWaitCh(op.ServerSeq) <- res
				continue
			}
			switch op.OpType {
			case QUERY:
				idx := op.Num
				if op.Num == -1 || op.Num >= len(sc.configs) {
					idx = len(sc.configs) - 1
				}
				res.Config = sc.configs[idx]
				res.Error = OK
			case MOVE:
				newConfig := CopyConfig(&sc.configs[len(sc.configs)-1])
				newConfig.Shards[op.Shard] = op.GIDs[0]
				sc.configs = append(sc.configs, newConfig)
				res.Error = OK
			case LEAVE:
				newConfig := CopyConfig(&sc.configs[len(sc.configs)-1])
				for _, GID := range op.GIDs {
					delete(newConfig.Groups, GID)
				}
				newConfig.ReAllocateGroups()
				sc.configs = append(sc.configs, newConfig)
				res.Error = OK
			case JOIN:
				newConfig := CopyConfig(&sc.configs[len(sc.configs)-1])
				for k, v := range op.Servers {
					newConfig.Groups[k] = v
				}
				newConfig.ReAllocateGroups()
				sc.configs = append(sc.configs, newConfig)
				res.Error = OK
			}

			sc.mu.Lock()
			sc.seqMap[op.ClientId] = op.SeqId
			sc.resMap[op.ClientId] = res
			sc.mu.Unlock()

			sc.getWaitCh(op.ServerSeq) <- res
		}
	}
}

func (sc *ShardCtrler) getWaitCh(serverSeq int64) chan OpResult {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	ch, exist := sc.waitChMap[serverSeq]
	if !exist {
		sc.waitChMap[serverSeq] = make(chan OpResult, 1)
		ch = sc.waitChMap[serverSeq]
	}
	return ch
}

func (sc *ShardCtrler) isDuplicated(clientId int64, seqId int) (bool, OpResult) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, exist := sc.seqMap[clientId]
	if exist {
		if seqId <= lastSeqId {
			return true, sc.resMap[clientId]
		}
	}
	return false, OpResult{}
}
