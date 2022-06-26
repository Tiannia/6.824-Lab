package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	TIMEOUT = 100
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId     int
	Key       string
	Value     string
	ClientId  int64
	ServerSeq int64
	OpType    string
}

type OpResult struct {
	Error Err
	Value string
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int           // clientId -> seqId
	resMap    map[int64]OpResult      // seqId -> OpResult
	waitChMap map[int64]chan OpResult // ServerSeq -> chan(OpResult)
	kvPersist map[string]string       // key -> value

	lastIncludeIndex int // raft snapshot
}

func (kv *KVServer) SerilizeState() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	e.Encode(kv.resMap)
	data := w.Bytes()
	return data
}

func (kv *KVServer) DeSerilizeState(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int
	var resMap map[int64]OpResult

	if d.Decode(&kvPersist) != nil ||
		d.Decode(&seqMap) != nil ||
		d.Decode(&resMap) != nil {
		fmt.Printf("[Server(%v)] Failed to deserilize snapshot!", kv.me)
	} else {
		kv.mu.Lock()
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
		kv.resMap = resMap
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	lastSeq, ok := kv.seqMap[args.ClientId]
	if ok {
		if lastSeq == args.SeqId {
			reply.Err = kv.resMap[args.ClientId].Error
			reply.Value = kv.resMap[args.ClientId].Value
			kv.mu.Unlock()
			return
		} else if lastSeq > args.SeqId {
			// Return immediately if recieve an out of date request.
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		SeqId:     args.SeqId,
		ClientId:  args.ClientId,
		ServerSeq: nrand(),
	}

	if _, _, ok := kv.rf.Start(op); !ok {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(op.ServerSeq)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.ServerSeq)
		kv.mu.Unlock()
	}()

	timer := time.After(TIMEOUT * time.Millisecond)

	select {
	case res := <-ch:
		reply.Err = res.Error
		reply.Value = res.Value
		DPrintf("Get-> select getreply:%v", reply.Err)
	case <-timer:
		reply.Err = ErrTimeOut
		DPrintf("Get-> select getreply:%v", reply.Err)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	lastSeq, ok := kv.seqMap[args.ClientId]
	if ok {
		if lastSeq == args.SeqId {
			reply.Err = kv.resMap[args.ClientId].Error
			kv.mu.Unlock()
			return
		} else if lastSeq > args.SeqId {
			// Return immediately if recieve an out of date request.
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		SeqId:     args.SeqId,
		ClientId:  args.ClientId,
		ServerSeq: nrand(),
	}
	if _, _, ok := kv.rf.Start(op); !ok {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getWaitCh(op.ServerSeq)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.ServerSeq)
		kv.mu.Unlock()
	}()

	timer := time.After(TIMEOUT * time.Millisecond)

	select {
	case res := <-ch:
		reply.Err = res.Error
		DPrintf("PutAppend-> select getreply:%v", reply.Err)
	case <-timer:
		reply.Err = ErrTimeOut
		DPrintf("PutAppend-> select getreply:%v", reply.Err)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.resMap = make(map[int64]OpResult)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int64]chan OpResult)
	kv.persister = persister

	snapshot := persister.ReadSnapshot()
	kv.DeSerilizeState(snapshot)

	go kv.applyMsgHandlerLoop()

	return kv
}

func (kv *KVServer) applyMsgHandlerLoop() {
	for command := range kv.applyCh {
		if command.CommandValid {

			// commandIndex is lower than lastIncludeIndex (in the snapshot)
			if command.CommandIndex <= kv.lastIncludeIndex {
				return
			}

			op := command.Command.(Op)

			isduplicated, res := kv.isDuplicated(op.ClientId, op.SeqId)
			if isduplicated {
				kv.getWaitCh(op.ServerSeq) <- res
				continue
			}
			switch op.OpType {
			case "Get":
				val, ok := kv.kvPersist[op.Key]
				if ok {
					res.Value = val
					res.Error = OK
				} else {
					res.Error = ErrNoKey
				}
			case "Put":
				kv.kvPersist[op.Key] = op.Value
				res.Error = OK
			case "Append":
				oriVal, ok := kv.kvPersist[op.Key]
				if ok {
					kv.kvPersist[op.Key] = oriVal + op.Value
				} else {
					kv.kvPersist[op.Key] = op.Value
				}
				res.Error = OK
			}
			DPrintf("applyMsgHandlerLoop -> op.OpType:%v", op.OpType)

			kv.mu.Lock()
			kv.seqMap[op.ClientId] = op.SeqId
			kv.resMap[op.ClientId] = res
			kv.mu.Unlock()

			// if need snapshot and raftstatesize() larger than maxraftstate
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				snapshot := kv.SerilizeState()
				kv.rf.Snapshot(command.CommandIndex, snapshot)
			}

			kv.getWaitCh(op.ServerSeq) <- res
		}

		if command.SnapshotValid {
			// A service wants to switch to snapshot. symbolically call this api to check if there is a conflict.
			if kv.rf.CondInstallSnapshot(command.SnapshotTerm, command.SnapshotIndex, command.Snapshot) {
				kv.DeSerilizeState(command.Snapshot)
				kv.lastIncludeIndex = command.SnapshotIndex
			}
		}
	}
}

func (kv *KVServer) isDuplicated(clientId int64, seqId int) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if exist {
		if seqId <= lastSeqId {
			return true, kv.resMap[clientId]
		}
	}
	return false, OpResult{}
}

func (kv *KVServer) getWaitCh(serverSeq int64) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, exist := kv.waitChMap[serverSeq]
	if !exist {
		kv.waitChMap[serverSeq] = make(chan OpResult, 1)
		ch = kv.waitChMap[serverSeq]
	}
	return ch
}
