package shardkv

import (
	"bytes"
	"fmt"

	"6.824/labgob"
	"6.824/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	ClientSeq uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	ClientSeq uint64
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	ShardId   int
	ConfigNum int
}

type GetShardReply struct {
	Shard Shard
	Error Err
}

type GCArgs struct {
	CfgNum  int
	ShardId int
}

type GCReply struct {
	Error Err
}

type Status string

const (
	WORKING Status = "woring"
	ACQUING Status = "acquring"
	EXPIRED Status = "expired"
	INVALID Status = "invalid"
)

const (
	GET           = "Get"
	PUT           = "Put"
	APPEND        = "Append"
	PUTAPP        = "PUTAPPEND"
	CHANGE_CONFIG = "CHANGE_CONFIG"
	GET_NEW_SHARD = "GET_NEW_SHARD"
	CHANGE_SHARD  = "CHANGE_SHARD"
	GC            = "GC"
)

type Shard struct {
	Data               map[string]string
	Client_to_last_req map[int64]uint64
	Client_to_last_res map[int64]OpResult
	Status             Status
}

func (sd *Shard) Copy() Shard {
	res := Shard{
		Data:               make(map[string]string),
		Client_to_last_req: make(map[int64]uint64),
		Client_to_last_res: make(map[int64]OpResult),
	}
	for k, v := range sd.Data {
		res.Data[k] = v
	}
	for k, v := range sd.Client_to_last_req {
		res.Client_to_last_req[k] = v
	}
	for k, v := range sd.Client_to_last_res {
		res.Client_to_last_res[k] = v
	}
	res.Status = sd.Status
	return res
}

func (sd *Shard) Clear() {
	sd.Data = nil
	sd.Client_to_last_req = nil
	sd.Client_to_last_res = nil
	sd.Status = INVALID
}

type CfgiData [shardctrler.NShards]Shard // store the shard for on config num

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string

	Key   string
	Value string

	NewConfig shardctrler.Config

	NewShardCfgNum int
	NewShardId     int
	NewShard       Shard

	GCCfgNum  int
	GCShardID int

	ClientId  int64
	ClientSeq uint64
	ServerSeq int64
}

type OpResult struct {
	Error Err
	Value string
}

func (kv *ShardKV) SerilizeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	begin_idx := 0
	kv.mu.Lock()
	for i := 0; i < kv.cur_config.Num-1; i++ {
		flag := true
		begin_idx = i
		for j := 0; j < shardctrler.NShards; j++ {
			if kv.allData[i][j].Status != INVALID {
				flag = false
				break
			}
		}
		if !flag {
			break
		}
	}
	e.Encode(kv.allData[begin_idx:])
	e.Encode(begin_idx)
	e.Encode(kv.last_config)
	e.Encode(kv.cur_config)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *ShardKV) DeSerilizeState(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data []CfgiData
	var begin_idx int
	var last_config shardctrler.Config
	var cur_config shardctrler.Config
	if d.Decode(&data) != nil ||
		d.Decode(&begin_idx) != nil ||
		d.Decode(&last_config) != nil ||
		d.Decode(&cur_config) != nil {
		fmt.Printf("S%d ShardKV Read Persist Error!\n", kv.me)
	} else {
		kv.mu.Lock()
		kv.allData = make([]CfgiData, begin_idx)
		for i := 0; i < begin_idx; i++ {
			for j := 0; j < shardctrler.NShards; j++ {
				kv.allData[i][j].Status = INVALID
			}
		}
		kv.allData = append(kv.allData, data...)
		kv.last_config = last_config
		kv.cur_config = cur_config
		kv.mu.Unlock()
	}
}
