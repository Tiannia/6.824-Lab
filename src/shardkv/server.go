package shardkv

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	TIMEOUT = 100
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck *shardctrler.Clerk
	// store all the history data, config_nuim -> shardId -> shard
	allData     []CfgiData
	chans       map[int64]chan OpResult // store the chan to reply client
	persister   *raft.Persister
	last_config shardctrler.Config // store the last config
	cur_config  shardctrler.Config // store the cur config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// the sharding status is working
	if last_process_seq, ok := kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[args.ClientId]; ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[args.ClientId].Error
			reply.Value = kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[args.ClientId].Value
			kv.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		Type:      GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.chans[op.ServerSeq] = rec_chan
	kv.mu.Unlock()
	if _, _, ok1 := kv.rf.Start(op); ok1 {
		select {
		case <-time.After(TIMEOUT * time.Millisecond):
			// timeout!
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
			reply.Value = res.Value
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.chans, op.ServerSeq)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	// the sharding status is working
	if last_process_seq, ok := kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[args.ClientId]; ok {
		if last_process_seq == args.ClientSeq {
			reply.Err = kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[args.ClientId].Error
			kv.mu.Unlock()
			return
		} else if last_process_seq > args.ClientSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.chans[op.ServerSeq] = rec_chan
	kv.mu.Unlock()
	if _, _, ok1 := kv.rf.Start(op); ok1 {
		select {
		case <-time.After(TIMEOUT * time.Millisecond):
			// timeout!
			reply.Err = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Err = res.Error
		}
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.chans, op.ServerSeq)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Shard{})
	labgob.Register(CfgiData{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.last_config = shardctrler.Config{}
	kv.cur_config = shardctrler.Config{}
	kv.allData = append(kv.allData, CfgiData{})
	for i := 0; i < shardctrler.NShards; i++ {
		kv.allData[kv.cur_config.Num][i].Status = INVALID
	}
	kv.chans = make(map[int64]chan OpResult)
	kv.DeSerilizeState(kv.persister.ReadSnapshot())

	// 定期从Master拉取配置
	go kv.PullConfig()
	// 处理从raft发送过来的log
	go kv.process()
	// 定期检查是否需要从其他Server拉取Shard
	go kv.CheckAcqShard()

	return kv
}

// pull new config from master periodically
func (kv *ShardKV) PullConfig() {
	for {

		time.Sleep(80 * time.Millisecond)
		// check all shard is working or expired
		kv.mu.Lock()
		if !kv.CheckStatus() {
			kv.mu.Unlock()
			continue
		}
		cur_config_num := kv.cur_config.Num + 1
		kv.mu.Unlock()
		new_config := kv.mck.Query(cur_config_num)
		kv.mu.Lock()
		has_new_config := new_config.Num > kv.cur_config.Num
		kv.mu.Unlock()
		if has_new_config {
			op := Op{
				Type:      CHANGE_CONFIG,
				NewConfig: new_config,
			}
			kv.rf.Start(op)
		}
	}
}

// check whethe change from last_config to cur_config is all working
func (kv *ShardKV) CheckStatus() bool {
	for _, v := range kv.allData[kv.cur_config.Num] {
		if v.Status == ACQUING {
			return false
		}
	}
	return true
}

func (kv *ShardKV) ResetStatus() {
	kv.allData = append(kv.allData, CfgiData{})
	for i := 0; i < len(kv.cur_config.Shards); i++ {
		// 原来不在本组，现在在本组
		if kv.last_config.Shards[i] != kv.gid && kv.cur_config.Shards[i] == kv.gid {
			kv.allData[kv.cur_config.Num][i] = Shard{
				Data:               make(map[string]string),
				Client_to_last_req: make(map[int64]uint64),
				Client_to_last_res: make(map[int64]OpResult),
				Status:             WORKING, // the data is not in other group, the shard wasn't belong to any group before
			}
			if kv.last_config.Shards[i] != 0 {
				// the data is in other group
				kv.allData[kv.cur_config.Num][i].Status = ACQUING
			}
		} else if kv.last_config.Shards[i] == kv.gid && kv.cur_config.Shards[i] != kv.gid { // 原来在本组，现在不在
			kv.allData[kv.last_config.Num][i].Status = EXPIRED
			kv.allData[kv.cur_config.Num][i].Status = INVALID
		} else if kv.last_config.Shards[i] == kv.gid && kv.cur_config.Shards[i] == kv.gid { // 一直都在本组
			kv.allData[kv.cur_config.Num][i] = kv.allData[kv.last_config.Num][i].Copy()
			kv.allData[kv.last_config.Num][i].Clear()
		} else { // 一直不在本组
			kv.allData[kv.cur_config.Num][i].Status = INVALID
		}
	}
}

// 周期性根据当前状态检查是否需要从其他group拉取shard
func (kv *ShardKV) CheckAcqShard() {

	for {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		for i, sd := range kv.allData[kv.cur_config.Num] {
			if sd.Status == ACQUING {
				// send rpc to get the shard
				go kv.SendAcqShard(i, kv.last_config.Groups[kv.last_config.Shards[i]], kv.last_config.Num)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) SendAcqShard(shardId int, servers []string, cfgNum int) {
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			args := GetShardArgs{
				ShardId:   shardId,
				ConfigNum: cfgNum,
			}
			reply := GetShardReply{}
			ok := srv.Call("ShardKV.AcquireShard", &args, &reply)
			if ok && reply.Error == OK {
				// chek the state first
				kv.mu.Lock()
				// need the configNum match and the status is acquiring
				need_add := cfgNum == kv.last_config.Num && kv.allData[kv.cur_config.Num][shardId].Status == ACQUING
				kv.mu.Unlock()

				if need_add {
					op := Op{
						Type:           CHANGE_SHARD,
						NewShardCfgNum: cfgNum + 1,
						NewShardId:     shardId,
						NewShard:       reply.Shard,
					}
					kv.rf.Start(op)
				}
				return
			} else {
				continue
			}
		}
	}
}

func (kv *ShardKV) AcquireShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check parameters
	if kv.cur_config.Num <= args.ConfigNum {
		reply.Error = ErrWrongLeader
		return
	}
	// need to check, because the data may be gced
	// when it is been gced, means this data has been received by other servers.
	// so this is a out ot date gc, reply ok is fine.
	if kv.allData[args.ConfigNum][args.ShardId].Status == EXPIRED {
		reply.Shard = kv.allData[args.ConfigNum][args.ShardId].Copy()
	}
	reply.Error = OK
}

func (kv *ShardKV) process_get(op Op) {
	shardId := key2shard(op.Key)
	// need to check the machine state
	res := OpResult{}
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		kv.mu.Unlock()
		res.Error = ErrWrongGroup
		kv.replyChan(op.ServerSeq, res)
		return
	} else {
		if val, ok2 := kv.allData[kv.cur_config.Num][shardId].Data[op.Key]; ok2 {
			res.Value = val
			res.Error = OK
		} else {
			res.Error = ErrNoKey
		}
	}
	kv.mu.Unlock()
	kv.update(op, res)
	kv.replyChan(op.ServerSeq, res)
}

func (kv *ShardKV) process_put(op Op) {
	shardId := key2shard(op.Key)
	res := OpResult{}
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		kv.mu.Unlock()
		res.Error = ErrWrongGroup
		kv.replyChan(op.ServerSeq, res)
		return
	} else {
		kv.allData[kv.cur_config.Num][shardId].Data[op.Key] = op.Value
		res.Error = OK
	}
	kv.mu.Unlock()
	kv.update(op, res)
	kv.replyChan(op.ServerSeq, res)
}

func (kv *ShardKV) process_append(op Op) {
	shardId := key2shard(op.Key)
	res := OpResult{}
	kv.mu.Lock()
	if kv.allData[kv.cur_config.Num][shardId].Status != WORKING {
		kv.mu.Unlock()
		res.Error = ErrWrongGroup
		kv.replyChan(op.ServerSeq, res)
		return
	} else {
		if val, ok2 := kv.allData[kv.cur_config.Num][shardId].Data[op.Key]; ok2 {
			kv.allData[kv.cur_config.Num][shardId].Data[op.Key] = val + op.Value
		} else {
			kv.allData[kv.cur_config.Num][shardId].Data[op.Key] = op.Value
		}
		res.Error = OK
	}
	kv.mu.Unlock()
	kv.update(op, res)
	kv.replyChan(op.ServerSeq, res)
}

func (kv *ShardKV) process_change_config(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 因为有可能有重复的命令被加入到raft的log中。
	// 因为对状态的修改需要经过如下步骤：
	// 1. 将命令加入到raft的队列中。
	// 2. log同步之后，从chan中拿到命令，并执行对应的状态修改操作。
	// 所以加入一个命令到这个命令真正其效果中间是有延迟的，且有可能会发生易主等多种情况
	// 所以一个命令从log中拿出来的环境和它被加入的环境不一定相同，所以这里需要进行重新判断
	if kv.cur_config.Num+1 == op.NewConfig.Num {
		// 这里只需要判断Num的关系，这是因为当这个命令被加入到队列的时候，就说明cur_config的所有shard的状态都被OK了
		kv.last_config = kv.cur_config
		kv.cur_config = op.NewConfig
		kv.ResetStatus()
	}
}

func (kv *ShardKV) process_change_shard(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// check the configNum match and the Shard has not be set
	if kv.cur_config.Num != op.NewShardCfgNum || kv.allData[kv.cur_config.Num][op.NewShardId].Status != ACQUING {
		return
	}
	// set the shard and change the status]
	kv.allData[kv.cur_config.Num][op.NewShardId] = op.NewShard.Copy()
	kv.allData[kv.cur_config.Num][op.NewShardId].Status = WORKING
	// send gc
	go kv.sendGc(kv.last_config.Groups[kv.last_config.Shards[op.NewShardId]], kv.last_config.Num, op.NewShardId)
}

func (kv *ShardKV) process_gc(op Op) {
	res := OpResult{}
	// check config num
	kv.mu.Lock()
	if kv.cur_config.Num <= op.GCCfgNum {
		kv.mu.Unlock()
		res.Error = ErrWrongLeader
		kv.replyChan(op.ServerSeq, res)
		return
	}
	// check whether has been gced
	if kv.allData[op.GCCfgNum][op.GCShardID].Status == INVALID {
		res.Error = OK
	} else if kv.allData[op.GCCfgNum][op.GCShardID].Status == EXPIRED {
		kv.allData[op.GCCfgNum][op.GCShardID].Clear()
		res.Error = OK
	} else {
		res.Error = ErrWrongLeader
	}
	kv.mu.Unlock()
	kv.replyChan(op.ServerSeq, res)
}

func (kv *ShardKV) process() {
	for command := range kv.applyCh {
		if command.CommandValid {
			op := command.Command.(Op)
			if op.Type == GET || op.Type == PUT || op.Type == APPEND {
				need_process, res := kv.check_dup(op)
				if !need_process {
					kv.replyChan(op.ServerSeq, res)
					continue
				}
			}
			switch op.Type {
			case GET:
				kv.process_get(op)
			case PUT:
				kv.process_put(op)
			case APPEND:
				kv.process_append(op)
			case CHANGE_CONFIG:
				kv.process_change_config(op)
			case CHANGE_SHARD:
				kv.process_change_shard(op)
			case GC:
				kv.process_gc(op)
			}
			// check whether need to snapshot
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.SerilizeState()
				kv.rf.Snapshot(command.CommandIndex, snapshot)
			}
		} else if command.SnapshotValid {
			// update self state
			if kv.rf.CondInstallSnapshot(command.SnapshotTerm, command.SnapshotIndex, command.Snapshot) {
				kv.DeSerilizeState(command.Snapshot)
			}
		}
	}
}

// check whether need to process
func (kv *ShardKV) check_dup(op Op) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	if client_last_process_seq, ok := kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[op.ClientId]; ok {
		if op.ClientSeq <= client_last_process_seq {
			return false, kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[op.ClientId]
		}
	}
	return true, OpResult{}
}

func (kv *ShardKV) update(op Op, res OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(op.Key)
	kv.allData[kv.cur_config.Num][shardId].Client_to_last_req[op.ClientId] = op.ClientSeq
	kv.allData[kv.cur_config.Num][shardId].Client_to_last_res[op.ClientId] = res
}

func (kv *ShardKV) replyChan(server_seq int64, res OpResult) {
	kv.mu.Lock()
	send_chan, ok := kv.chans[server_seq]
	kv.mu.Unlock()
	if ok {
		send_chan <- res
	}
}

func (kv *ShardKV) sendGc(servers []string, cfg_num, shardId int) {

	args := GCArgs{
		CfgNum:  cfg_num,
		ShardId: shardId,
	}
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			reply := GCReply{}
			ok := srv.Call("ShardKV.GC", &args, &reply)
			if ok && reply.Error == OK {
				return
			} else {
				continue
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) GC(args *GCArgs, reply *GCReply) {
	kv.mu.Lock()
	if kv.cur_config.Num <= args.CfgNum {
		reply.Error = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.allData[args.CfgNum][args.ShardId].Status != EXPIRED {
		// 已经被GC了，直接返回OK
		reply.Error = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 加入到raft层进行GC
	op := Op{
		Type:      GC,
		GCCfgNum:  args.CfgNum,
		GCShardID: args.ShardId,
		ServerSeq: nrand(),
	}
	rec_chan := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.chans[op.ServerSeq] = rec_chan
	kv.mu.Unlock()
	if _, _, ok1 := kv.rf.Start(op); ok1 {
		select {
		case <-time.After(TIMEOUT * time.Millisecond):
			// timeout!
			reply.Error = "TIMEOUT"
		case res := <-rec_chan:
			// this op has be processed!
			reply.Error = res.Error
		}
	} else {
		reply.Error = ErrWrongLeader
	}
	kv.mu.Lock()
	delete(kv.chans, op.ServerSeq)
	kv.mu.Unlock()
}
