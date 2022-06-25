package kvraft

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	for {
		ch := make(chan GetReply, 1)
		go func(ch chan GetReply, serverId int, clientId int64, seqId int) {
			args := GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId}
			reply := GetReply{}
			ck.servers[serverId].Call("KVServer.Get", &args, &reply)
			ch <- reply
		}(ch, serverId, ck.clientId, ck.seqId)
		timer := time.After(100 * time.Millisecond)
		select {
		case reply := <-ch:
			if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			}
			serverId = (serverId + 1) % len(ck.servers)
		case <-timer:
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	for {
		ch := make(chan PutAppendReply, 1)
		go func(ch chan PutAppendReply, serverId int, clientId int64, seqId int) {
			args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqId: ck.seqId}
			reply := PutAppendReply{}
			ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
			ch <- reply
		}(ch, serverId, ck.clientId, ck.seqId)
		timer := time.After(100 * time.Millisecond)
		select {
		case reply := <-ch:
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else {
				serverId = (serverId + 1) % len(ck.servers)
			}
		case <-timer:
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
