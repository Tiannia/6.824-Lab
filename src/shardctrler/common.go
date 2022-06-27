package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	SeqId    int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	SeqId    int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	SeqId    int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	ClientId int64
	SeqId    int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func CopyConfig(cfg *Config) Config {
	res := Config{}
	res.Num = cfg.Num + 1
	copy(res.Shards[0:], cfg.Shards[0:])
	res.Groups = make(map[int][]string)
	for k, v := range cfg.Groups {
		res.Groups[k] = v
	}
	return res
}

func (cfg *Config) ReAllocateGroups() {
	if len(cfg.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	// LEAVE
	for i := 0; i < NShards; i++ {
		if _, ok := cfg.Groups[cfg.Shards[i]]; !ok {
			cfg.Shards[i] = 0
		}
	}

	groupCount := make(map[int]int)
	for group := range cfg.Groups {
		groupCount[group] = 0
	}
	for _, GID := range cfg.Shards {
		// The very first configuration should be numbered zero.
		// It should contain no groups, and all shards should be assigned to GID zero (an invalid GID).
		if GID != 0 {
			groupCount[GID]++
		}
	}

	averageCount := NShards / len(cfg.Groups)
	remain := NShards - averageCount*len(cfg.Groups)
	curRemain := 0
	lessThanAvg := make([]int, 0)
	equalToAvg := make([]int, 0)
	for k, v := range groupCount {
		if v < averageCount {
			lessThanAvg = append(lessThanAvg, k)
		} else if v == averageCount+1 {
			curRemain++
		} else if v == averageCount {
			equalToAvg = append(equalToAvg, k)
		}
	}

	// In Go, map iteration order is not deterministic.
	sort.Ints(lessThanAvg)
	sort.Ints(equalToAvg)

	// Move Shards
	// Traverse shard to see if the group it belongs to exceeds the avg value.
	for i, g := range cfg.Shards {
		// 1. GID = 0, need to alloc.
		// 2. GID != 0, groupCount larger than avg+1, need to move out
		// 3. GID != 0, groupCount = avg+1 and (curRemain > remain)
		// curRemain(the number of avg+1) > remain(remainder) ex:(3,3,4)(4,4,2)
		if g == 0 || groupCount[g] > averageCount+1 || (curRemain > remain && groupCount[g] == averageCount+1) {
			if len(lessThanAvg) != 0 {
				// Get the first group in lessThanAvg
				cfg.Shards[i] = lessThanAvg[0]
				groupCount[lessThanAvg[0]]++
				// if up to avgCount, change to equalToAvg
				if groupCount[lessThanAvg[0]] == averageCount {
					equalToAvg = append(equalToAvg, lessThanAvg[0])
					lessThanAvg = lessThanAvg[1:]
				}
			} else {
				cfg.Shards[i] = equalToAvg[0]
				groupCount[equalToAvg[0]]++
				equalToAvg = equalToAvg[1:]
				curRemain++
			}
			if g != 0 {
				groupCount[g]--
				if groupCount[g] == averageCount+1 {
					curRemain++
				} else if groupCount[g] == averageCount {
					curRemain--
				}
			}
		}
	}
}
