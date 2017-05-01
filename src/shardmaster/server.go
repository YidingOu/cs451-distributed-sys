package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "log"
import "time"


var Debug = 0

const time_1 = time.Second * 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	ack map[int64]int
	ret map[int]chan OpReply
	configNum int


	configs []Config // indexed by config num
}

const Join = "Join"
const Leave = "Leave"
const Move = "Move"
const Query = "Query"



type Op struct {
	// Your data here.
	OpType string
	Args interface{}
}

type OpReply struct{
	OpType string
	Args interface{}
	reply interface{}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	f := Op{OpType: "Join", Args: *args}
	index, _, isLeader := sm.rf.Start(f)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	_, ok := sm.ret[index]
	if ok == false {
		sm.ret[index] = make(chan OpReply, 1)

	}
	cha := sm.ret[index]
	sm.mu.Unlock()

	select {
	case log := <- cha:
		argus, ok := log.args.(JoinArgs)
		if ok == false {
			reply.WrongLeader = true
		}else{
			if args.ClientId == argus.ClientId {
				if args.RequestId == argus.RequestId {
					reply.Err = log.reply.(JoinReply).Err
					reply.WrongLeader = false
					}else{
						reply.WrongLeader= true
					}
			}else{
				reply.WrongLeader = true
			}
		}
	case <- time.After(time_1):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	f := Op{OpType: "Leave", Args: *args}
	index, _, isLeader := sm.rf.Start(f)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	_, ok := sm.ret[index]
	if ok == false {
		sm.ret[index] = make(chan OpReply, 1)

	}
	cha := sm.ret[index]
	sm.mu.Unlock()

	select {
	case log := <- cha:
		argus, ok := log.args.(LeaveArgs)
		if ok == false {
			reply.WrongLeader = true
		}else{
			if args.ClientId == argus.ClientId {
				if args.RequestId == argus.RequestId {
					reply.Err = log.reply.(LeaveReply).Err
					reply.WrongLeader = false
					}else{
						reply.WrongLeader= true
					}
			}else{
				reply.WrongLeader = true
			}
		}
	case <- time.After(time_1):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	f := Op{OpType: "Move", Args: *args}
	index, _, isLeader := sm.rf.Start(f)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	_, ok := sm.ret[index]
	if ok == false {
		sm.ret[index] = make(chan OpReply, 1)

	}
	cha := sm.ret[index]
	sm.mu.Unlock()

	select {
	case log := <- cha:
		argus, ok := log.args.(MoveArgs)
		if ok == false {
			reply.WrongLeader = true
		}else{
			if args.ClientId == argus.ClientId {
				if args.RequestId == argus.RequestId {
					reply.Err = log.reply.(MoveReply).Err
					reply.WrongLeader = false
					}else{
						reply.WrongLeader= true
					}
			}else{
				reply.WrongLeader = true
			}
		}
	case <- time.After(time_1):
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	f := Op{OpType: "Query", Args: *args}
	index, _, isLeader := sm.rf.Start(f)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	_, ok := sm.ret[index]
	if ok == false {
		sm.ret[index] = make(chan OpReply, 1)

	}
	cha := sm.ret[index]
	sm.mu.Unlock()

	select {
	case log := <- cha:
		argus, ok := log.args.(QueryArgs)
		if ok == false {
			reply.WrongLeader = true
		}else{
			if args.ClientId == argus.ClientId {
				if args.RequestId == argus.RequestId {
					reply.Err = log.reply.(QueryReply).Err
					reply.WrongLeader = false
					}else{
						reply.WrongLeader= true
					}
			}else{
				reply.WrongLeader = true
			}
		}
	case <- time.After(time_1):
		reply.WrongLeader = true
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//

func (sm *ShardMaster) Update() {
	for true {
		log := <- sm.applyCh
		tp := log.Command.(Op)
		var cid int64
		var rid int
		var result OpReply
		switch tp.OpType{
		case Join:
			args := tp.Args.(JoinArgs)
			cid = args.ClientId
			rid = args.RequestId
			result.args = args
		case Leave:
			args := tp.Args.(LeaveArgs)
			cid = args.ClientId
			rid = args.RequestId
			result.args = args
		case Move:
			args := tp.Args.(MoveArgs)
			cid = args.ClientId
			rid = args.RequestId
			result.args = args
		case Query:
			args := tp.Args.(QueryArgs)
			cid = args.ClientId
			rid = args.RequestId
			result.args = args
		}
		result.OpType = tp.OpType
		dup := sm.duplication(cid, rid)
		result.reply = sm.getApply(tp, dup)
		sm.sendResult(log.Index, result)
		sm.Validation()
	}
}

func (sm *ShardMaster) Validation(){
	c := sm.configs[sm.configNum]
	for _, v := range c.Shards {
		if len(c.Groups) == 0 && v == 0 {
			continue
		}
		if _, ok := c.Groups[v]; !ok {
			DPrintln("Check failed that", v, "group does not exit", c.Shards, c.Groups)
		}
	}
}


func (sm *ShardMaster) Apply(request Op, isDuplicated bool) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch request.Args.(type) {
	case JoinArgs:
		var reply JoinReply
		if !isDuplicated {
			sm.ApplyJoin(request.Args.(JoinArgs))
		}
		reply.Err = OK
		return reply
	case LeaveArgs:
		var reply LeaveReply
		if !isDuplicated {
			sm.ApplyLeave(request.Args.(LeaveArgs))
		}
		reply.Err = OK
		return reply
	case MoveArgs:
		var reply MoveReply
		if !isDuplicated {
			sm.ApplyMove(request.Args.(MoveArgs))
		}
		reply.Err = OK
		return reply
	case QueryArgs:
		var reply QueryReply
		args := request.Args.(QueryArgs)
		if args.Num == -1 || args.Num > sm.configNum {
			reply.Config = sm.configs[sm.configNum]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		reply.Err = OK
		return reply
	}
	return nil
}

func (sm *ShardMaster) ApplyJoin(args JoinArgs) {
	cfg := sm.NextConfig()
	_, exist := cfg.Groups[args.GID]
	if exist == false {
		cfg.Groups[args.GID] = args.Servers
		sm.ReBalanceShards(cfg, Join, args.GID)
	}
}

func (sm *ShardMaster) ApplyLeave(args LeaveArgs) {
	cfg := sm.NextConfig()
	_, exist := cfg.Groups[args.GID]
	if exist == false {
		delete(cfg.Groups, args.GID)
		sm.ReBalanceShards(cfg, Leave, args.GID)
	}
}

func (sm *ShardMaster) ApplyMove(args MoveArgs) {
	cfg := sm.NextConfig()
	cfg.Shards[args.Shard] = args.GID
}

func (sm *ShardMaster) ReBalanceShards(cfg *Config, request string, gid int) {
	shardsCount := sm.CountShards(cfg)
	switch request {
	case Join:
		meanNum := NShards / len(cfg.Groups)
		for i := 0; i < meanNum; i++ {
			maxGid := sm.GetMaxGidByShards(shardsCount)
			if len(shardsCount[maxGid]) == 0 {
				DPrintf("ReBalanceShards: max gid does not have shards")
				debug.PrintStack()
				os.Exit(-1)
			}
			cfg.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	case Leave:
		shardsArray := shardsCount[gid]
		delete(shardsCount, gid)
		for _, v := range(shardsArray) {
			minGid := sm.GetMinGidByShards(shardsCount)
			cfg.Shards[v] = minGid
			shardsCount[minGid] = append(shardsCount[minGid], v)
		}
	}
}

func (sm *ShardMaster) GetMaxGidByShards(shardsCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardsCount {
		if max < len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) GetMinGidByShards(shardsCount map[int][]int) int {
	min := -1
	var gid int
	for k, v := range shardsCount {
		if min == -1 || min > len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) CountShards(cfg *Config) map[int][]int {
	shardsCount := map[int][]int{}
	for k := range cfg.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range cfg.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}


func (sm *ShardMaster) sendResult(i int, result OpReply) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.ret[i]
	if ok == false {
		sm.ret[i] = make(chan OpReply, 1)
	} else {
		select {
		case <- sm.ret[i]:
		default:
		}
	}
	sm.ret[i] <- result
}


func (sm *ShardMaster) duplication(i int64, j int) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	value, ok := sm.ack[i]
	if ok == true{
		if value >= j {
			return true
		}
	}
	sm.ack[i] = j
	return false
}


func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveReply{})
	gob.Register(MoveReply{})
	gob.Register(QueryReply{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.configNum = 0
	sm.ack = make(map[int64]int)
	sm.ret = make(map[int]chan OpReply,1)	
	go sm.

	return sm
}


