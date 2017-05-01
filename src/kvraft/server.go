package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
)
const Debug = 1

const time_1 = time.Second * 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType	string
	Args	interface{}
}

type OpReply struct {
	OpType	string
	args  interface{}
	reply interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string	
	ack 	map[int64]int		
	ret map[int]chan OpReply
	persister *raft.Persister
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	f := Op{OpType: "Get", Args: *args}
	index, _, isLeader := kv.rf.Start(f)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	_, ok := kv.ret[index]
	if ok == false {
		kv.ret[index] = make(chan OpReply, 1)

	}
	cha := kv.ret[index]
	kv.mu.Unlock()

	select {
	case log := <- cha:
		argus, ok := log.args.(GetArgs)
		if ok == false {
			reply.WrongLeader = true
		}else{
			if args.ClientId == argus.ClientId {
				if args.RequestId == argus.RequestId {
					*reply = log.reply.(GetReply)
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

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{OpType: "PutAppend", Args: *args})
	if isLeader == false {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	_, ok := kv.ret[index] 
	if ok == false {
		kv.ret[index] = make(chan OpReply, 1)
	}
	cha := kv.ret[index]
	kv.mu.Unlock()
	select {
	case log := <- cha:
		argus, ok := log.args.(PutAppendArgs)
		if ok == false {
			reply.WrongLeader = true
		}else{
			if args.ClientId == argus.ClientId {
				if args.RequestId == argus.RequestId {
					reply.Err = log.reply.(PutAppendReply).Err
					reply.WrongLeader = false
				}else{
					reply.WrongLeader = true
				}
			}else{
				reply.WrongLeader = true
			}
		}
	case <- time.After(time_1):
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) UseSnp(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var LastIncludedIndex int
	var LastIncludedTerm int
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int)

	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	d.Decode(&kv.data)
	d.Decode(&kv.ack)
}

func (kv * RaftKV) CheckSnp(index int) {
	if kv.maxraftstate != -1 && float64(kv.rf.GetPersistSize()) > float64(kv.maxraftstate)*0.8 {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.ack)
		data := w.Bytes()
		go kv.rf.StartSnapshot(data, index)
	}
}

func (kv *RaftKV) Update() {
	for true {
		log := <- kv.applyCh
		if log.UseSnapshot == false{
			tp := log.Command.(Op)
			var cid int64
			var rid int
			var result OpReply
			if tp.OpType != "Get" {
				args := tp.Args.(PutAppendArgs)
				cid = args.ClientId
				rid = args.RequestId
				result.args = args
			} else {
				args := tp.Args.(GetArgs)
				cid = args.ClientId
				rid = args.RequestId
				result.args = args
			}
			result.OpType = tp.OpType
			dup := kv.duplication(cid, rid)
			result.reply = kv.getApply(tp, dup)
			kv.sendResult(log.Index, result)
			kv.CheckSnp(log.Index)
		}else{
			kv.UseSnp(log.Snapshot)
			
		}
	}
}



func (kv *RaftKV) sendResult(i int, result OpReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.ret[i]
	if ok == false {
		kv.ret[i] = make(chan OpReply, 1)
	} else {
		select {
		case <- kv.ret[i]:
		default:
		}
	}
	kv.ret[i] <- result
}

func (kv *RaftKV) helper(t Op, reply GetReply) GetReply{
		args := t.Args.(GetArgs)
		value, ok := kv.data[args.Key]
		if ok == false {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = value
		}
		return reply
}

func (kv *RaftKV) helper_2(t Op, reply PutAppendReply, dup bool) PutAppendReply{
		args := t.Args.(PutAppendArgs)
		if dup == false {
			if args.Op == "Put" {
				kv.data[args.Key] = args.Value
			} else {
				kv.data[args.Key] += args.Value
			}
		}
		reply.Err = OK
		return reply
}

func (kv *RaftKV) getApply(t Op, dup bool) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch t.Args.(type) {
	case GetArgs:
		var reply GetReply
		reply = kv.helper(t, reply)
		return reply
	case PutAppendArgs:
		var reply PutAppendReply
		reply = kv.helper_2(t,reply,dup)
		return reply
	}
	return nil
}

func (kv *RaftKV) duplication(i int64, j int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.ack[i]
	if ok == true{
		if value >= j {
			return true
		}
	}
	kv.ack[i] = j
	return false
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
func  StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int)
	kv.ret = make(map[int]chan OpReply)
	go kv.Update()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

