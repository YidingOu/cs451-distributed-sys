package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	leader	int
	clientId 	int64
	requestId	int
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
	ck.leader = 0
	ck.requestId = 0
	ck.clientId = nrand()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs {Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId = ck.requestId + 1
	m := ""
	n := len(ck.servers)
	for i := ck.leader; true; i = (i + 1) % n {
		reply := GetReply{}
		sev := ck.servers[i]
		ok := sev.Call("RaftKV.Get", &args, &reply) 
		if ok == true {
			if reply.WrongLeader == false {
				ck.leader = i
				if reply.Err != OK {
					m = ""
					break
				} else {
					m = reply.Value
					break
				}
			}
		}
	}
	return m
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs {Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId = ck.requestId + 1
	n:= len(ck.servers)
	for i := ck.leader; true; i = (i + 1) % n {
		reply := PutAppendReply{}
		sev := ck.servers[i]
		ok:= sev.Call("RaftKV.PutAppend", &args, &reply) 
		if ok == true {
			if reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}