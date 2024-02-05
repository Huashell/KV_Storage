package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClerkId  int64
	Version  int
	LeaderId int
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
	ck.ClerkId = nrand()
	ck.LeaderId = 0
	ck.Version = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:      key,
		Version:  ck.Version,
		ClientId: int(ck.ClerkId),
	}
	reply := &GetReply{}
	ret := ""
	// You will have to modify this function.
	for {
		if ck.servers[ck.LeaderId].Call("KVServer.Get", &args, &reply) {
			switch reply.Err {
			case ErrWrongLeader, ErrTimeOut:
				ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
				break
			case ErrNoKey, OK:
				ret = reply.Value
				ck.Version += 1
				return ret
			}
		} else {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Version:  int64(ck.Version),
		ClientId: ck.ClerkId,
	}
	reply := &PutAppendReply{}
	for {
		if ck.servers[ck.LeaderId].Call("KVServer.PutAppend", &args, &reply) {
			switch reply.Err {
			case ErrWrongLeader, ErrTimeOut:
				ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
				break
			case ErrNoKey, OK:
				ck.Version += 1
				return
			}
		} else {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
