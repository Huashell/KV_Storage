package kvraft

import (
	//"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Version  int64
	OpType   string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 3A
	stateMachine   *MemoryKV
	notifyChanMap  map[int]chan *CommonReply
	lastRequestMap map[int64]ReplyContext
	// 3B
	persist     *raft.Persister
	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		Version:  args.Version,
		OpType:   GET,
		Key:      args.Key,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()

	DPrintf("server = {%d} wait notify or timeout", kv.me)
	select {
	case ret := <-notifyChan:
		DPrintf("server = %d get ret = %v", kv.me, ret)
		reply.Value, reply.Err = ret.Value, ret.Err
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			DPrintf("server = {%d} notify false node, isLeader = %t currentTerm = %d term = %d", kv.me, isLeader, currentTerm, term)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("server = {%d} timeout", kv.me)
		reply.Err = TimeOut
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if kv.isOldRequest(args.ClientId, args.Version) {
		DPrintf("server = {%d} get old request", kv.me)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		Version:  args.Version,
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.Err = ret.Err
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
	kv.stateMachine = NewMemoryKV()
	kv.notifyChanMap = make(map[int]chan *CommonReply)
	kv.lastRequestMap = make(map[int64]ReplyContext)
	kv.persist = persister

	go kv.applier()
	DPrintf("server = %d start", kv.me)
	return kv
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("server {%d} receive applyMsg = %v", kv.me, applyMsg)
			kv.mu.Lock()
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf("server {%d} drop command index = %d lastApplied = %d", kv.me, applyMsg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				reply := kv.apply(applyMsg.Command)
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CommandTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}

				kv.lastApplied = applyMsg.CommandIndex
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) apply(cmd interface{}) *CommonReply {
	reply := &CommonReply{}
	op := cmd.(Op)
	DPrintf("server {%d} apply command = %v\n", kv.me, op)
	if op.OpType != GET && kv.isOldRequest(op.ClientId, op.Version) {
		reply.Err = OK
	} else {
		reply = kv.applyLogToStateMachine(&op)
		if op.OpType != GET {
			kv.updateLastRequest(&op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (kv *KVServer) applyLogToStateMachine(op *Op) *CommonReply {
	var reply = &CommonReply{}

	switch op.OpType {
	case GET:
		reply.Value = kv.stateMachine.get(op.Key)
	case PUT:
		kv.stateMachine.put(op.Key, op.Value)
	case APPEND:
		kv.stateMachine.appendVal(op.Key, op.Value)
	}

	reply.Err = OK

	return reply
}

func (kv *KVServer) notify(index int, reply *CommonReply) {
	DPrintf("server {%d} notify index = %d\n", kv.me, index)
	if notifyCh, ok := kv.notifyChanMap[index]; ok {
		notifyCh <- reply
	}
	DPrintf("server {%d} notify index = %d\n", kv.me, index)
}

func (kv *KVServer) isOldRequest(clientId int64, Version int64) bool {
	if cxt, ok := kv.lastRequestMap[clientId]; ok {
		if Version <= cxt.LastVersion {
			return true
		}
	}

	return false
}

func (kv *KVServer) updateLastRequest(op *Op, reply *CommonReply) {

	ctx := ReplyContext{
		LastVersion: op.Version,
		Reply:       *reply,
	}

	lastCtx, ok := kv.lastRequestMap[op.ClientId]
	if (ok && lastCtx.LastVersion < op.Version) || !ok {
		kv.lastRequestMap[op.ClientId] = ctx
	}
}
