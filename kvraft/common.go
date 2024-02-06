package kvraft

import (
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 500 * time.Millisecond

const (
	PUT    = "PUT"
	GET    = "GET"
	APPEND = "APPEND"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "Timeout"
)

type Err string

// PUT or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "PUT" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Version  int64 // 客户端请求Id
	ClientId int64 // 访问的客户端Id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Version  int64 // 客户端请求Id
	ClientId int64 // 访问的客户端Id
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonArgs struct {
	Version  int64
	ClientId int64
}

type CommonReply struct {
	Err   Err
	Value string
}

type ReplyContext struct {
	LastVersion int64
	Reply       CommonReply
}

type MemoryKV struct {
	Data map[string]string
}

func (mkv *MemoryKV) put(key string, value string) {
	mkv.Data[key] = value
}

func (mkv *MemoryKV) appendVal(key string, value string) {
	originVal := mkv.Data[key]
	mkv.Data[key] = originVal + value
}

func (mkv *MemoryKV) hasKey(key string) bool {
	_, ok := mkv.Data[key]
	return ok
}

func (mkv *MemoryKV) get(key string) string {
	return mkv.Data[key]
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{
		make(map[string]string),
	}
}
