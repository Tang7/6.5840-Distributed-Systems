package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv      map[string]string
	history map[int64]*SeqValue
}

type SeqValue struct {
	Value  string
	SeqNum uint64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// always return the latest value
	reply.Value = kv.kv[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// ignore old requests
	if v, ok := kv.history[args.ClientId]; ok && v.SeqNum >= args.SeqNum {
		return
	}

	kv.kv[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// return old value in history if it's an old request
	if v, ok := kv.history[args.ClientId]; ok && v.SeqNum >= args.SeqNum {
		reply.Value = v.Value
		return
	}
	// based on test file line 258, append value should not in returned value
	reply.Value = kv.kv[args.Key]

	// update history to the current latest value
	// based on test file line 258, append value should not in history
	kv.history[args.ClientId] = &SeqValue{
		Value:  kv.kv[args.Key],
		SeqNum: args.SeqNum,
	}

	kv.kv[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.history = make(map[int64]*SeqValue)

	return kv
}
