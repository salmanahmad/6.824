package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

import "encoding/hex"
import "crypto/rand"

func GenUUID() string {
  uuid := make([]byte, 16)
  n, err := rand.Read(uuid)
  if n != len(uuid) || err != nil {
    return ""
  }
  
  uuid[8] = 0x80 // variant bits see page 5
  uuid[4] = 0x40 // version 4 Pseudo Random, see page 7
  
  return hex.EncodeToString(uuid)
}


const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  
  ClientId string
  RequestId int
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
  
  ClientId string
  RequestId int
}

type GetReply struct {
  Err Err
  Value string
}

type PutShardArgs struct {
  ConfigNum int
  Database map[string]string
  
  ClientId string
  RequestId int
}

type PutShardReply struct {
  Err Err
}

