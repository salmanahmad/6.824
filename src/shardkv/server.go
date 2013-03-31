package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const PutType = "Put"
const GetType = "Get"
const ConfigType = "Config"
const PutShardType = "PutShardType"

type Op struct {
  Id string
  ClientId string
  RequestId int
  
  Type string
  Key string
  Value string
  
  Config shardmaster.Config
  
  ConfigNum int
  Shard int
  Database map[string]string
}

type Reply struct {
  ClientId string
  RequestId int
  Err Err
  Value string // Only for Get requests
}

type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos
  
  gid int64 // my replica group ID
  
  nextPaxosSeq int
  database map[string]string
  lastClientReplies map[string]Reply
  
  shardkvClerk *Clerk
  currentConfig shardmaster.Config
  obtainedShards map[int]bool
  
}

func (kv *ShardKV) ProcessingConfiguration() bool {
  for shard, gid := range kv.currentConfig.Shards {
    if gid == kv.gid {
      var found, obtained = kv.obtainedShards[shard]
      if !(obtained && found) {
        return true
      }
    }
  }
  
  return false
}

func (kv *ShardKV) Poll(seq int) interface{} {
  to := 10 * time.Millisecond
  for {
    decided, value := kv.px.Status(seq)
    
    if decided {
      return value
    }
    
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  
  return nil
}

func (kv *ShardKV) InsertOperationIntoLog(operation Op) int {
  var seq = kv.nextPaxosSeq
  
  for {
    kv.px.Start(seq, operation)
    
    var agreedValue Op
    agreedValue = kv.Poll(seq).(Op)
    
    if agreedValue.Id == operation.Id {
      break
    }
    
    seq++
  }
  
  return seq
}

func (kv *ShardKV) ProcessLog(stop int) Reply {
  var start = kv.nextPaxosSeq
  var returnReply Reply = Reply{}
  
  for i := start; i <= stop; i++ {
    var done, value = kv.px.Status(i)
    
    if done {
      var currentOp Op = value.(Op)
      var lastReply Reply = kv.lastClientReplies[currentOp.ClientId]
      
      var reply = Reply{}
      reply.ClientId = currentOp.ClientId
      reply.RequestId = currentOp.RequestId
      
      if currentOp.Type == PutType {
        if lastReply.RequestId >= currentOp.RequestId {
          // TODO: This may not be correct...
          returnReply = lastReply
        } else {
          if kv.ProcessingConfiguration() {
            // We are in the middle of a reconfiguration
            reply.Err = ErrWrongGroup
            kv.lastClientReplies[reply.ClientId] = reply
            returnReply = reply
          } else {
            if kv.currentConfig.Shards[key2shard(currentOp.Key)] == kv.gid {
              reply.Err = OK
              kv.lastClientReplies[reply.ClientId] = reply
              returnReply = reply
            
              kv.database[currentOp.Key] = currentOp.Value
            } else {
              reply.Err = ErrWrongGroup
              kv.lastClientReplies[reply.ClientId] = reply
              returnReply = reply
            }
          }
        }
      } else if currentOp.Type == GetType {
        if lastReply.RequestId >= currentOp.RequestId {
          // TODO: This may not be correct...
          returnReply = lastReply
        } else {
          if kv.ProcessingConfiguration() {
            // We are in the middle of a reconfiguration
            reply.Err = ErrWrongGroup
            kv.lastClientReplies[reply.ClientId] = reply
            returnReply = reply
          } else {
            if kv.currentConfig.Shards[key2shard(currentOp.Key)] == kv.gid {
              reply.Err = OK
              reply.Value = kv.database[currentOp.Key]
              kv.lastClientReplies[reply.ClientId] = reply
              returnReply = reply
            } else {
              reply.Err = ErrWrongGroup
              kv.lastClientReplies[reply.ClientId] = reply
              returnReply = reply
            }
          }
        }
      } else if currentOp.Type == ConfigType {
        if currentOp.Config.Num == kv.currentConfig.Num + 1 {
          // This is the next configuration from where we are now, so we will accept it.
          
          for shard, gid := range kv.currentConfig.Shards {
            if gid == kv.gid {
              // I currently own this shard. I need to send it...
              var destinationGid = currentOp.Config.Shards[shard]
              if destinationGid != gid {
                // I am not next owner. So I actually need to send it...
                
                var database = make(map[string]string)
                for key, value := range kv.database {
                  if key2shard(key) == shard {
                    database[key] = value
                  }
                }
                
                kv.shardkvClerk.PutShard(currentOp.Config.Groups[destinationGid], currentOp.Config.Num, database)
              }
            }
          }
          
          kv.currentConfig = currentOp.Config
          kv.obtainedShards = make(map[int]bool)
          returnReply = Reply{}
          returnReply.Err = OK
          
          for key, _ := range kv.database {
            if kv.currentConfig.Shards[key2shard(key)] != kv.gid {
              // This is not my key anymore...
              delete(kv.database, key)
            }
          }
        } else if currentOp.Config.Num <= kv.currentConfig.Num {
          // We have already seen this configuration.
          returnReply = Reply{}
          returnReply.Err = OK
        } else {
          // We are not ready for this configuration yet.
          returnReply = Reply{}
          returnReply.Err = ErrWrongGroup
        }
      } else if currentOp.Type == PutShardType {
        if (currentOp.ConfigNum == kv.currentConfig.Num) && 
           (kv.ProcessingConfiguration())  {
          // This is the database that we were waiting for.
          
          kv.obtainedShards[currentOp.Shard] = true
          
          for key, value := range currentOp.Database {
            kv.database[key] = value
          }
          
          returnReply = Reply{}
          returnReply.Err = OK
        } else if currentOp.ConfigNum <= kv.currentConfig.Num {
          // We already got this database. Thanks, though!
          returnReply = Reply{}
          returnReply.Err = OK
        } else {
          // We have not seen this configuration yet. Try again.
          returnReply = Reply{}
          returnReply.Err = ErrWrongGroup
        }
      }
    }
  }
  
  return returnReply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var op Op = Op{}
  op.Id = GenUUID()
  op.Type = GetType
  op.ClientId = args.ClientId
  op.RequestId = args.RequestId
  op.Key = args.Key
  
  var stop int = kv.InsertOperationIntoLog(op)
  var resp Reply = kv.ProcessLog(stop)
  
  reply.Err = resp.Err
  reply.Value = resp.Value
  
  kv.px.Done(stop)
  kv.nextPaxosSeq = stop + 1
  
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var op Op = Op{}
  op.Id = GenUUID()
  op.Type = PutType
  op.ClientId = args.ClientId
  op.RequestId = args.RequestId
  op.Key = args.Key
  op.Value = args.Value
  
  var stop int = kv.InsertOperationIntoLog(op)
  var resp Reply = kv.ProcessLog(stop)
  
  reply.Err = resp.Err
  
  kv.px.Done(stop)
  kv.nextPaxosSeq = stop + 1
  
  return nil
}

func (kv *ShardKV) PutShard(args *PutShardArgs, reply *PutShardReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var op Op = Op{}
  op.Id = GenUUID()
  op.Type = PutShardType
  op.ClientId = args.ClientId
  op.RequestId = args.RequestId
  op.ConfigNum = args.ConfigNum
  op.Shard = args.Shard
  op.Database = args.Database
  
  var stop int = kv.InsertOperationIntoLog(op)
  var resp Reply = kv.ProcessLog(stop)
  
  reply.Err = resp.Err
  
  kv.px.Done(stop)
  kv.nextPaxosSeq = stop + 1
  
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var previousConfigNum int = kv.currentConfig.Num
  var stop int = -1
  
  for {
    var config shardmaster.Config = kv.sm.Query(previousConfigNum + 1)
    if config.Num != previousConfigNum {
      var op Op = Op{}
      op.Id = GenUUID()
      op.Type = ConfigType
      op.Config = config
      
      stop = kv.InsertOperationIntoLog(op)
    } else {
      break
    }
  }
  
  kv.ProcessLog(stop)
  kv.px.Done(stop)
  kv.nextPaxosSeq = stop + 1
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  
  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  
  // Your initialization code here.
  // Don't call Join().
  kv.database = make(map[string]string)
  kv.lastClientReplies = make(map[string]Reply)
  kv.obtainedShards = make(map[int]bool)
  kv.shardkvClerk = MakeClerk(shardmasters)
  
  rpcs := rpc.NewServer()
  rpcs.Register(kv)
  
  kv.px = paxos.Make(servers, me, rpcs)
  
  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
