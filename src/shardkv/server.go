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
  if kv.currentConfig.Num == 0 || kv.currentConfig.Num == 1 {
    return false
  }
  
  for shard, gid := range kv.currentConfig.Shards {
    if gid == kv.gid {
      var found, obtained = kv.obtainedShards[shard]
      if !(obtained && found) {
        //fmt.Printf("ProcessingConfig: Group %d has configuration: (%v) %v\n", kv.gid, kv.currentConfig.Num, kv.currentConfig.Shards)
        //fmt.Printf("ProcessingConfig: Group %d still waiting for shard: %d\n", kv.gid, shard)
        //fmt.Printf("ProcessingConfig: Group %d has obtained: %v\n", kv.gid, kv.obtainedShards)
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
  
  //fmt.Printf("\n\n%v\n\n", kv.lastClientReplies)
  
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
          //fmt.Printf("ProcessLog: Get Returning Last Reply: (%v, %v)\n", lastReply.RequestId, currentOp.RequestId)
          returnReply = lastReply
        } else {
          if kv.ProcessingConfiguration() {
            //fmt.Printf("ProcessLog: Get NOT Returning Last Reply - Processing Configuraiton\n")
            // We are in the middle of a reconfiguration
            reply.Err = ErrWrongGroup
            kv.lastClientReplies[reply.ClientId] = reply
            returnReply = reply
          } else {
            //fmt.Printf("ProcessLog: Get NOT Returning Last Reply - NOT Processing Configuraiton\n")
            if kv.currentConfig.Shards[key2shard(currentOp.Key)] == kv.gid {
              //fmt.Printf("ProcessLog: Reply: OK\n")
              reply.Err = OK
              reply.Value = kv.database[currentOp.Key]
              kv.lastClientReplies[reply.ClientId] = reply
              returnReply = reply
            } else {
              //fmt.Printf("ProcessLog: Reply: ErrWrongGroup: %v\n", kv.currentConfig.Shards)
              reply.Err = ErrWrongGroup
              kv.lastClientReplies[reply.ClientId] = reply
              returnReply = reply
            }
          }
        }
      } else if currentOp.Type == ConfigType {
        //fmt.Printf("ProcessLog: Configuration Change!\n")
        if (currentOp.Config.Num == kv.currentConfig.Num + 1) && (!kv.ProcessingConfiguration()) {
          // We have processed the last configuration and
          // this is the next one. We will accept it.
          
          //fmt.Printf("ProcessLog: Transitioning to a new view: (%v) %v\n", currentOp.Config.Num, currentOp.Config.Shards)
          kv.obtainedShards = make(map[int]bool)
          
          for shard, gid := range kv.currentConfig.Shards {
            var destinationGid = currentOp.Config.Shards[shard]
            if gid == kv.gid {
              // I currently own this shard. I need to send it...
              if destinationGid != gid {
                // I am not next owner. So I actually need to send it...
                
                var database = make(map[string]string)
                for key, value := range kv.database {
                  if key2shard(key) == shard {
                    database[key] = value
                  }
                }
                
                //fmt.Printf("Group %d sending shard %d to group %d\n", kv.gid, shard, destinationGid)
                //fmt.Printf("Current configuration: %v\n", currentOp.Config.Shards)
                
                go kv.shardkvClerk.PutShard(currentOp.Config.Groups[destinationGid], currentOp.Config.Num, shard, database)
                //fmt.Printf("Hi!\n")
              } else {
                kv.obtainedShards[shard] = true
              }
            } else if (gid == 0 && (destinationGid == kv.gid)) {
              kv.obtainedShards[shard] = true
            }
          }
          
          //fmt.Printf("ProcessLog: %v Obtained Shards:%v\n", kv.gid, kv.obtainedShards)
          
          kv.currentConfig = currentOp.Config
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
          //fmt.Printf("ProcessLog: We have already seen this configuration.\n")
          returnReply = Reply{}
          returnReply.Err = OK
        } else {
          // We are not ready for this configuration yet.
          //fmt.Printf("ProcessLog: We are not ready for this configuraiton yet.\n")
          returnReply = Reply{}
          returnReply.Err = ErrWrongGroup
        }
      } else if currentOp.Type == PutShardType {
        if (currentOp.ConfigNum == kv.currentConfig.Num) && 
           (kv.ProcessingConfiguration())  {
          // This is the database that we were waiting for.
          
          //fmt.Printf("ProcessLog: Got shard: %d\n", currentOp.Shard)
          
          kv.obtainedShards[currentOp.Shard] = true
          
          for key, value := range currentOp.Database {
            kv.database[key] = value
          }
          
          returnReply = Reply{}
          returnReply.Err = OK
        } else if currentOp.ConfigNum <= kv.currentConfig.Num {
          //fmt.Printf("ProcessLog: Ignoring shard because we already have it: %d\n", currentOp.Shard)
          // We already got this database. Thanks, though!
          returnReply = Reply{}
          returnReply.Err = OK
        } else {
          // We have not seen this configuration yet. Try again.
          //fmt.Printf("ProcessLog: Ignoring shard because we are not ready: %d\n", currentOp.Shard)
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
  
  //fmt.Printf("Processing Get\n")
  
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
  
  //fmt.Printf("Return Value: %v\n", reply.Err)
  
  kv.px.Done(stop)
  kv.nextPaxosSeq = stop + 1
  
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  //fmt.Printf("Processing Put\n")
  
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

  //fmt.Printf("PutShard: Waiting\n")

  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  //fmt.Printf("PutShard: Start\n")
  
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
  
  //fmt.Printf("PutShard: Response: %v\n", reply.Err)
  
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
  
  //fmt.Printf("Tick...\n")
  
  var stop int = -1
  
  var config shardmaster.Config = kv.sm.Query(kv.currentConfig.Num + 1)
  
  if config.Num == kv.currentConfig.Num + 1 {
    var op Op = Op{}
    op.Id = GenUUID()
    op.Type = ConfigType
    op.Config = config
    
    //fmt.Printf("Inserting Operation: %v\n", op)
    
    stop = kv.InsertOperationIntoLog(op)
    kv.ProcessLog(stop)
    kv.px.Done(stop)
    kv.nextPaxosSeq = stop + 1
  }
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
