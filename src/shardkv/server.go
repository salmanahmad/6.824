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
  configurationLock sync.Mutex
  configurationChannel chan int
  database map[string]string
  lastClientReplies map[string]Reply
  
  currentConfig shardmaster.Config
  nextConfig shardmaster.Config
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
  
  var lastReply Reply = Reply{}
  
  for i := start; i <= stop; i++ {
    
  }
  
  return lastReply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  kv.configurationLock.Lock()
  defer kv.configurationLock.Unlock()
  
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
  
  kv.configurationLock.Lock()
  defer kv.configurationLock.Unlock()
  
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
  kv.configurationLock.Lock()
  defer kv.configurationLock.Unlock()
  
  var op Op = Op{}
  op.Id = GenUUID()
  op.Type = PutShardType
  op.ClientId = args.ClientId
  op.RequestId = args.RequestId
  op.ConfigNum = args.ConfigNum
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
  
  kv.configurationLock.Lock()
  
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
  
  if stop != -1 {
    kv.ProcessLog(stop)
    kv.px.Done(stop)
    kv.nextPaxosSeq = stop + 1
    
    var nextConfigurationNum = kv.nextConfig.Num
    
    kv.configurationLock.Unlock()
    
    for {
      var configurationNum int = <- kv.configurationChannel
      if  nextConfigurationNum == configurationNum {
        break
      }
    }
  } else {
    kv.configurationLock.Unlock()
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
  kv.configurationChannel = make(chan int)
  kv.database = make(map[string]string)
  kv.lastClientReplies = make(map[string]Reply)

  
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
