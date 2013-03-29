package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

import "encoding/hex"
import cryptorand "crypto/rand"

func GenUUID() string {
  uuid := make([]byte, 16)
  n, err := cryptorand.Read(uuid)
  if n != len(uuid) || err != nil {
    return ""
  }
  
  uuid[8] = 0x80 // variant bits see page 5
  uuid[4] = 0x40 // version 4 Pseudo Random, see page 7

  return hex.EncodeToString(uuid)
}

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  nextPaxosSeq int
}

const (
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
)

type Op struct {
  Id string
  Type string
  GID int64
  Servers []string // for Join
  Shard int // for Move
}

func (sm *ShardMaster) Poll(seq int) interface{} {
  to := 10 * time.Millisecond
  for {
    decided, value := sm.px.Status(seq)
    
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


func (sm *ShardMaster) InsertOperationIntoLog(operation Op) int {
  var seq = sm.nextPaxosSeq
  
  for {
    sm.px.Start(seq, operation)
    
    var agreedValue Op
    agreedValue = sm.Poll(seq).(Op)
    
    if agreedValue.Id == operation.Id {
      break
    }
    
    seq++
  }
  
  return seq
}

func (sm *ShardMaster) ProcessLog(stop int) {
  var start = sm.nextPaxosSeq
  
  for i := start; i <= stop; i++ {
    var done, value = sm.px.Status(i)
    
    var currentConfig Config = sm.configs[len(sm.configs) - 1]
    var nextConfig Config = Config{}
    
    nextConfig.Num = currentConfig.Num + 1
    nextConfig.Groups = make(map[int64][]string)
    nextConfig.Shards = currentConfig.Shards
    for k, v := range currentConfig.Groups {
      nextConfig.Groups[k] = v
    }
    
    if done {
      var currentOp Op = value.(Op)
      
      if currentOp.Type == Join {
        nextConfig.Groups[currentOp.GID] = currentOp.Servers
        var optimalGroupSize int = NShards / len(nextConfig.Groups)
        var groupSize map[int64]int = make(map[int64]int)
        var newGroup = currentOp.GID
        
        for _, group := range nextConfig.Shards {
          if group != 0 {
            groupSize[group]++
          }
        }
        
        for index, group := range nextConfig.Shards {
          if groupSize[newGroup] == optimalGroupSize {
            break;
          }
          
          if group == 0 || groupSize[group] > optimalGroupSize {
            groupSize[group]--
            groupSize[newGroup]++
            nextConfig.Shards[index] = newGroup
          }
        }
        
      } else if currentOp.Type == Leave {
        delete(nextConfig.Groups, currentOp.GID)
        
        var oldGroup = currentOp.GID
        var groupSize map[int64]int = make(map[int64]int)
        
        for group, _ := range nextConfig.Groups {
          groupSize[group] = 0
        }
        
        for _, group := range nextConfig.Shards {
          if group != 0 && group != oldGroup {
            groupSize[group]++
          }
        }
        
        for index, group := range nextConfig.Shards {
          if group == oldGroup {
            
            var newGroup int64 = 0
            var min = -1
            for k, v := range groupSize {
              if min == -1 || v < min {
                min = v
                newGroup = k
              }
            }
            
            nextConfig.Shards[index] = newGroup
            groupSize[newGroup]++
          }
        }
        
      } else if currentOp.Type == Move {
        nextConfig.Shards[currentOp.Shard] = currentOp.GID
      }
    }
    
    sm.configs = append(sm.configs, nextConfig)
  }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  var operation = Op {}
  operation.Id = GenUUID()
  operation.Type = Join
  operation.GID = args.GID
  operation.Servers = args.Servers
  
  var stop int = sm.InsertOperationIntoLog(operation)
  sm.ProcessLog(stop)
  
  sm.px.Done(stop)
  sm.nextPaxosSeq = stop + 1
  
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  var operation = Op {}
  operation.Id = GenUUID()
  operation.Type = Leave
  operation.GID = args.GID
  
  var stop int = sm.InsertOperationIntoLog(operation)
  sm.ProcessLog(stop)
  
  sm.px.Done(stop)
  sm.nextPaxosSeq = stop + 1
  
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  var operation = Op {}
  operation.Id = GenUUID()
  operation.Type = Move
  operation.GID = args.GID
  operation.Shard = args.Shard
  
  var stop int = sm.InsertOperationIntoLog(operation)
  sm.ProcessLog(stop)

  sm.px.Done(stop)
  sm.nextPaxosSeq = stop + 1
  
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  
  var operation = Op {}
  operation.Id = GenUUID()
  operation.Type = Query
  
  var stop int = sm.InsertOperationIntoLog(operation)
  sm.ProcessLog(stop)
  
  sm.px.Done(stop)
  sm.nextPaxosSeq = stop + 1
  
  var index int = args.Num
  
  if index < 0 || index >= len(sm.configs) {
    index = len(sm.configs) - 1
  }
  
  reply.Config = sm.configs[index]
  
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
