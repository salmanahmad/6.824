package kvpaxos

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

// Begin Salman Additions
import "time"
// End Salman Additions

const (
  PUT = "PUT"
  GET = "GET"
)

type Op struct {
  // Begin Salman Additions
  // TODO: I need to add some sort of thing to identify this request...the client request ide or something...
  Type string
  Id string
  Key string
  Value string
  // End Salman Additions
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Begin Salman Addition
  
  // End Salman Addition
}


func (kv *KVPaxos) Poll(seq int) interface{} {
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


func (kv *KVPaxos) ClearLog() {
  var min = kv.px.Min()
  var max = kv.px.Max()
      
  var putKeys map[string]int = make(map[string]int)
  var duplicatePutKeys map[string]int = make(map[string]int)
  var minDone int = 0
      
  for i := max; i >= min; i-- {
    // Search the log
    var done, value = kv.px.Status(i)
        
    if !done {
      if i <= minDone {
        minDone = i
      }
    } else {
      var op = value.(Op)
          
      if (op.Type == PUT) {
        var _, found = putKeys[op.Key]
        if found {
          var _, duplicateFound = duplicatePutKeys[op.Key]
          if !duplicateFound {
            duplicatePutKeys[op.Key] = i
          }
        } else {
          putKeys[op.Key] = 1
        }
      }
    }
  }
      
  var done = -1
  for key, _ := range(duplicatePutKeys) {
    var value = duplicatePutKeys[key]
    if done == -1 || value <= done {
      done = value
    }
  }
      
  if done > minDone {
    kv.px.Done(done)
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var operation = Op {}
  operation.Type = GET
  operation.Id = args.Id
  operation.Key = args.Key
  operation.Value = ""
  
  var seq = kv.px.Max()
  
  for {
    seq++
    
    kv.px.Start(seq, operation)
    var agreedValue = kv.Poll(seq)
    
    if agreedValue == operation {
      var min = kv.px.Min()
      
      for i := seq; i >= min; i-- {
        // Search the log
        var done, value = kv.px.Status(i)
        
        if done {
          var op = value.(Op)
          
          if (op.Type == PUT) && (op.Key == args.Key) {
            reply.Value = op.Value
            break
          }
        }
      }
      
      kv.ClearLog()
      break
    }
  }
  
  reply.Err = OK
  return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var operation = Op {}
  operation.Type = PUT
  operation.Id = args.Id
  operation.Key = args.Key
  operation.Value = args.Value
  
  var start = kv.px.Min()
  var stop = kv.px.Max()
  
  for i := start; i <= stop; i++ {
    var done, value = kv.px.Status(i)
    if done {
      var op = value.(Op)
      if op  == operation {
        reply.Err = OK
        return nil
      }
    }
  }
  
  var seq = kv.px.Max()
  
  for {
    seq++
      
    kv.px.Start(seq, operation)
    var agreedValue = kv.Poll(seq)
    
    if agreedValue == operation {
      kv.ClearLog()
      break
    }
  }
  
  reply.Err = OK
  
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // this call is all that's needed to persuade
  // Go's RPC library to marshall/unmarshall
  // struct Op.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.

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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

