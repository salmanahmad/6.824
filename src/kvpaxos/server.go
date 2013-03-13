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
  ClientId string
  Id string
  Key string
  Value string
  // End Salman Additions
}

type Response struct {
  Err Err
  Value string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Begin Salman Addition
  nextStart int
  data map[string]string
  pastClientRequestResponses map[string]Response
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

func (kv *KVPaxos) InsertOperationIntoLog(operation Op) int {
  var seq = kv.nextStart
  
  for {
    kv.px.Start(seq, operation)
    var agreedValue = kv.Poll(seq)
    
    if agreedValue == operation {
      break
    }
    
    seq++
  }
  
  return seq
}

func (kv *KVPaxos) ProcessLog(start int, stop int) {
  for i := start; i <= stop; i++ {
    var done, value = kv.px.Status(i)
    
    if done {
      var currentOp Op = value.(Op)
      
      if currentOp.Type == GET {
        var _, pastResponseFound = kv.pastClientRequestResponses[currentOp.Id]
        if !pastResponseFound {
          var response = Response{}
          
          var keyValue, keyFound = kv.data[currentOp.Key]
          if keyFound {
            response.Err = OK
            response.Value = keyValue
          } else {
            response.Err = ErrNoKey
          }
          
          kv.pastClientRequestResponses[currentOp.Id] = response
        }
      } else if currentOp.Type == PUT {
        var _, pastResponseFound = kv.pastClientRequestResponses[currentOp.Id]
        if !pastResponseFound {
          var response = Response{}
          response.Err = OK
          kv.pastClientRequestResponses[currentOp.Id] = response
          
          kv.data[currentOp.Key] = currentOp.Value
        }
      }
    }
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var operation = Op {}
  operation.Type = GET
  operation.ClientId = args.ClientId
  operation.Id = args.Id
  operation.Key = args.Key
  operation.Value = ""
  
  
  var response, found = kv.pastClientRequestResponses[operation.Id]
  if found {
    reply.Err = response.Err
    reply.Value = response.Value
    return nil
  }
  
  var seq int = kv.InsertOperationIntoLog(operation)
  
  var start = kv.nextStart
  var stop = seq
  
  kv.ProcessLog(start, stop)
  
  
  response, found = kv.pastClientRequestResponses[operation.Id]
  if found {
    reply.Err = response.Err
    reply.Value = response.Value
  }
  
  kv.px.Done(stop)
  kv.nextStart = stop + 1
  
  return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  var operation = Op {}
  operation.Type = PUT
  operation.ClientId = args.ClientId
  operation.Id = args.Id
  operation.Key = args.Key
  operation.Value = args.Value
  
  var response, found = kv.pastClientRequestResponses[operation.Id]
  if found {
    reply.Err = response.Err
    return nil
  }
  
  var seq int = kv.InsertOperationIntoLog(operation)
  
  var start = kv.nextStart
  var stop = seq
  
  kv.ProcessLog(start, stop)
  
  response, found = kv.pastClientRequestResponses[operation.Id]
  if found {
    reply.Err = response.Err
  }
  
  kv.px.Done(stop)
  kv.nextStart = stop + 1
  
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

  // Start Salman Addition
  kv.data = make(map[string]string)
  kv.pastClientRequestResponses = make(map[string]Response)
  // Start Salman Addition


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

