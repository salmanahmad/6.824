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


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
}


// catch up -- execute operations we have not seen yet,
// up to and including seq.
// caller must hold the mutex, at least to avoid
// concurrent calls.
// maybe a separate thread should be doing this.
func (kv *KVPaxos) catchUp(seq int) {
  for kv.seq <= seq {
    to := 10 * time.Millisecond
    for {
      if kv.seq < kv.px.Min() {
        log.Fatalf("%v: catchUp(%v) replay wait for kv.seq=%v Min()=%v\n",
          kv.me, seq, kv.seq, kv.px.Min())
      }
      decided, xop := kv.px.Status(kv.seq)
      if decided {
        yop := xop.(Op)
        want, _ := kv.cseqs[yop.CID]
        if want == yop.Cseq {
          kv.cseqs[yop.CID] = want + 1
          if yop.Op == opPut {
            kv.data[yop.Key] = yop.Value
          }
        } else if yop.Op != opNop {
          fmt.Printf("me=%v wrong cseq; seq=%v op %v key=%v value=%v cid %v expecting %v got %v\n",
            kv.me, kv.seq, yop.Op, yop.Key, yop.Value, yop.CID, want, yop.Cseq)
        }
        break
      }
      time.Sleep(to) // XXX too long?
      to *= 2
      if to > 100 * time.Millisecond {
        kv.startNop(kv.seq)
      }
    }
    kv.seq += 1
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.


  return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.


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

