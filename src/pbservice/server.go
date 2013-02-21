package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  
  // START Salman's Additions
  client *Clerk
  
  view viewservice.View
  data map[string]string
  // END Salman's Additions
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if(pb.IsPrimary()) {
    reply.Value = pb.DoGet(args.Key)
    reply.Err = OK    
  } else {
    reply.Err = ErrWrongServer
  }

  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  if(pb.IsPrimary()) {
    pb.DoPut(args.Key, args.Value)
    
    if(pb.HasBackup()) {
      pb.client.view = pb.view
      pb.client.ForwardPut(args)
    }
    
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer
  }
  
  return nil
}

func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  if(pb.IsBackup()) {
    pb.DoPut(args.Key, args.Value)
    reply.Err = OK    
  } else {
    reply.Err = ErrWrongServer
  }

  
  return nil
}

func (pb *PBServer) SetData(args *SetDataArgs, reply *SetDataReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  if(pb.IsBackup()) {
    pb.data = args.Data
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer
  }
  
  return nil
}

func (pb *PBServer) IsPrimary() bool {
  return pb.view.Primary == pb.me
}

func (pb *PBServer) IsBackup() bool {
  return pb.view.Backup == pb.me
}

func (pb *PBServer) HasBackup() bool {
  return pb.view.Backup != ""
}

func (pb *PBServer) DoGet(key string) string {
  return pb.data[key]
}

func (pb *PBServer) DoPut(key string, value string) {
  pb.data[key] = value
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  view, _ := pb.vs.Ping(pb.view.Viewnum)

  if(view != pb.view) {
    pb.view = view
    
    if(pb.IsPrimary() && pb.HasBackup()) {
      pb.client.view = pb.view
      pb.client.SetData(pb.data)
    }
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  
  // START Salman's Additions
  pb.client = MakeClerk(vshost, me)
  pb.data = make(map[string]string)
  // END Salman's Additions

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
