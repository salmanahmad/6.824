package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  startUp bool
  timeKeeper map[string]time.Time
  currentView View
  primaryView View
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  
  if vs.startUp {
    // The first request is special. Any server can be primary
    vs.startUp = false

    // Note: The primary view has not changed yet because it has not acked
    vs.currentView.Viewnum++
    vs.currentView.Primary = args.Me
  }

  if args.Me == vs.currentView.Primary {
    // Send the primary the pending view and wait for it to ack it
    if args.Viewnum == vs.primaryView.Viewnum {
      vs.timeKeeper[args.Me] = time.Now()
    }

    if args.Viewnum == vs.currentView.Viewnum {
      vs.primaryView = vs.currentView
    }

    reply.View = vs.currentView
  } else {
    // For everyone else respond with the current view
    vs.timeKeeper[args.Me] = time.Now()

    if vs.currentView.Viewnum == vs.primaryView.Viewnum {
      if vs.currentView.Backup == "" {
        vs.currentView.Viewnum++
        vs.currentView.Backup = args.Me

        //fmt.Printf("View Service: Setting the backup server to: %s\n", args.Me)
      }
    }

    reply.View = vs.currentView
  }

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.currentView
  
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  for server, _ := range vs.timeKeeper {
    var lastPingTime = vs.timeKeeper[server]
    var duration = time.Now().Sub(lastPingTime)
    if duration > (PingInterval * DeadPings) {
      // The server is considered dead...
      //fmt.Printf("Killing server: %s\n", server)
      delete(vs.timeKeeper, server)
      if(server == vs.currentView.Primary) {
        if vs.currentView.Viewnum == vs.primaryView.Viewnum {
          vs.currentView.Viewnum++
          vs.currentView.Primary = vs.currentView.Backup
          vs.currentView.Backup = ""

          vs.primaryView.Viewnum = 0
          vs.primaryView.Primary = ""
          vs.primaryView.Backup = ""
        }
      }
      
      if(server == vs.currentView.Backup) {
        vs.currentView.Viewnum++
        vs.currentView.Backup = ""
      }
    }
  }
  
  //fmt.Printf("View Service: Current View: (%s, %s)\n", vs.currentView.Primary, vs.currentView.Backup)    
  
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  
  // START Your vs.* initializations here.
  vs.startUp = true
  vs.timeKeeper = make(map[string]time.Time)
  // END Your vs.* initializations here.

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
