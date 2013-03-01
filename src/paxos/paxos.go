package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

// Start Salman Additions
import "container/list"
import "time"
// End Salman Additions

func Max(a int, b int) int {
  var max int
  if a > b {
    max = a
  } else {
    max = b
  }
  
  return max
}

func Min(a int, b int) int {
  var min int
  if a < b {
    min = a
  } else {
    min = b
  }
  
  return min
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  
  // Begin Salman Additions
  instances map[int]Instance
  maxReportedDones map[string]int
  // End Salman Additions
}

type Instance struct {
  agreementReached bool
  maxPromisedProposal int
  maxAcceptedProposal int
  value interface{}
}

func NewInstance() Instance {
  return Instance{false, -1, -1, nil}
}

type PrepareArgs struct {
  Instance int
  Proposal int
  Me string
  MaxDone int
}

type PrepareReply struct {
  Ok bool
  SuggestedNextProposal int
  MaxAcceptedProposal int
  Value interface{}
}

type AcceptArgs struct {
  Instance int
  Proposal int
  Value interface{}
}

type AcceptReply struct {
  Ok bool
  SuggestedNextProposal int
}

type DecidedArgs struct {
  Instance int
  Value interface{}
}

type DecidedReply struct {
  Ok bool
}

func (px *Paxos) Propose(instance int, value interface{}) {
  var proposal = 0
  var nextProposal = -1
  
  for {
    nextProposal += 1
    proposal = nextProposal
    
    var prepareReplies = list.New()
    var agreementThreshold = len(px.peers) / 2
    
    for peer := range px.peers {
      var me = px.peers[px.me]
      var prepareArgs = &PrepareArgs{instance, proposal, me, px.maxReportedDones[me]}
      var prepareReply PrepareReply
      
      if peer == px.me {
        px.Prepare(prepareArgs, &prepareReply)
        prepareReplies.PushBack(prepareReply)
      } else {
        ok := call(px.peers[peer], "Paxos.Prepare", prepareArgs, &prepareReply)
        if ok {
          prepareReplies.PushBack(prepareReply)
        } 
      }
    }
    
    var prepareOkCount = 0
    var maxAcceptedProposalValue interface{} = value
    var maxAcceptedProposal int = -1
    
    for e := prepareReplies.Front(); e != nil; e = e.Next() {
    	var reply = e.Value.(PrepareReply)
      if reply.Ok {
    	  prepareOkCount += 1
        
        if reply.MaxAcceptedProposal != -1 {
          if reply.MaxAcceptedProposal > maxAcceptedProposal {
            maxAcceptedProposal = reply.MaxAcceptedProposal
            maxAcceptedProposalValue = reply.Value
          }
        }
    	} else {
    	  nextProposal = Max(nextProposal, reply.SuggestedNextProposal)
    	}
    }
    
    var acceptReplies = list.New()
    
    if (prepareOkCount > agreementThreshold) {
      // We got agreement on the proposal number
      for peer := range px.peers {
        var acceptArgs = &AcceptArgs{instance, proposal, maxAcceptedProposalValue}
        var acceptReply AcceptReply
        
        if(peer == px.me) {
          px.Accept(acceptArgs, &acceptReply)
          acceptReplies.PushBack(acceptReply)
        } else {
          ok := call(px.peers[peer], "Paxos.Accept", acceptArgs, &acceptReply)
          if ok {
            acceptReplies.PushBack(acceptReply)
          }
        }
        
      }
      
      var acceptOkCount = 0
      for e := acceptReplies.Front(); e != nil; e = e.Next() {
        var reply = e.Value.(AcceptReply)
        if reply.Ok {
          acceptOkCount += 1
        } else {
          nextProposal = Max(nextProposal, reply.SuggestedNextProposal)
        }
      }
      
      if acceptOkCount > agreementThreshold {
        // We got agreement on the decided value. Send it out...
        
        var decidedArgs = &DecidedArgs{instance, maxAcceptedProposalValue}
        var decidedReply DecidedReply
        
        for peer := range px.peers {
          if peer == px.me {
            px.Decided(decidedArgs, &decidedReply)
          } else {
            call(px.peers[peer], "Paxos.Decided", decidedArgs, &decidedReply) 
          }
        }
        
        break
      }
    }
    
    time.Sleep(10 * time.Millisecond) 
  }
}

func (px *Paxos) GetInstance(seq int) *Instance {
  var instance, found = px.instances[seq]
  if !found {
    px.instances[seq] = NewInstance()
    instance = px.instances[seq]
  }
  
  return &instance
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  px.maxReportedDones[args.Me] = Max(px.maxReportedDones[args.Me], args.MaxDone)
  
  var min = px.Min()
  for instance := range(px.instances) {
    if instance < min {
      delete(px.instances, instance)
    }
  }
  
  var instance = px.GetInstance(args.Instance)
  if args.Proposal > instance.maxPromisedProposal {
    instance.maxPromisedProposal = args.Proposal
    
    px.instances[args.Instance] = *instance
    
    reply.MaxAcceptedProposal = instance.maxAcceptedProposal
    reply.Value = instance.value
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  var instance = px.GetInstance(args.Instance)
  if args.Proposal >= instance.maxPromisedProposal {
    instance.maxPromisedProposal = args.Proposal
    instance.maxAcceptedProposal = args.Proposal
    instance.value = args.Value
    
    px.instances[args.Instance] = *instance
    
    reply.Ok = true
  } else {
    reply.Ok = false
  }
  
  return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  var i = px.GetInstance(args.Instance)
  i.value = args.Value
  i.agreementReached = true
  px.instances[args.Instance] = *i
  
  reply.Ok = true
  return nil
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  if seq < px.Min() {
    return
  }
  
  var instance = NewInstance()
  px.instances[seq] = instance
  
  go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  px.maxReportedDones[px.peers[px.me]] = Max(px.maxReportedDones[px.peers[px.me]], seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  var seq = -1
  for key := range(px.instances) {
    seq = Max(seq, key)
  }
    
  return seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  var min = px.maxReportedDones[px.peers[px.me]]
  for peer := range(px.maxReportedDones) {
    min = Min(px.maxReportedDones[peer], min)
  }
  
  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peers state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  if seq < px.Min() {
    return false, nil
  }
  
  var instance = px.GetInstance(seq)
  
  //fmt.Printf("Status Of: %v. Agreement: %v. Value: %v.\n", px.peers[px.me], instance.agreementReached, instance.value)
  
  return instance.agreementReached, instance.value
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  
  // Start Salman Addition
  px.instances = make(map[int]Instance)
  px.maxReportedDones = make(map[string]int)
  
  for peer := range(px.peers) {
    px.maxReportedDones[px.peers[peer]] = -1
  }
    
  // End Salman Addition

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
