package pbservice

import "viewservice"
import "net/rpc"
import "time"
import "fmt"

type Clerk struct {
  vs *viewservice.Clerk
  view viewservice.View
}

func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  ck.view, _ = ck.vs.Get()

  return ck
}

const DEBUG bool = false

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
  args := &GetArgs{}
  args.Key = key
  var reply GetReply
  
  for {
    
    if DEBUG {
      fmt.Printf("Sending Get Request to: %s\n", ck.view.Primary)
    }
    
    ok := call(ck.view.Primary, "PBServer.Get", args, &reply)
    if !ok || reply.Err != OK {
      ck.view, _ = ck.vs.Get()
      
      
      if DEBUG {
        fmt.Printf("Reply: %s\n", reply.Err)
      }
      
      time.Sleep(viewservice.PingInterval)
      
    } else {
      break
    }
  }
  
  return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
  args := &PutArgs{}
  args.Key = key
  args.Value = value
  var reply PutReply
  
  for {
    
    if DEBUG {
      fmt.Printf("Sending ForwardPut Request\n") 
    }
    
    ok := call(ck.view.Primary, "PBServer.Put", args, &reply)
    if !ok || reply.Err != OK {
      ck.view, _ = ck.vs.Get()
      time.Sleep(viewservice.PingInterval)
    } else {
      break
    }
  }
}


func (ck *Clerk) ForwardPut(theArgs *PutArgs) {
  args := &PutArgs{}
  args.Key = theArgs.Key
  args.Value = theArgs.Value
  var reply PutReply
  
  for {
    if ck.view.Backup == "" {
      break
    }
    
    if DEBUG {
      fmt.Printf("Sending ForwardPut Request\n")
      fmt.Printf("ForwardPut: %s\n", ck.view.Backup)
    }
    
    ok := call(ck.view.Backup, "PBServer.ForwardPut", args, &reply)
    if !ok || reply.Err != OK {
      ck.view, _ = ck.vs.Get()
      time.Sleep(viewservice.PingInterval)
    } else {
      break
    }
  }
}

func (ck *Clerk) SetData(data map[string]string) {
  args := &SetDataArgs{}
  args.Data = data
  var reply SetDataReply
  
  for {
    var server = ck.view.Backup
    
    if DEBUG {
      fmt.Printf("Sending SetData Request\n") 
    }
    
    ok := call(server, "PBServer.SetData", args, &reply)
    if !ok || reply.Err != OK {
      ck.view, _ = ck.vs.Get()
      time.Sleep(viewservice.PingInterval)
    } else {
      break
    }
  }
}

