package lockservice

import "net/rpc"


// START NOT MY CODE
// Taken from: http://www.ashishbanerjee.com/home/go/go-generate-uuid
import "encoding/hex"
import "crypto/rand"

func GenUUID() (string, error) {
  uuid := make([]byte, 16)
  n, err := rand.Read(uuid)
  if n != len(uuid) || err != nil {
    return "", err
  }
  
  // TODO: verify the two lines implement RFC 4122 correctly
  uuid[8] = 0x80 // variant bits see page 5
  uuid[4] = 0x40 // version 4 Pseudo Random, see page 7

  return hex.EncodeToString(uuid), nil
}
// END NOT MY CODE






//
// the lockservice Clerk lives in the client
// and maintains a little state.
//
type Clerk struct {
  servers [2]string // primary port, backup port
  uuid string
}

func MakeClerk(primary string, backup string) *Clerk {
  ck := new(Clerk)
  ck.servers[0] = primary
  ck.servers[1] = backup
  ck.uuid, _ = GenUUID();

  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// call() returns true if the server responded, and false
// if call() was not able to contact the server. in particular,
// reply's contents are valid if and only if call() returned true.
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
// ask the lock service for a lock.
// returns true if the lock service
// granted the lock, false otherwise.
//
func (ck *Clerk) LockUsingRequest(lockname string, requestId string) bool {
  // prepare the arguments.
  args := &LockArgs{}
  args.Lockname = lockname
  args.RequestId = requestId
  var reply LockReply
  
  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Lock", args, &reply)
  if ok == false {
    ok := call(ck.servers[1], "LockServer.Lock", args, &reply)
    if ok == false {
      return false
    }
  }
  
  return reply.OK
}


func (ck *Clerk) Lock(lockname string) bool {
  var id string = ""
  id, _ = GenUUID()
  return ck.LockUsingRequest(lockname, id)
}


//
// ask the lock service to unlock a lock.
// returns true if the lock was previously held,
// false otherwise.
//
func (ck *Clerk) UnlockUsingRequest(lockname string, requestId string) bool {
  // prepare the arguments.
  args := &UnlockArgs{}
  args.Lockname = lockname
  args.RequestId = requestId
  var reply UnlockReply
  
  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Unlock", args, &reply)
  if ok == false {
    ok := call(ck.servers[1], "LockServer.Unlock", args, &reply)
    if ok == false {
      return false
    }
  }
  
  return reply.OK
}

func (ck *Clerk) Unlock(lockname string) bool {
  var id string = ""
  id, _ = GenUUID()
  return ck.UnlockUsingRequest(lockname, id)
}



