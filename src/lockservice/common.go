package lockservice

//
// RPC definitions for a simple lock service.
//
// You will need to modify this file.
//

//
// Lock(lockname) returns OK=true if the lock is not held.
// If it is held, it returns OK=false immediately.
// 
type LockArgs struct {
  // Go's net/rpc requires that these field
  // names start with upper case letters!
  Lockname string  // lock name
  RequestId string  // the client uuid
}

type LockReply struct {
  OK bool
}

//
// Unlock(lockname) returns OK=true if the lock was held.
// It returns OK=false if the lock was not held.
//
type UnlockArgs struct {
  Lockname string
  RequestId string
}

type UnlockReply struct {
  OK bool
}
