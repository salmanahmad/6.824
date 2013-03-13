package kvpaxos

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

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  
  // Begin Salman Additions
  Id string
  // End Salman Additions
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
  
  // Begin Salman Additions
  Id string
  // End Salman Additions
}

type GetReply struct {
  Err Err
  Value string
}


