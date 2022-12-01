package abdproto

import (
	"state"
  "fastrpc"
)

type Timestamp struct {
  Ts int32
  Cid int32
}

type Read struct {
  RequestId int32
  K state.Key
}

type ReadReply struct {
  RequestId int32
  ReplicaId int32
  V state.Value
  Ts Timestamp
  OK uint8
}

type Write struct {
  RequestId int32
  K state.Key
  V state.Value
  Ts Timestamp
}

type WriteReply struct {
  RequestId int32
  ReplicaId int32
  OK uint8
}

func (a *Timestamp) Compare(b Timestamp) int {
  if a.Ts < b.Ts || a.Ts == b.Ts && a.Cid < b.Cid {
    return -1;
  } else if a.Ts == b.Ts && a.Cid == b.Cid {
    return 0;
  } else {
    return 1;
  }
}

func (a *Timestamp) GreaterThan(b Timestamp) bool {
  return a.Compare(b) > 0
}

func (a *Timestamp) LessThan(b Timestamp) bool {
  return a.Compare(b) < 0
}

func (a *Timestamp) Equals(b Timestamp) bool {
  return a.Compare(b) == 0
}

func (t *Read) New() fastrpc.Serializable {
	return new(Read)
}

func (t *Write) New() fastrpc.Serializable {
	return new(Write)
}

func (t *ReadReply) New() fastrpc.Serializable {
	return new(ReadReply)
}

func (t *WriteReply) New() fastrpc.Serializable {
	return new(WriteReply)
}
