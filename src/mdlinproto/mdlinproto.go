package mdlinproto

import (
	"state"
  "io"
)

const (
	COORDREQ_REPLY = iota
)

type Tag struct {
	K state.Key
	PID int64
	SeqNo int64
	//Instance  int32 // TODO this is an optimization!! Index in the log, comes from crtInstance
}

type Propose struct {
	CommandId int32
	Command   state.Command // state.key in here
	Timestamp int64
	SeqNo     int64 // Add these for multidispatch
	PID       int64
  Predecessor Tag
}

type ProposeReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
}

type Prepare struct {
	LeaderId   int32
	Ballot     int32
	ToInfinity uint8
	Instance   []Tag
}

type PrepareReply struct {
	Instance []Tag
  Ballot   int32
	OK       uint8
	Command  []state.Command
}

type Accept struct {
	LeaderId     int32
	Ballot       int32
	Command      []state.Command
  PIDs         []int64
	SeqNos       []int64
	ExpectedSeqs map[int64]int64
  Epoch        int64
  CommandId    []int32
}

type AcceptReply struct {
	OK         uint8
	Ballot     int32
  IdTag      []Tag
}

type FinalAccept struct {
  LeaderId  int32
  Instance  int32
  Ballot    int32
  CmdTags   []Tag
  Command []state.Command
  PIDs []int64
  SeqNos []int64
  ExpectedSeqs  map[int64]int64
  EpochSize []int64
}

type FinalAcceptReply struct {
  Instance  int32
  OK        uint8
  Ballot    int32
}

type OldAccept struct {
  LeaderId     int32
  Instance     int32
  Ballot       int32
  Command      []state.Command
  PIDs         []int64
  SeqNos       []int64
  ExpectedSeqs map[int64]int64
}

type Commit struct {
	LeaderId  int32
	Instance  int32
	Ballot    int32
	Command   []state.Command
	PIDs      int64
	SeqNos    int64
	Status    uint8
  EpochSize []int64
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
	Status   int32
}

// Message types for MultiShard MDL
type CoordinationRequest struct {
	AskerTag      Tag
	AskeeTag      Tag
	From          int32
}

type CoordinationResponse struct {
	AskerTag        Tag
	AskeeTag        Tag
  AskeeEpoch      int64
	From            int32
  OK              uint8
}

// Marshalling and Unmarshalling helpers for Tag type
func (t *Tag) Marshal(w io.Writer) {
  var b [8]byte

  t.K.Marshal(w)
  bs := b[:8]
	tmp64 := t.PID
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	w.Write(bs)

	bs = b[:8]
	tmp64 = t.SeqNo
	bs[0] = byte(tmp64)
	bs[1] = byte(tmp64 >> 8)
	bs[2] = byte(tmp64 >> 16)
	bs[3] = byte(tmp64 >> 24)
	bs[4] = byte(tmp64 >> 32)
	bs[5] = byte(tmp64 >> 40)
	bs[6] = byte(tmp64 >> 48)
	bs[7] = byte(tmp64 >> 56)
	w.Write(bs)
}

func (t *Tag) Unmarshal(w io.Reader) error {
  t.K.Unmarshal(w)

  var b [8]byte
  bs := b[:8]
  if _, err := io.ReadAtLeast(w, bs, 8); err != nil {
    return err
  }
  t.PID = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))

  bs = b[:8]
  if _, err := io.ReadAtLeast(w, bs, 8); err != nil {
    return err
  }
  t.SeqNo = int64((uint64(bs[0]) | (uint64(bs[1]) << 8) | (uint64(bs[2]) << 16) | (uint64(bs[3]) << 24) | (uint64(bs[4]) << 32) | (uint64(bs[5]) << 40) | (uint64(bs[6]) << 48) | (uint64(bs[7]) << 56)))
  return nil
}
