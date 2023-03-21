package mdlinproto

import (
	"state"
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
	NumConf   int64
}

type Prepare struct {
	LeaderId   int32
	Instance   int32
	Ballot     int32
	ToInfinity uint8
}

type PrepareReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
	Command  []state.Command
}

type Accept struct {
	LeaderId     int32
	Instance     int32
	Ballot       int32
	Command      []state.Command
  FinalRound   bool
  Epoch        int32

	//PIDs         int64
	//SeqNos       int64
	//ExpectedSeqs map[int64]int64
}

type AcceptReply struct {
	Instance   int32
	OK         uint8
	Ballot     int32
  FinalRound bool
  Epoch      int32
}

type Commit struct {
	LeaderId  int32
	Instance  int32
	Ballot    int32
	Command   []state.Command
	PIDs      int64
	SeqNos    int64
	Versions  state.Version
	BatchDeps []Tag
	Status    uint8
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
	From            int32
  OK              uint8
}

