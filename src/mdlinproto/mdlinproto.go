package mdlinproto

import (
	"state"
)

const (
	PREPARE uint8 = iota
	PREPARE_REPLY
	PROPOSE
	PROPOSE_REPLY
	ACCEPT
	ACCEPT_REPLY
	COMMIT
	COMMIT_SHORT
	INTERSHARD
	INTERSHARD_REPLY
)

type Tag struct {
	K         state.Key
	Version   state.Version
	PID       int64
	CommandId int32
	//Instance  int32 // TODO this is an optimization!! Index in the log, comes from crtInstance
}

type Propose struct {
	CommandId int32
	Command   state.Command
	Timestamp int64
	SeqNo     int64 // Add these for multidispatch
	PID       int64
	BatchDeps []Tag
}

type ProposeReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
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
	PIDs         int64
	SeqNos       int64
	ExpectedSeqs map[int64]int64
	Versions     state.Version
	BatchDeps    []Tag
}

type AcceptReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
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
	Status uint8
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
	Status int32
}

// Message types for MultiShard MDL
type InterShard struct {
	AskerInstance  int32
	AskeeCommandId int32
}

type InterShardReply struct {
	AskerInstance   int32
	AskeeCommandId  int32
	LogDependencies []Tag
}

type Reorder struct {
	LeaderId    int32
	OldInstance int32
	NewInstance int32
}

type ReorderReply struct {
	OldInstance int32
	OK       uint8
}
