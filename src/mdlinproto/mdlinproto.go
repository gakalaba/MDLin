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
	INTERSHARD_TEST
)

type Propose struct {
	CommandId int32
	Command   state.Command
	Timestamp int64
	SeqNo     int64 // Add this for multidispatch
	PID       int64
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
	PIDs         []int64
	SeqNos       []int64
	ExpectedSeqs map[int64]int64
}

type AcceptReply struct {
	Instance int32
	OK       uint8
	Ballot   int32
}

type Commit struct {
	LeaderId int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	PIDs     []int64
	SeqNos   []int64
}

type CommitShort struct {
	LeaderId int32
	Instance int32
	Count    int32
	Ballot   int32
}

type InterShardTest struct {
	TestMessage int32
}
