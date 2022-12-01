package gryffproto

import (
	"state"
  "fastrpc"
)

type ValTag struct {
  V state.Value
  T Tag
}

type Tag struct {
  Ts int32
  Cid int32
  Rmwc int32
}

type Dep struct {
  Key state.Key
  Vt ValTag
}

type Read1 struct {
  RequestId int32
  ClientId int32
  K state.Key
  D Dep
}

type Read1Proxied struct {
  RequestId int32
  ClientId int32
  K state.Key
  D Dep
  ReplicaId int32
  Vt ValTag
}

type Read1Reply struct {
  RequestId int32
  ClientId int32
  ReplicaId int32
  Vt ValTag
}

type Read2 struct {
  RequestId int32
  ClientId int32
  K state.Key
  Vt ValTag
  UpdateId int64
}

type Read2Proxied struct {
  RequestId int32
  ClientId int32
  K state.Key
  Vt ValTag
  ReplicaId int32
  UpdateId int64
}


type Read2Reply struct {
  RequestId int32
  ClientId int32
  ReplicaId int32
  T Tag
}

type Read struct {
  RequestId int32
  ClientId int32
  K state.Key
  D Dep
}

type ReadReply struct {
  RequestId int32
  ClientId int32
  V state.Value
  MaxSeenTag Tag
  OK uint8
}

type Write struct {
  RequestId int32
  ClientId int32
  K state.Key
  V state.Value
  D Dep
}

type WriteReply struct {
  RequestId int32
  ClientId int32
  MaxSeenTag Tag
  OK uint8
}

type Write1 struct {
  RequestId int32
  ClientId int32
  N int32
  ForceWrite int32
  K state.Key
  V state.Value
  D Dep
  ReplicaId int32
  MaxAppliedTag Tag
}

type Write1Proxied struct {
  RequestId int32
  ClientId int32
  N int32
  ForceWrite int32
  K state.Key
  V state.Value
  D Dep
  ReplicaId int32
  MaxAppliedTag Tag
}

type Write1Reply struct {
  RequestId int32
  ClientId int32
  ReplicaId int32
  Vt ValTag
}

type Write2 struct {
  RequestId int32
  ClientId int32
  K state.Key
  Vt ValTag
}

type Write2Proxied struct {
  RequestId int32
  ClientId int32
  N int32
  K state.Key
  Vt ValTag
  ReplicaId int32
}

type Write2Reply struct {
  RequestId int32
  ClientId int32
  ReplicaId int32
  T Tag
  Applied int32
}

type RMW struct {
  RequestId int32
  ClientId int32
  K state.Key
  D Dep
  OldValue state.Value
  NewValue state.Value
}

type RMWReply struct {
  RequestId int32
  ReplicaId int32
  OldValue state.Value
  OK uint8
}

type Proposal struct {
  RequestId int32
  ClientId int32
  Ballot int32
  K state.Key
  NewValue state.Value
  T Tag
  OldValue state.Value
}

type Prepare struct {
  ClientRequestId int64
  CoordinatorRequestId int64
  CoordinatorId int32
  K state.Key
  D Dep
  Ballot int32
}

type Promise struct {
  ClientRequestId int64
  CoordinatorRequestId int64
  ReplicaId int32
  K state.Key
  BallotPromised int32
  AcceptedProposal Proposal
  V state.Value
  T Tag
  RequestCommitted uint8
}

type Propose struct {
  ClientRequestId int64
  CoordinatorRequestId int64
  CoordinatorId int32
  K state.Key
  Ballot int32
  Prop Proposal
}

type Accept struct {
  ClientRequestId int64
  CoordinatorRequestId int64
  ReplicaId int32
  K state.Key
  BallotPromised int32
}

type Commit struct {
  ClientRequestId int64
  CoordinatorRequestId int64
  CoordinatorId int32
  K state.Key
  Vt ValTag
  Prop Proposal
}

type CommitReply struct {
  ClientRequestId int64
  CoordinatorRequestId int64
  ReplicaId int32
  K state.Key
}

/* EPaxos Proto */
type EPrepare struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
}

type PrepareReply struct {
	AcceptorId int32
	Replica    int32
	Instance   int32
	OK         uint8
	Ballot     int32
	Status     int8
	Command    []state.Command
	Seq        int32
	Deps       [5]int32
  Base       ValTag
}

type PreAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Seq      int32
	Deps     [5]int32
  Base     ValTag
}

type PreAcceptReply struct {
	Replica       int32
	Instance      int32
	OK            uint8
	Ballot        int32
	Seq           int32
	Deps          [5]int32
	CommittedDeps [5]int32
  Base          ValTag
}

type PreAcceptOK struct {
	Instance int32
}

type PreAcceptOKReply struct {
  Replica int32
  Instance int32
}

type EAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Count    int32
	Seq      int32
	Deps     [5]int32
  Base     ValTag
}

type AcceptReply struct {
	Replica  int32
	Instance int32
	OK       uint8
	Ballot   int32
}

type AcceptOKReply struct {
  Replica int32
  Instance int32
}

type ECommit struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Command  []state.Command
	Seq      int32
	Deps     [5]int32
  Base     ValTag
}

type CommitShort struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Count    int32
	Seq      int32
	Deps     [5]int32
}

type TryPreAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Seq      int32
	Deps     [5]int32
  Base     ValTag
}

type TryPreAcceptReply struct {
	AcceptorId       int32
	Replica          int32
	Instance         int32
	OK               uint8
	Ballot           int32
	ConflictReplica  int32
	ConflictInstance int32
	ConflictStatus   int8
}

type Executed struct {
  Replica int32
  Instance int32
}

func (t *PreAccept) New() fastrpc.Serializable {
	return new(PreAccept)
}

func (t *PreAcceptReply) New() fastrpc.Serializable {
	return new(PreAcceptReply)
}

func (t *EAccept) New() fastrpc.Serializable {
	return new(EAccept)
}

func (t *ECommit) New() fastrpc.Serializable {
	return new(ECommit)
}

func (t *PrepareReply) New() fastrpc.Serializable {
	return new(PrepareReply)
}

func (t *TryPreAccept) New() fastrpc.Serializable {
	return new(TryPreAccept)
}

func (t *Executed) New() fastrpc.Serializable {
  return new(Executed)
}

func (t *PreAcceptOKReply) New() fastrpc.Serializable {
  return new(PreAcceptOKReply)
}

func (t *AcceptOKReply) New() fastrpc.Serializable {
  return new(AcceptOKReply)
}

/* End EPaxos Proto */

func (a *Tag) Compare(b Tag) int {
  if a.Ts < b.Ts || a.Ts == b.Ts && a.Cid < b.Cid || a.Ts == b.Ts && a.Cid == b.Cid &&
      a.Rmwc < b.Rmwc {
    return -1
  } else if a.Ts == b.Ts && a.Cid == b.Cid && a.Rmwc == b.Rmwc {
    return 0
  } else {
    return 1
  }
}

func (a *Tag) GreaterThan(b Tag) bool {
  return a.Compare(b) > 0
}

func (a *Tag) GreaterThanOrEqual(b Tag) bool {
  return a.Compare(b) >= 0
}

func (a *Tag) LessThan(b Tag) bool {
  return a.Compare(b) < 0
}

func (a *Tag) Equals(b Tag) bool {
  return a.Compare(b) == 0
}

func (t *Read1) New() fastrpc.Serializable {
	return new(Read1)
}

func (t *Read1Proxied) New() fastrpc.Serializable {
	return new(Read1Proxied)
}

func (t *Read2) New() fastrpc.Serializable {
	return new(Read2)
}

func (t *Read2Proxied) New() fastrpc.Serializable {
	return new(Read2Proxied)
}

func (t *Read1Reply) New() fastrpc.Serializable {
	return new(Read1Reply)
}

func (t *Read2Reply) New() fastrpc.Serializable {
	return new(Read2Reply)
}

func (t *Write1) New() fastrpc.Serializable {
	return new(Write1)
}

func (t *Write1Proxied) New() fastrpc.Serializable {
	return new(Write1Proxied)
}

func (t *Write2) New() fastrpc.Serializable {
	return new(Write2)
}

func (t *Write2Proxied) New() fastrpc.Serializable {
	return new(Write2Proxied)
}

func (t *Write1Reply) New() fastrpc.Serializable {
	return new(Write1Reply)
}

func (t *Write2Reply) New() fastrpc.Serializable {
	return new(Write2Reply)
}

func (t *RMW) New() fastrpc.Serializable {
	return new(RMW)
}

func (t *RMWReply) New() fastrpc.Serializable {
	return new(RMWReply)
}

func (t *Prepare) New() fastrpc.Serializable {
	return new(Prepare)
}

func (t *Promise) New() fastrpc.Serializable {
	return new(Promise)
}

func (t *Propose) New() fastrpc.Serializable {
	return new(Propose)
}

func (t *Accept) New() fastrpc.Serializable {
	return new(Accept)
}

func (t *Commit) New() fastrpc.Serializable {
	return new(Commit)
}

func (t *CommitReply) New() fastrpc.Serializable {
	return new(CommitReply)
}

func (t *Write) New() fastrpc.Serializable {
  return new(Write)
}

func (t *WriteReply) New() fastrpc.Serializable {
  return new(WriteReply)
}

func (t *Read) New() fastrpc.Serializable {
  return new(Read)
}

func (t *ReadReply) New() fastrpc.Serializable {
  return new(ReadReply)
}
