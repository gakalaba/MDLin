package clientproto

import (
	"fastrpc"
)

const (
	GEN_PROPOSE uint8 = iota
	GEN_PROPOSE_REPLY
	GEN_READ
	GEN_READ_REPLY
	GEN_PROPOSE_AND_READ
	GEN_PROPOSE_AND_READ_REPLY
	GEN_GENERIC_SMR_BEACON
	GEN_GENERIC_SMR_BEACON_REPLY

	GEN_PING
	GEN_PING_REPLY

	ABD_READ
	ABD_READ_REPLY
	ABD_WRITE
	ABD_WRITE_REPLY

	Gryff_READ_1
	Gryff_READ_1_REPLY
	Gryff_READ_2
	Gryff_READ_2_REPLY
	Gryff_WRITE_1
	Gryff_WRITE_1_REPLY
	Gryff_WRITE_2
	Gryff_WRITE_2_REPLY
	Gryff_RMW
	Gryff_RMW_REPLY
	Gryff_READ
	Gryff_READ_REPLY
	Gryff_WRITE
	Gryff_WRITE_REPLY

	MDL_PROPOSE
	MDL_PROPOSE_REPLY
  MDL_COORDREQ
)

type Ping struct {
	ClientId int32
	Ts       uint64
}

type PingReply struct {
	ReplicaId int32
	Ts        uint64
}

func (t *Ping) New() fastrpc.Serializable {
	return new(Ping)
}

func (t *PingReply) New() fastrpc.Serializable {
	return new(PingReply)
}
