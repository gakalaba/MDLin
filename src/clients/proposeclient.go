package clients

import (
	"clientproto"
	"dlog"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"state"
)

type ProposeClient struct {
	*AbstractClient
	proposeReplyChan chan fastrpc.Serializable
	propose          *genericsmrproto.Propose
	opCount          int32
	fast             bool
	noLeader         bool
}

func NewProposeClient(id int32, masterAddr string, masterPort int, forceLeader int, statsFile string,
	fast bool, noLeader bool) *ProposeClient {
	pc := &ProposeClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE), // proposeReplyChan
		new(genericsmrproto.Propose),                                 // propose
		0,                                                            // opCount
		fast,                                                         // fast
		noLeader,                                                     // noLeader
	}
	pc.RegisterRPC(new(genericsmrproto.ProposeReplyTS), clientproto.GEN_PROPOSE_REPLY,
		pc.proposeReplyChan)
	return pc
}

func (c *ProposeClient) AppRequest(opTypes []state.Operation, keys []int64) (bool, int64) {
	for i, opType := range opTypes {
		k := keys[i]

		var success bool
		if opType == state.GET {
			success, _ = c.Read(k)
		} else if opType == state.PUT {
			success = c.Write(k, int64(k))
		} else {
			success, _ = c.CompareAndSwap(k, int64(k-1), int64(k))
		}

		if !success {
			return false, -1
		}
	}

	return true, 0
}

func (c *ProposeClient) Read(key int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, 0)
	c.propose.Command.Op = state.GET
	return c.sendProposeAndReadReply()
}

func (c *ProposeClient) Write(key int64, value int64) bool {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = state.PUT
	success, _ := c.sendProposeAndReadReply()
	return success
}

func (c *ProposeClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, newValue)
	c.propose.Command.OldValue = state.Value(newValue)
	c.propose.Command.Op = state.CAS
	return c.sendProposeAndReadReply()
}

func (c *ProposeClient) preparePropose(commandId int32, key int64, value int64) {
	c.propose.CommandId = commandId
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = state.Value(value)
}

func (c *ProposeClient) sendProposeAndReadReply() (bool, int64) {
	c.sendPropose()
	return c.readProposeReply(c.propose.CommandId)
}

func (c *ProposeClient) sendPropose() {
	if !c.fast {
		replica := int32(c.leader)
		if c.noLeader {
			if c.forceLeader >= 0 {
				replica = int32(c.forceLeader)
			} else {
				replica = c.replicasByPingRank[0]
			}
		}
		dlog.Printf("Sending request to %d\n", replica)
		c.writers[replica].WriteByte(clientproto.GEN_PROPOSE)
		c.propose.Marshal(c.writers[replica])
		c.writers[replica].Flush()
	} else {
		dlog.Printf("Sending request to all replicas\n")
		for i := 0; i < c.numLeaders; i++ {
			c.writers[i].WriteByte(clientproto.GEN_PROPOSE)
			c.propose.Marshal(c.writers[i])
			c.writers[i].Flush()
		}
	}
}

func (c *ProposeClient) readProposeReply(commandId int32) (bool, int64) {
	for !c.shutdown {
		reply := (<-c.proposeReplyChan).(*genericsmrproto.ProposeReplyTS)
		if reply.OK == 0 {
			return false, 0
		} else {
			dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
			if commandId == reply.CommandId {
				return true, int64(reply.Value)
			}
		}
	}
	return false, 0
}
