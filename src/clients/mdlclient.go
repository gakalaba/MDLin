package clients

import (
	"clientproto"
	"dlog"
	"fastrpc"
	"genericsmr"
	"mdlinproto"
	"state"
)

type MDLClient struct {
	*AbstractClient
	proposeReplyChan chan fastrpc.Serializable
	propose          *mdlinproto.Propose
	opCount          int32
	fast             bool
	noLeader         bool
	seqnos           map[int]int
}

func NewMDLClient(id int32, masterAddr string, masterPort int, forceLeader int, statsFile string,
	fast bool, noLeader bool) *MDLClient {
	pc := &MDLClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE), // proposeReplyChan
		new(mdlinproto.Propose), // propose
		0,                       // opCount
		fast,                    // fast
		noLeader,                // noLeader
		make(map[int]int),
	}
	pc.RegisterRPC(new(mdlinproto.ProposeReply), clientproto.MDL_PROPOSE_REPLY, pc.proposeReplyChan)

	return pc
}

func (c *MDLClient) AppRequest(opTypes []state.Operation, keys []int64) (bool, int64) {
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

func (c *MDLClient) Read(key int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, 0)
	c.propose.Command.Op = state.GET
	return c.sendProposeAndReadReply()
}

func (c *MDLClient) Write(key int64, value int64) bool {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = state.PUT
	success, _ := c.sendProposeAndReadReply()
	return success
}

func (c *MDLClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, newValue)
	c.propose.Command.OldValue = state.Value(newValue)
	c.propose.Command.Op = state.CAS
	return c.sendProposeAndReadReply()
}

func (c *MDLClient) preparePropose(commandId int32, key int64, value int64) {
	c.propose.CommandId = commandId
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = state.Value(value)
}

func (c *MDLClient) sendProposeAndReadReply() (bool, int64) {
	c.sendPropose()
	return c.readProposeReply(c.propose.CommandId)
}

func (c *MDLClient) sendPropose() {
	shard := c.GetShardFromKey(c.propose.Command.K)
	dlog.Printf("Sending request to shard %d\n", shard)

	c.writers[shard].WriteByte(clientproto.MDL_PROPOSE)
	c.propose.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

func (c *MDLClient) readProposeReply(commandId int32) (bool, int64) {
	for !c.shutdown {
		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
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
