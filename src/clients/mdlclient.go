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
	seqnos           map[int]int64
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
		make(map[int]int64),
	}
	pc.propose.PID = int64(id) // only need to set this once per client
	pc.RegisterRPC(new(mdlinproto.ProposeReply), clientproto.MDL_PROPOSE_REPLY, pc.proposeReplyChan)

	return pc
}

func (c *MDLClient) AppRequest(opTypes []state.Operation, keys []int64) (bool, int64) {
	fanout := len(keys)
	start := c.opCount
	batchDeps := make([]mdlinproto.Tag, 0)
	for i, opType := range opTypes {
		k := keys[i]
		l := c.GetShardFromKey(state.Key(k))
		// Figure out the sequence number
		if _, ok := c.seqnos[l]; !ok {
			c.seqnos[l] = 0
		} else {
			c.seqnos[l]++
		}
		// Assign the sequence number and batch dependencies for this request
		c.setSeqnoBd(c.seqnos[l], c.filterdeps(batchDeps, state.Key(k)))
		if opType == state.GET {
			c.Read(k)
		} else if opType == state.PUT {
			c.Write(k, int64(k))
		} else {
			c.CompareAndSwap(k, int64(k-1), int64(k))
		}
		batchDeps = append(batchDeps, mdlinproto.Tag{K: state.Key(k), PID: int64(c.id), SeqNo: c.seqnos[l]})
	}
	success, e := c.readReplies(start, fanout)
	if !success {
		dlog.Printf("ProposeReply for PID %d - CommandId %d return OK=False\n", c.id, e)
		return false, -1
	}

	return true, 0
}

func (c *MDLClient) Read(key int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, 0)
	c.propose.Command.Op = state.GET
	c.sendPropose()
	return true, 0
}

func (c *MDLClient) Write(key int64, value int64) bool {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = state.PUT
	c.sendPropose()
	return true
}

func (c *MDLClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, newValue)
	c.propose.Command.OldValue = state.Value(newValue)
	c.propose.Command.Op = state.CAS
	c.sendPropose()
	return true, 0
}

func (c *MDLClient) preparePropose(commandId int32, key int64, value int64) {
	c.propose.CommandId = commandId
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = state.Value(value)
}

func (c *MDLClient) setSeqnoBd(seqno int64, bd []mdlinproto.Tag) {
	c.propose.SeqNo = seqno
	c.propose.BatchDeps = bd
}

func (c *MDLClient) sendPropose() {
	shard := c.GetShardFromKey(c.propose.Command.K)
	dlog.Printf("Sending request to shard %d\n", shard)

	c.writers[shard].WriteByte(clientproto.MDL_PROPOSE)
	c.propose.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

// func (c *MDLClient) sendProposeAndReadReply() (bool, int64) {
// 	c.sendPropose()
// 	return c.readProposeReply(c.propose.CommandId)
// }

func (c *MDLClient) readReplies(start int32, fanout int) (bool, int64) {
	rarray := make([]int, fanout)
	done := false
	for !done {
		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
		if reply.OK == 0 {
			return false, int64(reply.CommandId)
		} else {
			dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
			rarray[reply.CommandId-start] = 1

			// Check if we have all replies
			done = true
			for i := 0; i < fanout; i++ {
				if rarray[i] == 0 {
					done = false
					break
				}
			}
		}
	}
	return true, int64(fanout)
}

// func (c *MDLClient) readProposeReply(commandId int32) (bool, int64) {
// 	for !c.shutdown {
// 		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
// 		if reply.OK == 0 {
// 			return false, 0
// 		} else {
// 			dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
// 			if commandId == reply.CommandId {
// 				return true, int64(reply.Value)
// 			}
// 		}
// 	}
// 	return false, 0
// }

func (c *MDLClient) filterdeps(deps []mdlinproto.Tag, k state.Key) []mdlinproto.Tag {
	result := make([]mdlinproto.Tag, 0)
	myLeader := c.GetShardFromKey(k)
	for _, d := range deps {
		dLeader := c.GetShardFromKey(d.K)
		if myLeader != dLeader {
			result = append(result, d)
		}
	}
	return result
}
