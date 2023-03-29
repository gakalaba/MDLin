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
  coordinationReq  *mdlinproto.CoordinationRequest
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
    new(mdlinproto.CoordinationRequest), // coordinationReq
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
  var prevTag mdlinproto.Tag
  readPrefix := true // track the contiguous prefix of reads
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
		c.setSeqno(c.seqnos[l])
    c.setPredSize(int32(i))
    if (i == 0) {
      c.propose.Predecessor = mdlinproto.Tag{K: state.Key(-1), PID: int64(-1), SeqNo: -1}
    } else {
      c.propose.Predecessor = prevTag
    }
		if opType == state.GET {
      if (readPrefix) {
        c.propose.Predecessor = mdlinproto.Tag{K: state.Key(-1), PID: int64(-1), SeqNo: -1}
        c.setPredSize(0) // reads should just be sorted based on arrival order in the epoch
      }
			c.Read(k)
		} else if opType == state.PUT {
			c.Write(k, int64(k))
      readPrefix = false
		} else {
			c.CompareAndSwap(k, int64(k-1), int64(k))
      readPrefix = false
		}
    if (i > 0 && !readPrefix) {
      // Send the coordination request
      // (Keep this after sending the request for now, since 
      // logic in the send function assumes request was send)
      c.sendCoordinationRequest(prevTag, l)
    }
    prevTag = mdlinproto.Tag{K: state.Key(k), PID: int64(c.id), SeqNo: c.seqnos[l]}
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
  dlog.Println("Propose{CommandId %v, Command %v, Timestamp %v, SeqNo %v, PID %v, Predecessor %v, PredSize %v}", c.propose.CommandId, c.propose.Command, c.propose.Timestamp, c.propose.SeqNo, c.propose.PID, c.propose.Predecessor, c.propose.PredSize)
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

func (c *MDLClient) setSeqno(seqno int64) {
	c.propose.SeqNo = seqno
}

func (c *MDLClient) setPredSize(n int32) {
  c.propose.PredSize = n
}

func (c *MDLClient) sendPropose() {
	shard := c.GetShardFromKey(c.propose.Command.K)
	dlog.Printf("Sending request to shard %d\n", shard)

	c.writers[shard].WriteByte(clientproto.MDL_PROPOSE)
	c.propose.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

func (c *MDLClient) sendCoordinationRequest(predecessorTag mdlinproto.Tag, myShard int) {
	shard := c.GetShardFromKey(predecessorTag.K)
  if (shard == myShard) {
    //TODO remove this
    dlog.Printf("Not sending CoordinationRequest to same shard as predecessor")
    return
  }
	dlog.Printf("Sending CoordinationRequest to shard %d\n", shard)

  // Prepare the coordination request object
  c.coordinationReq.AskerTag = mdlinproto.Tag{K: c.propose.Command.K, PID: int64(c.id), SeqNo: c.propose.SeqNo}
  c.coordinationReq.AskeeTag = predecessorTag
  c.coordinationReq.From = int32(c.GetShardFromKey(c.propose.Command.K))

	c.writers[shard].WriteByte(clientproto.MDL_COORDREQ)
	c.coordinationReq.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

// func (c *MDLClient) sendProposeAndReadReply() (bool, int64) {
// 	c.sendPropose()
// 	return c.readProposeReply(c.propose.CommandId)
// }

func (c *MDLClient) readReplies(start int32, fanout int) (bool, int64) {
	rarray := make([]int, fanout)
	done := false
  dlog.Println("Client completed AppRequest, now is awaiting results from leader")
	for !done {
		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
		if reply.OK == 0 {
      dlog.Println("Client received FAIL response for request")
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
