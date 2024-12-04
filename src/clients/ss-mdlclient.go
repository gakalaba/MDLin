package clients

import (
	"clientproto"
	"dlog"
	"fastrpc"
	"fmt"
	"genericsmr"
	"mdlinproto"
	"state"
	"time"
)

type SSMDLClient struct {
	*AbstractClient
	proposeReplyChan chan fastrpc.Serializable
	propose          *mdlinproto.Propose
	opCount          int32
	fast             bool
	noLeader         bool
	seqnos           map[int]int64
}

func NewSSMDLClient(id int32, masterAddr string, masterPort int, forceLeader int, statsFile string,
	fast bool, noLeader bool) *SSMDLClient {
	pc := &SSMDLClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE), // proposeReplyChan
		new(mdlinproto.Propose),             // propose
		0,                                   // opCount
		fast,                                // fast
		noLeader,                            // noLeader
		make(map[int]int64),
	}
	pc.propose.PID = int64(id) // only need to set this once per client
	pc.RegisterRPC(new(mdlinproto.ProposeReply), clientproto.MDL_PROPOSE_REPLY, pc.proposeReplyChan)

	return pc
}

func (c *SSMDLClient) AppRequest(opTypes []state.Operation, keys []int64, oldValue []int64, newValue []int64) (bool, int64) {
	fanout := len(keys)
	startTimes := make([]time.Time, fanout)
	startIdx := c.opCount
	var prevTag mdlinproto.Tag
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
		if i == 0 {
			c.propose.Predecessor = mdlinproto.Tag{K: state.Key(-1), PID: int64(-1), SeqNo: -1}
		} else {
			c.propose.Predecessor = prevTag
		}

		startTimes[i] = time.Now()

		if opType == state.GET {
			c.Read(k)
		} else if opType == state.PUT {
			c.Write(k, int64(k))
		} else {
			c.CompareAndSwap(k, int64(k-1), int64(k))
		}
		prevTag = mdlinproto.Tag{K: state.Key(k), PID: int64(c.id), SeqNo: c.seqnos[l]}
	}
	success, _ := c.readReplies(startIdx, fanout, opTypes, keys, startTimes)
	if !success {
		//dlog.Printf("ProposeReply for PID %d - CommandId %d return OK=False\n", c.id, e)
		return false, -1
	}

	return true, 0
}

func (c *SSMDLClient) AppResponse(commandId int32) (state.Value, uint8) {
  return 0,0
}

func (c *SSMDLClient) GrabHighestResponse() int32 {
	return 0
}

func (c *SSMDLClient) Read(key int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, 0)
	c.propose.Command.Op = state.GET
	c.sendPropose()
	return true, 0
}

func (c *SSMDLClient) Write(key int64, value int64) bool {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = state.PUT
	c.sendPropose()
	return true
}

func (c *SSMDLClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, newValue)
	c.propose.Command.OldValue = state.Value(newValue)
	c.propose.Command.Op = state.CAS
	c.sendPropose()
	return true, 0
}

func (c *SSMDLClient) preparePropose(commandId int32, key int64, value int64) {
	c.propose.CommandId = commandId
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = state.Value(value)
}

func (c *SSMDLClient) setSeqno(seqno int64) {
	c.propose.SeqNo = seqno
}

func (c *SSMDLClient) sendPropose() {
	shard := c.GetShardFromKey(c.propose.Command.K)
	//dlog.Println(fmt.Sprintf("Sending request to shard %d, Propose{CommandId %v, SeqNo %v, PID %v, Predecessor %v at time %v}", shard, c.propose.CommandId, c.propose.SeqNo, c.propose.PID, c.propose.Predecessor, time.Now().UnixMilli()))

	c.writers[shard].WriteByte(clientproto.MDL_PROPOSE)
	c.propose.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

// func (c *MDLClient) sendProposeAndReadReply() (bool, int64) {
// 	c.sendPropose()
// 	return c.readProposeReply(c.propose.CommandId)
// }

func (c *SSMDLClient) readReplies(startId int32, fanout int, opTypes []state.Operation,
	keys []int64, startTimes []time.Time) (bool, int64) {
	rarray := make([]int, fanout)
	done := false
	for !done {
		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
		if reply.OK == 0 {
			dlog.Println("Client received FAIL response for request")
			return false, int64(reply.CommandId)
		} else {
			//dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
			idx := reply.CommandId - startId

			// Print op latency
			k := keys[idx]
			lat := time.Now().Sub(startTimes[idx]).Nanoseconds()
			opType := ""
			switch opTypes[idx] {
			case state.GET:
				opType = "read"
			case state.PUT:
				opType = "write"
			case state.CAS:
				opType = "rmw"
			default:
				panic(fmt.Sprintf("Unexpected op type: %v", opTypes[idx]))
			}

			fmt.Printf("%s,%d,%d,%d\n", opType, lat, k, idx)

			// Mark op as complete
			rarray[idx] = 1

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

func (c *SSMDLClient) filterdeps(deps []mdlinproto.Tag, k state.Key) []mdlinproto.Tag {
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
