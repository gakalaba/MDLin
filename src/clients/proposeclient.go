package clients

import (
	"clientproto"
	"fastrpc"

	"fmt"
	"dlog"
	"genericsmr"
	"genericsmrproto"
	"state"
	"time"
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

func (c *ProposeClient) AppRequest(opTypes []state.Operation, keys []int64, newValues []state.Value, oldValues []state.Value) (bool, state.Value) {
	fmt.Printf("AppRequest, running operation, opTypes: %v, keys: %v, oldValues: %v, newValues: %v\n", opTypes, keys, oldValues, newValues)
	for i, opType := range opTypes {
		key := keys[i]
		oldValue := oldValues[i]
		newValue := newValues[i]

		//before := time.Now()
		//var opTypeStr string
		var success bool
		var returnValue state.Value

		if opType == state.GET || opType == state.SCARD || opType == state.SUBSCRIBE || opType == state.LISTEN || opType == state.EXISTS {
			success, returnValue = c.Read(opType, key, state.NewString("0"))
		} else if opType == state.HMGET {
			success, returnValue = c.Read(opType, key, newValue)
		} else if opType == state.PUT || opType == state.SET || opType == state.INCR || opType == state.SADD || opType == state.PUBLISH || opType == state.SREM || opType==state.SISMEMBER || opType==state.ZADD {
			success = c.Write(opType, key, newValue)
			returnValue = state.NewString("0")
		} else {
			success, returnValue = c.CompareAndSwap(opType, key, oldValue, newValue)
		}

		if success {
			//lat := after.Sub(before).Nanoseconds()
			//fmt.Printf("%s,%d,%d,%d\n", opTypeStr, lat, k, i)
			return true, returnValue
		} else {
			return false, state.NewString("-1")
		}
	}

	return true, state.NewString("0")
}

func (c *ProposeClient) AppResponse(commandId int32) (state.Value, uint8) {
  return state.NewString("0"), 0
}

func (c *ProposeClient) GrabHighestResponse() int32 {
	return 0
}

func (c *ProposeClient) Read(opType state.Operation, key int64, value state.Value) (bool, state.Value) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = opType
	return c.sendProposeAndReadReply()
}

func (c *ProposeClient) Write(opType state.Operation, key int64, value state.Value) bool {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, value)
	c.propose.Command.Op = opType
	success, _ := c.sendProposeAndReadReply()
	return success
}

func (c *ProposeClient) CompareAndSwap(opType state.Operation, key int64, oldValue state.Value,
	newValue state.Value) (bool, state.Value) {
	commandId := c.opCount
	c.opCount++
	c.preparePropose(commandId, key, newValue)
	c.propose.Command.OldValue = oldValue
	c.propose.Command.Op = opType
	return c.sendProposeAndReadReply()
}

func (c *ProposeClient) preparePropose(commandId int32, key int64, value state.Value) {
	c.propose.CommandId = commandId
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = value
}

func (c *ProposeClient) sendProposeAndReadReply() (bool, state.Value) {
	c.sendPropose()
	return c.readProposeReply(c.propose.CommandId)
}

func (c *ProposeClient) sendPropose() {
	if !c.fast {
		replica := c.GetShardFromKey(c.propose.Command.K)
		if c.noLeader {
			if c.forceLeader >= 0 {
				replica = c.forceLeader
			} else {
				panic("shouldn't be here...PingRank isn't implemented")
				replica = int(c.replicasByPingRank[0])
			}
		}
		//dlog.Printf("@Sending request to %d\n", replica)
		A := time.Now()
		c.writers[replica].WriteByte(clientproto.GEN_PROPOSE)
		B := time.Now()
		c.propose.Marshal(c.writers[replica])
		C := time.Now()
		c.writers[replica].Flush()
		D := time.Now()
		dlog.Printf("WriteByte=%v, Marshal=%v, Flush=%v", B.Sub(A), C.Sub(B), D.Sub(C))
	} else {
		//dlog.Printf("Sending request to all replicas\n")
		for i := 0; i < c.numLeaders; i++ {
			c.writers[i].WriteByte(clientproto.GEN_PROPOSE)
			c.propose.Marshal(c.writers[i])
			c.writers[i].Flush()
		}
	}
}

func (c *ProposeClient) readProposeReply(commandId int32) (bool, state.Value) {
	for !c.shutdown {
		reply := (<-c.proposeReplyChan).(*genericsmrproto.ProposeReplyTS)
		if reply.OK == 0 {
			return false, state.NewString("0")
		} else {
			//dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
			if commandId == reply.CommandId {
				return true, reply.Value
			}
		}
	}
	return false, state.NewString("0")
}
