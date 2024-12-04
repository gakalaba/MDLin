package clients

import (
	"clientproto"
	"dlog"
	"fastrpc"
	//"fmt"
	"genericsmr"
	"mdlinproto"
	"state"
	"time"
  "sync"
)

type AsynchClient struct {
	*AbstractClient
	proposeReplyChan chan fastrpc.Serializable
	propose          *mdlinproto.Propose
	coordinationReq  *mdlinproto.CoordinationRequest
	opCount          int32
	fast             bool
	noLeader         bool
	seqnos           map[int]int64
	SSA		 bool
	latestReceived   int32
  lastSentTag      mdlinproto.Tag
  mu               *sync.Mutex
  mapMu            *sync.Mutex
  repliesMap       map[int32]*mdlinproto.ProposeReply
}

func NewAsynchClient(id int32, masterAddr string, masterPort int, forceLeader int, statsFile string,
	fast bool, noLeader bool, SSA bool) *AsynchClient {
	pc := &AsynchClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE), // proposeReplyChan
		new(mdlinproto.Propose),             // propose
		new(mdlinproto.CoordinationRequest), // coordinationReq
		0,                                   // opCount
		fast,                                // fast
		noLeader,                            // noLeader
		make(map[int]int64),
		SSA,
		-1,
		mdlinproto.Tag{K: state.Key(-1), PID: int64(-1), SeqNo: -1},
		new(sync.Mutex),
		new(sync.Mutex),
		make(map[int32]*mdlinproto.ProposeReply),
	}
	pc.propose.PID = int64(id) // only need to set this once per client
	pc.RegisterRPC(new(mdlinproto.ProposeReply), clientproto.MDL_PROPOSE_REPLY, pc.proposeReplyChan)
  go pc.asynchReadReplies()
	return pc
}

func (c *AsynchClient) AppRequest(opTypes []state.Operation, keys []int64, oldValues []int64, newValues []int64) (bool, int64) {
  if len(opTypes) > 1 || len(keys) > 1 || len(oldValues) > 1 || len(newValues) > 1 {
    panic("Can only send one request at a time with AppRequest")
  }
  key := keys[0]
  // TODO how to clear r.seen list in impl?? c.setTimestamp(0, n)
	l := c.GetShardFromKey(state.Key(key))
	// Figure out the sequence number
	if _, ok := c.seqnos[l]; !ok {
		c.seqnos[l] = 0
	} else {
		c.seqnos[l]++
	}
	// Assign the sequence number and batch dependencies for this request
	c.setSeqno(c.seqnos[l])
	//c.setTimestamp(i, n)
	c.setTimestamp()

  myCommandId := c.setCommandId()

  c.mu.Lock()
  lastReceived := c.latestReceived
  c.mu.Unlock()

	// We should set the predecessor tag based on whether we are concurrent with the previous sent request
  sendCoord := false
  if lastReceived == (myCommandId-1) {
	  dlog.Printf("CommandId %v is NOT concurrent", myCommandId)
		c.propose.Predecessor = mdlinproto.Tag{K: state.Key(-1), PID: int64(-1), SeqNo: -1}
	} else {
    sendCoord = true
	  dlog.Printf("CommandId %v is Concurrent with CommandId %v", myCommandId, lastReceived+1)
		c.propose.Predecessor = c.lastSentTag
	}

	if opTypes[0] == state.GET {
		c.Read(key)
	} else if opTypes[0] == state.PUT {
		//dlog.Printf("type is Put!")
		c.Write(key, newValues[0])
	} else {
		c.CompareAndSwap(key, oldValues[0], newValues[0])
	}

	// We should only send this if we assigned a predecessor... and it should be whatever our predecessor tag is!
	if (sendCoord && !c.SSA) {
		// Send the coordination request
		// (Keep this after sending the request for now, since
		// logic in the send function assumes request was send)
		dlog.Printf("AND is sending coord req to %v on shard %v", c.lastSentTag, l)
		c.sendCoordinationRequest(c.lastSentTag, l)
	}
	c.lastSentTag = mdlinproto.Tag{K: state.Key(key), PID: int64(c.id), SeqNo: c.seqnos[l]}
	return true, int64(c.propose.CommandId)
}

/***********************************************************************/
/*************For Grafana Testing Only!! Not for Austin's API **********/
/***********************************************************************/
func (c *AsynchClient) GrabHighestResponse() int32 {
	c.mu.Lock()
	result := c.latestReceived
	c.mu.Unlock()
	return result
}

func (c *AsynchClient) AppResponse(commandId int32) (state.Value, uint8) {
  for true {
	  c.mapMu.Lock()
	  reply, OK := c.repliesMap[commandId]
	  if OK {
		  // For the purposes of double await, we will NOT delete the value
		  //delete(c.repliesMap, commandId)
		  c.mapMu.Unlock()
		  return reply.Value, reply.OK
	  }
	  c.mapMu.Unlock()
	  time.Sleep(100000)
  }
  //go cleanMap(request.CommandId)
  return 0, 0
}

func (c *AsynchClient) cleanMap(bound int32) {
  var keysToDelete map[int32]bool
  c.mapMu.Lock()
  for k, _ := range c.repliesMap {
    if k < bound {
      keysToDelete[k] = true
    }
  }

  for k, _ := range keysToDelete {
    delete(c.repliesMap, k)
  }
  c.mapMu.Unlock()
}

func (c *AsynchClient) asynchReadReplies() {
	for true {
		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
		c.mu.Lock()
		if reply.CommandId > c.latestReceived {
			c.latestReceived = reply.CommandId
		}
		c.mu.Unlock()
		c.mapMu.Lock()
		c.repliesMap[reply.CommandId] = reply
		c.mapMu.Unlock()
	}
}

func (c *AsynchClient) Read(key int64) (bool, int64) {
	c.preparePropose(state.GET, key, 0)
	c.sendPropose()
	return true, 0
}

func (c *AsynchClient) Write(key int64, value int64) bool {
	//dlog.Printf("inside write...")
	c.preparePropose(state.PUT, key, value)
	c.sendPropose()
	return true
}

func (c *AsynchClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	c.preparePropose(state.CAS, key, newValue)
	c.propose.Command.OldValue = state.Value(newValue)
	c.sendPropose()
	return true, 0
}

func (c *AsynchClient) preparePropose(opType state.Operation, key int64, value int64) {
	c.propose.Command.K = state.Key(key)
	c.propose.Command.V = state.Value(value)
  c.propose.Command.Op = opType
}

func (c *AsynchClient) setSeqno(seqno int64) {
	c.propose.SeqNo = seqno
}

func (c *AsynchClient) setCommandId() int32 {
  commandId := c.opCount
  c.opCount++
  c.propose.CommandId = commandId
  return commandId
}

func (c *AsynchClient) setTimestamp() {
        c.propose.Timestamp = 1
        // TODO How will we clean r.seen??
	/*if (i == n-1) {
          c.propose.Timestamp = 0
        }*/
}

func (c *AsynchClient) sendPropose() {
	shard := c.GetShardFromKey(c.propose.Command.K)
	//dlog.Println(fmt.Sprintf("Sending request to shard %d, Propose{CommandId %v, SeqNo %v, PID %v, Predecessor %v at time %v}", shard, c.propose.CommandId, c.propose.SeqNo, c.propose.PID, c.propose.Predecessor, time.Now().UnixMilli()))

	c.writers[shard].WriteByte(clientproto.MDL_PROPOSE)
	c.propose.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

func (c *AsynchClient) sendCoordinationRequest(predecessorTag mdlinproto.Tag, myShard int) {
	shard := c.GetShardFromKey(predecessorTag.K)
	//dlog.Printf("Sending CoordinationRequest to shard %d on behalf of CommandId %v at time %v\n", shard, c.propose.CommandId, time.Now().UnixMilli())

	// Prepare the coordination request object
	c.coordinationReq.AskerTag = mdlinproto.Tag{K: c.propose.Command.K, PID: int64(c.id), SeqNo: c.propose.SeqNo}
	c.coordinationReq.AskeeTag = predecessorTag
	c.coordinationReq.From = int32(c.GetShardFromKey(c.propose.Command.K))

	c.writers[shard].WriteByte(clientproto.MDL_COORDREQ)
	c.coordinationReq.Marshal(c.writers[shard])
	c.writers[shard].Flush()
}

// func (c *AsynchClient) sendProposeAndReadReply() (bool, int64) {
// 	c.sendPropose()
// 	return c.readProposeReply(c.propose.CommandId)
// }

func (c *AsynchClient) StartAsynchReadReplies(doneChan chan bool, resultChan chan int) {
  done := false
  numReplies := 0
  highestReplyCommandId := -1
  dlog.Printf("yoyoyoyoy")
  for !done {
    select {
    case <-doneChan:
      done = true
      dlog.Printf("Ah, we got to the done chan")
      break
    case reply := <-c.proposeReplyChan:
      //reply = (reply).(*mdlinproto.ProposeReply)
      if (reply).(*mdlinproto.ProposeReply).OK == 0 {
        panic("Client received FAIL response for request")
      }
      numReplies++
      if (reply).(*mdlinproto.ProposeReply).CommandId > int32(highestReplyCommandId) {
        highestReplyCommandId = int((reply).(*mdlinproto.ProposeReply).CommandId)
      }
      break
    }
  }
  dlog.Printf("hellow?")
  resultChan <- numReplies
  resultChan <- highestReplyCommandId
}

func (c *AsynchClient) StopAsynchReadReplies(doneChan chan bool, resultChan chan int) (int, int) {
  dlog.Printf("Calling stop on client!")
  doneChan <- true
  dlog.Printf("pushed on done chan")
  numReplies := <-resultChan
  highestReplyCommand := <-resultChan
  return numReplies, highestReplyCommand
}

func (c *AsynchClient) readReplies(startId int32, fanout int, opTypes []state.Operation,
	keys []int64, startTimes []time.Time) (bool, int64) {
	//rarray := make([]int, fanout)
	rarray := make([]int, fanout)
	//done := 0
	done := false
	//for (done < fanout) {
	for !done {
		reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
		if reply.OK == 0 {
			dlog.Println("Client received FAIL response for request")
			return false, int64(reply.CommandId)
		} else {
			//dlog.Printf("Received ProposeReply for %d\n", reply.CommandId)
			idx := reply.CommandId - startId

			// Print op latency
			/*k := keys[idx]
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
			*/

			//
			// Mark op as complete
			rarray[idx] = 1

			// Check if we have all replies
			done = true
			for i := 0; i < fanout; i++ {
				if rarray[i] == 0 {
					done = false
					break
				}
			}//
			//done++
		}
	}
	return true, int64(fanout)
}

// func (c *AsynchClient) readProposeReply(commandId int32) (bool, int64) {
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

func (c *AsynchClient) filterdeps(deps []mdlinproto.Tag, k state.Key) []mdlinproto.Tag {
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
