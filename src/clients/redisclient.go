package clients

import (
	"clientproto"
	"dlog"
	"fastrpc"
	"fmt"
	"genericsmr"
	"mdlinproto"
	"state"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
	repliesMap       map[int32]state.Value
	// MOCKING THE STATE 
	mockState        *state.State
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
		make(map[int32]state.Value),
		state.NewState(),
	}
// 	pc.propose.PID = int64(id) // only need to set this once per client
// 	pc.RegisterRPC(new(mdlinproto.ProposeReply), clientproto.MDL_PROPOSE_REPLY, pc.proposeReplyChan)
//   go pc.asynchReadReplies()
	return pc
}

func (c *AsynchClient) AppRequest(opTypes []state.Operation, keys []int64, oldValues []state.Value, newValues []state.Value) (bool, state.Value) {
  if len(opTypes) > 1 || len(keys) > 1 || len(oldValues) > 1 || len(newValues) > 1 {
    panic("Can only send one request at a time with AppRequest")
  }
  key := keys[0]
  var oldValueObj, newValueObj state.Value
  if len(oldValues) > 0 {
    oldValueObj = oldValues[0]
  }
  if len(newValues) > 0 {
    newValueObj = newValues[0]
  }

  command := &state.Command{
	Op:       opTypes[0],
	K:        state.Key(key),
	V:        newValueObj,
	OldValue: oldValueObj,
	}

  fmt.Println("Executing command locally: ", command)
  result := command.Execute(c.mockState)
  fmt.Println("Completed execution: ", result)
  commandId := atomic.AddInt32(&c.opCount, 1)
  fmt.Println("Calcualted commandiD : ", commandId)
  c.repliesMap[commandId] = result
  fmt.Println("Added reply to map: ", c.repliesMap[commandId])
//   fmt.Println("Command result: %+v\n", result)
	
//   l := c.GetShardFromKey(state.Key(key))
//   // Figure out the sequence number
//   if _, ok := c.seqnos[l]; !ok {
//     c.seqnos[l] = 0
//   } else {
//     c.seqnos[l]++
//   }
//   // Assign the sequence number and batch dependencies for this request
//   c.setSeqno(c.seqnos[l])
//   c.setTimestamp()

//   myCommandId := c.setCommandId()

//   c.mu.Lock()
//   lastReceived := c.latestReceived
//   c.mu.Unlock()

  // We should set the predecessor tag based on whether we are concurrent with the previous sent request
//   sendCoord := false
//   if lastReceived == (myCommandId-1) {
//     dlog.Printf("CommandId %v is NOT concurrent", myCommandId)
//     c.propose.Predecessor = mdlinproto.Tag{K: state.Key(-1), PID: int64(-1), SeqNo: -1}
//   } else {
//     sendCoord = true
//     dlog.Printf("CommandId %v is Concurrent with CommandId %v", myCommandId, lastReceived+1)
//     c.propose.Predecessor = c.lastSentTag
//   }

//   if opTypes[0] == state.GET {
//     c.Read(key)
//   } else if opTypes[0] == state.PUT {
//     c.Write(key, newValueObj)
//   } else {
//     c.CompareAndSwap(key, oldValueObj, newValueObj)
//   }

//   if sendCoord {
//     c.sendCoordinationRequest(c.propose.Predecessor, l)
//   }
  fmt.Println("Returning id ", int(commandId))
  return true, state.NewString(strconv.Itoa(int(commandId)))
}

func (c *AsynchClient) Write(key int64, value state.Value) bool {
  c.preparePropose(state.PUT, key, value)
  c.sendPropose()
  return true
}

func (c *AsynchClient) CompareAndSwap(key int64, oldValue state.Value, newValue state.Value) (bool, state.Value) {
  c.preparePropose(state.CAS, key, newValue)
  c.propose.Command.OldValue = oldValue
  c.sendPropose()
  return true, state.NIL
}

func (c *AsynchClient) preparePropose(opType state.Operation, key int64, value state.Value) {
  c.propose.Command.K = state.Key(key)
  c.propose.Command.V = value
  c.propose.Command.Op = opType
}

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
	  fmt.Println("THis is reply", reply)
      return reply, 1
    }
    c.mapMu.Unlock()
    time.Sleep(100000)
  }
  
  return state.NIL, 0
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
	// for true {
	// 	reply := (<-c.proposeReplyChan).(*mdlinproto.ProposeReply)
	// 	c.mu.Lock()
	// 	if reply.CommandId > c.latestReceived {
	// 		c.latestReceived = reply.CommandId
	// 	}
	// 	c.mu.Unlock()
	// 	c.mapMu.Lock()
	// 	c.repliesMap[reply.CommandId] = reply
	// 	c.mapMu.Unlock()
	// }
}

func (c *AsynchClient) Read(key int64) (bool, state.Value) {
	c.preparePropose(state.GET, key, state.NewString("0"))
	c.sendPropose()
  return true, state.NIL
}

func (c *AsynchClient) sendPropose() {
	// shard := c.GetShardFromKey(c.propose.Command.K)
	//dlog.Println(fmt.Sprintf("Sending request to shard %d, Propose{CommandId %v, SeqNo %v, PID %v, Predecessor %v at time %v}", shard, c.propose.CommandId, c.propose.SeqNo, c.propose.PID, c.propose.Predecessor, time.Now().UnixMilli()))

	// c.writers[shard].WriteByte(clientproto.MDL_PROPOSE)
	// c.propose.Marshal(c.writers[shard])
	// c.writers[shard].Flush()
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
