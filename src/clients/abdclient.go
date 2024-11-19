package clients

import (
	"abdproto"
	"clientproto"
	"fastrpc"
	"log"
	"state"
)

type Operation struct {
	id              int32
	key             int64
	write           bool
	writeValue      int64
	readValue       int64
	doneChan        chan bool
	success         bool
	maxV            int64
	maxTs           abdproto.Timestamp
	numReadReplies  int
	numWriteReplies int
	firstReadReply  bool
	sentWriteRound  bool
	readsWithMaxTs  int
}

func newOperation(opId int32, key int64, write bool, writeValue int64) *Operation {
	r := &Operation{
		opId,                     // id
		key,                      // key
		write,                    // write
		writeValue,               // writeValue
		0,                        // readValue
		make(chan bool, 1),       // doneChan
		false,                    // success
		0,                        // maxV
		abdproto.Timestamp{0, 0}, // maxTs
		0,                        // numReadReplies
		0,                        // numWriteReplies
		true,                     // firstReadReply
		false,                    // sentWriteReply
		0,                        // readsWithMaxTs
	}
	return r
}

type AbdClient struct {
	*AbstractClient
	readReplyChan  chan fastrpc.Serializable
	writeReplyChan chan fastrpc.Serializable
	read           *abdproto.Read
	write          *abdproto.Write
	regular        bool
	opCount        int32
	currOp         *Operation
	opChan         chan *Operation
}

func NewAbdClient(id int32, masterAddr string, masterPort int, forceLeader int, statsFile string, regular bool) *AbdClient {
	abdc := &AbdClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // readReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // writeReplyChan
		new(abdproto.Read),                      // read
		new(abdproto.Write),                     // write
		regular,                                 // regular
		0,                                       // opCount
		nil,                                     // currOp
		make(chan *Operation, CHAN_BUFFER_SIZE), // opChan
	}
	abdc.RegisterRPC(new(abdproto.ReadReply), clientproto.ABD_READ_REPLY,
		abdc.readReplyChan)
	abdc.RegisterRPC(new(abdproto.WriteReply), clientproto.ABD_WRITE_REPLY,
		abdc.writeReplyChan)

	go abdc.run()

	return abdc
}

func (c *AbdClient) AppRequest(opTypes []state.Operation, keys []int64) (bool, int64) {
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

func (c *AbdClient) OpenAppRequest(opTypes state.Operation, keys int64) {
	return
}

func (c *AbdClient) StartAsynchReadReplies(doneChan chan bool, resultChan chan int) {
	return
}

func (c *AbdClient) StopAsynchReadReplies(doneChan chan bool, resultChan chan int) (int, int) {
	return 0,0
}

func (c *AbdClient) Read(key int64) (bool, int64) {
	return c.doOp(key, 0, false)
}

func (c *AbdClient) Write(key int64, value int64) bool {
	success, _ := c.doOp(key, value, true)
	return success
}

func (c *AbdClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	log.Fatalf("ABD does not support compare-and-swap.\n")
	return false, 0
}

func (c *AbdClient) doOp(key int64, value int64, write bool) (bool, int64) {
	opId := c.opCount
	c.opCount++
	op := newOperation(opId, key, write, value)
	c.opChan <- op
	<-op.doneChan
	if write {
		return op.success, value
	} else {
		return op.success, op.maxV
	}
}

func (c *AbdClient) handleOp(op *Operation) {
	c.currOp = op
	c.read.RequestId = op.id
	c.read.K = state.Key(op.key)
	c.sendReadToAll()
}

func (c *AbdClient) sendReadToAll() {
	for i := 0; i < c.numLeaders; i++ {
		c.writers[i].WriteByte(clientproto.ABD_READ)
		c.read.Marshal(c.writers[i])
		c.writers[i].Flush()
	}
}

func (c *AbdClient) sendWriteToAll() {
	for i := 0; i < c.numLeaders; i++ {
		c.writers[i].WriteByte(clientproto.ABD_WRITE)
		c.write.Marshal(c.writers[i])
		c.writers[i].Flush()
	}
}

func (c *AbdClient) handleReadReply(readReply *abdproto.ReadReply) {
	// TODO: how do we handle clients reacting gracefully when more than
	//   f replicas fail to respond? I guess for our purposes, if this
	//   happens, the experiment should fail/crash anyway.

	// for now assuming readReply is well-formed
	if c.currOp == nil || c.currOp.id != readReply.RequestId {
		// we must have already completed this op
		return
	}

	if !c.currOp.sentWriteRound {
		c.stats.IncrementStrInt("read_quorum", int(readReply.ReplicaId))
	}

	c.currOp.numReadReplies++
	if c.currOp.firstReadReply || readReply.Ts.GreaterThan(c.currOp.maxTs) {
		c.currOp.maxV = int64(readReply.V)
		c.currOp.maxTs = readReply.Ts
		c.currOp.readsWithMaxTs = 0
	}
	c.currOp.firstReadReply = false
	if readReply.Ts.Equals(c.currOp.maxTs) {
		c.currOp.readsWithMaxTs++
	}

	if c.currOp.numReadReplies >= (c.numLeaders/2)+1 {
		if !c.currOp.write && (c.regular || c.currOp.readsWithMaxTs >= (c.numLeaders/2)+1) {
			c.stats.Increment("fast_reads")
			c.completeOperation()
		} else if !c.currOp.sentWriteRound {
			c.currOp.readValue = c.currOp.maxV
			c.doWriteRound()
			c.currOp.sentWriteRound = true
		}
	}
}

func (c *AbdClient) doWriteRound() {
	c.write.RequestId = c.currOp.id
	c.write.K = state.Key(c.currOp.key)
	var writeValue int64
	var writeTs abdproto.Timestamp
	if c.currOp.write {
		writeValue = c.currOp.writeValue
		writeTs = abdproto.Timestamp{c.currOp.maxTs.Ts + 1, int32(c.id)}
	} else {
		writeValue = c.currOp.readValue
		writeTs = c.currOp.maxTs
	}
	c.write.V = state.Value(writeValue)
	c.write.Ts = writeTs
	c.sendWriteToAll()
}

func (c *AbdClient) handleWriteReply(writeReply *abdproto.WriteReply) {
	if c.currOp == nil || c.currOp.id != writeReply.RequestId {
		// we must have already completed this op
		return
	}

	c.stats.IncrementStrInt("write_quorum", int(writeReply.ReplicaId))

	c.currOp.numWriteReplies++

	if c.currOp.numWriteReplies >= (c.numLeaders/2)+1 {
		if !c.currOp.write {
			c.stats.Increment("slow_reads")
		}
		c.completeOperation()
	}
}

func (c *AbdClient) completeOperation() {
	doneChan := c.currOp.doneChan
	c.currOp.success = true
	c.currOp = nil
	doneChan <- true
}

func (c *AbdClient) run() {
	for !c.shutdown {
		select {
		case op := <-c.opChan:
			c.handleOp(op)
			break
		case readReplyS := <-c.readReplyChan:
			readReply := readReplyS.(*abdproto.ReadReply)
			c.handleReadReply(readReply)
			break
		case writeReplyS := <-c.writeReplyChan:
			writeReply := writeReplyS.(*abdproto.WriteReply)
			c.handleWriteReply(writeReply)
			break
		}
	}
}
