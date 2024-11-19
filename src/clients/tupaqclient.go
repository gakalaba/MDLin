package clients

import (
	"clientproto"
	"dlog"
	"fastrpc"
	"gryffcommon"
	"gryffproto"
	"state"
	"stats"
	"sync"
	"time"
)

func MaxTag(a gryffproto.Tag, b gryffproto.Tag) gryffproto.Tag {
	if a.GreaterThan(b) {
		return a
	} else {
		return b
	}
}

func Max(a int32, b int32) int32 {
	if a > b {
		return a
	} else {
		return b
	}
}

type RMWType int

const (
	COMPARE_AND_SWAP RMWType = iota
)

type RMWOp struct {
	*gryffcommon.Op
	rmwType        RMWType
	oldValue       int64
	newValue       int64
	actualOldValue int64
	success        bool
}

func newRMW(requestId int32, clientId int32, key int64, oldValue int64,
	newValue int64, rmwType RMWType, dep *gryffproto.Dep) *RMWOp {
	rmw := &RMWOp{
		gryffcommon.NewOp(requestId, clientId, state.Key(key), dep),
		rmwType,
		oldValue,
		newValue,
		0,
		false,
	}
	return rmw
}

type GryffClient struct {
	*AbstractClient
	coord                  *gryffcommon.GryffCoordinator
	read1ReplyChan         chan fastrpc.Serializable
	read2ReplyChan         chan fastrpc.Serializable
	write1ReplyChan        chan fastrpc.Serializable
	write2ReplyChan        chan fastrpc.Serializable
	rmwReplyChan           chan fastrpc.Serializable
	readReplyChan          chan fastrpc.Serializable
	writeReplyChan         chan fastrpc.Serializable
	read1                  *gryffproto.Read1
	read2                  *gryffproto.Read2
	write1                 *gryffproto.Write1
	write2                 *gryffproto.Write2
	rmw                    *gryffproto.RMW
	opCount                int32
	rmwChan                chan *RMWOp
	readValue              int64
	newValue               int64
	success                bool
	maxSeenTag             gryffproto.Tag
	proxy                  bool
	useDefaultReplicaOrder bool
	defaultReplicaOrder    []int32
	doneChans              map[int32]chan bool
	mtx                    *sync.Mutex
	dep                    *gryffproto.Dep
	sequential             bool
	readTag                *gryffproto.Tag
}

func emptyDep() *gryffproto.Dep {
	return &gryffproto.Dep{-1, gryffproto.ValTag{0, gryffproto.Tag{0, 0, 0}}}
}

func NewGryffClient(id int32, masterAddr string, masterPort int,
	forceLeader int, statsFile string, regular bool, sequential bool,
	proxy bool, thrifty bool, useDefaultReplicaOrder bool,
	epaxosMode bool) *GryffClient {
	tc := &GryffClient{
		NewAbstractClient(id, masterAddr, masterPort, forceLeader, statsFile),
		nil,
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read1ReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read2ReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write1ReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write2ReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // rmwReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // readReplyChan
		make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // writeReplyChan
		new(gryffproto.Read1),                             // read1
		new(gryffproto.Read2),                             // read2
		new(gryffproto.Write1),                            // write1
		new(gryffproto.Write2),                            // write2
		new(gryffproto.RMW),                               // rmw
		0,                                                 // opCount
		make(chan *RMWOp, CHAN_BUFFER_SIZE),               // rmwChan
		0,                                                 // readValue
		0,                                                 // newValue
		false,                                             // success
		gryffproto.Tag{0, 0, 0},                           // maxSeenTag
		proxy,                                             // proxy
		useDefaultReplicaOrder,                            // useDefaultReplicaOrder
		nil,                                               // defaultReplicaOrder
		make(map[int32]chan bool),                         // doneChans
		new(sync.Mutex),                                   // mtx
		emptyDep(),                                        // dep
		sequential,                                        // sequential
		&gryffproto.Tag{0, 0, 0},                          // readTag
	}

	if proxy {
		tc.RegisterRPC(new(gryffproto.ReadReply), clientproto.Gryff_READ_REPLY,
			tc.readReplyChan)
		tc.RegisterRPC(new(gryffproto.WriteReply), clientproto.Gryff_WRITE_REPLY,
			tc.writeReplyChan)
	} else {
		tc.coord = gryffcommon.NewGryffCoordinator(tc.id, tc, regular,
			tc.numLeaders, false, thrifty, epaxosMode)
		tc.RegisterRPC(new(gryffproto.Read1Reply), clientproto.Gryff_READ_1_REPLY,
			tc.read1ReplyChan)
		tc.RegisterRPC(new(gryffproto.Read2Reply), clientproto.Gryff_READ_2_REPLY,
			tc.read2ReplyChan)
		tc.RegisterRPC(new(gryffproto.Write1Reply), clientproto.Gryff_WRITE_1_REPLY,
			tc.write1ReplyChan)
		tc.RegisterRPC(new(gryffproto.Write2Reply), clientproto.Gryff_WRITE_2_REPLY,
			tc.write2ReplyChan)
	}
	tc.RegisterRPC(new(gryffproto.RMWReply), clientproto.Gryff_RMW_REPLY,
		tc.rmwReplyChan)

	tc.defaultReplicaOrder = make([]int32, tc.numLeaders)
	for i := 0; i < tc.numLeaders; i++ {
		tc.defaultReplicaOrder[i] = int32((int(tc.id)%tc.numLeaders + 1 + i) % tc.numLeaders)
	}

	go tc.run()

	return tc
}

/**
 * gryffcommon.IGryffCoordinator methods
 */
func (c *GryffClient) Printf(fmt string, v ...interface{}) {
	dlog.Printf(fmt, v...)
}

func (c *GryffClient) SendRead1(rop *gryffcommon.ReadOp, replicas []int32) {
	c.read1.RequestId = rop.RequestId
	c.read1.ClientId = c.id
	c.read1.K = state.Key(rop.Key)
	c.read1.D = *rop.Dep

	for i := 0; i < len(replicas); i++ {
		c.writers[replicas[i]].WriteByte(clientproto.Gryff_READ_1)
		c.read1.Marshal(c.writers[replicas[i]])
		c.writers[replicas[i]].Flush()
	}
}

func (c *GryffClient) SendRead2(rop *gryffcommon.ReadOp, replicas []int32) {
	c.read2.RequestId = rop.RequestId
	c.read2.ClientId = c.id
	c.read2.K = state.Key(rop.Key)
	c.read2.Vt = *rop.ReadValTag
	c.read2.UpdateId = rop.ReadUpdateId

	for i := 0; i < len(replicas); i++ {
		c.writers[replicas[i]].WriteByte(clientproto.Gryff_READ_2)
		c.read2.Marshal(c.writers[replicas[i]])
		c.writers[replicas[i]].Flush()
	}
}

func (c *GryffClient) SendWrite1(wop *gryffcommon.WriteOp, replicas []int32) {
	c.write1.RequestId = wop.RequestId
	c.write1.ClientId = c.id
	c.write1.K = state.Key(wop.Key)
	c.write1.V = state.Value(wop.WriteValue)
	c.write1.D = *wop.Dep

	for i := 0; i < len(replicas); i++ {
		c.writers[replicas[i]].WriteByte(clientproto.Gryff_WRITE_1)
		c.write1.Marshal(c.writers[replicas[i]])
		c.writers[replicas[i]].Flush()
	}
}

func (c *GryffClient) SendWrite2(wop *gryffcommon.WriteOp, replicas []int32) {
	c.write2.RequestId = wop.RequestId
	c.write2.ClientId = c.id
	c.write2.K = state.Key(wop.Key)
	c.write2.Vt = gryffproto.ValTag{state.Value(wop.WriteValue), *wop.WriteTag}

	for i := 0; i < len(replicas); i++ {
		c.writers[replicas[i]].WriteByte(clientproto.Gryff_WRITE_2)
		c.write2.Marshal(c.writers[replicas[i]])
		c.writers[replicas[i]].Flush()
	}
}

func (c *GryffClient) CompleteRead(requestId int32,
	clientId int32, success bool, readValue int64) {
	c.mtx.Lock()
	doneChan, ok := c.doneChans[requestId]
	if ok {
		delete(c.doneChans, requestId)
	}
	c.mtx.Unlock()
	if ok {
		c.success = success
		c.readValue = readValue
		doneChan <- true
	}
}

func (c *GryffClient) CompleteWrite(requestId int32,
	clientId int32, success bool) {
	c.mtx.Lock()
	doneChan, ok := c.doneChans[requestId]
	if ok {
		delete(c.doneChans, requestId)
	}
	c.mtx.Unlock()
	if ok {
		c.success = success
		doneChan <- true
	}
}

func (c *GryffClient) GetStats() *stats.StatsMap {
	return c.stats
}

func (c *GryffClient) GetReplicasByRank() []int32 {
	if c.useDefaultReplicaOrder {
		return c.defaultReplicaOrder
	} else {
		return c.replicasByPingRank
	}
}

func (c *GryffClient) SetForceWrite() {
	// only relevant for server coordinator
}

func (c *GryffClient) HandleOverwrite(key state.Key, newValue state.Value,
	newTag *gryffproto.Tag) {
	// only relevant for server coordinator
}

/**
 * End gryffcommon.IGryffCoordinator methods
 */

func (c *GryffClient) AppRequest(opTypes []state.Operation, keys []int64) (bool, int64) {
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

func (c *GryffClient) OpenAppRequest(opTypes state.Operation, keys int64) {
	return
}

func (c *GryffClient) StartAsynchReadReplies(doneChan chan bool, resultChan chan int) {
	return
}

func (c *GryffClient) StopAsynchReadReplies(doneChan chan bool, resultChan chan int) (int, int) {
	return 0,0
}

/**
 * Begin Read related procedures
 */
func (c *GryffClient) Read(key int64) (bool, int64) {
	opId := c.opCount
	c.opCount++
	doneChan := make(chan bool, 1)
	c.mtx.Lock()
	c.doneChans[opId] = doneChan
	c.mtx.Unlock()

	dlog.Printf("Starting Read(%d).\n", opId)

	if c.proxy {
		c.sendReadToNearest(opId, state.Key(key))
	} else {
		c.coord.StartRead(opId, c.id, state.Key(key), c.dep)
	}

	<-doneChan
	if c.sequential {
		c.dep = &gryffproto.Dep{state.Key(key),
			gryffproto.ValTag{state.Value(c.readValue), *c.readTag}}
	}
	dlog.Printf("Finished Read(%d).\n", opId)
	return c.success, c.readValue
}

func (c *GryffClient) sendReadToNearest(requestId int32, key state.Key) {
	read := &gryffproto.Read{requestId, c.id, state.Key(key), *c.dep}

  leader := c.GetShardFromKey(key)
	if c.forceLeader >= 0 {
		leader = c.forceLeader
	} else {
    panic("shouldn't be here, PingRank isn't implemented")
		leader = int(c.replicasByPingRank[0])
	}
	dlog.Printf("Sending Read to replica %d out of %d.\n", leader,
		len(c.replicasByPingRank))

	c.writers[leader].WriteByte(clientproto.Gryff_READ)
	read.Marshal(c.writers[leader])
	c.writers[leader].Flush()
}

func (c *GryffClient) handleReadReply(readReply *gryffproto.ReadReply) {
	dlog.Printf("Received ReadReply[%d,%d].\n", readReply.RequestId,
		readReply.ClientId)
	c.CompleteRead(readReply.RequestId, readReply.ClientId, readReply.OK == 1,
		int64(readReply.V))
}

func (c *GryffClient) handleRead1Reply(read1Reply *gryffproto.Read1Reply) {
	c.coord.HandleRead1Reply(gryffcommon.ShiftInts(read1Reply.RequestId,
		read1Reply.ClientId), &read1Reply.Vt, read1Reply.ReplicaId)
}

func (c *GryffClient) handleRead2Reply(read2Reply *gryffproto.Read2Reply) {
	c.coord.HandleRead2Reply(gryffcommon.ShiftInts(read2Reply.RequestId,
		read2Reply.ClientId), read2Reply.T, read2Reply.ReplicaId)
}

/**
 * End Read related procedures
 */

/**
 * Begin Write related procedures
 */
func (c *GryffClient) Write(key int64, value int64) bool {
	opId := c.opCount
	c.opCount++
	doneChan := make(chan bool, 1)
	c.mtx.Lock()
	c.doneChans[opId] = doneChan
	c.mtx.Unlock()

	dlog.Printf("Starting Write(%d).\n", opId)

	if c.proxy {
		c.sendWriteToNearest(opId, state.Key(key), state.Value(value))
	} else {
		c.coord.StartWrite(opId, c.id, state.Key(key), state.Value(value), c.dep)
	}

	<-doneChan
	if c.sequential {
		c.dep = emptyDep()
	}

	dlog.Printf("Finished Write(%d).\n", opId)
	return c.success
}

func (c *GryffClient) sendWriteToNearest(requestId int32, key state.Key,
	value state.Value) {
	write := &gryffproto.Write{requestId, c.id, state.Key(key),
		state.Value(value), *c.dep}

  leader := int32(c.GetShardFromKey(key))
	if c.forceLeader >= 0 {
		leader = int32(c.forceLeader)
	} else {
    panic("shouldn't be here, PingRank isn't implemneted")
		leader = c.replicasByPingRank[0]
	}
	dlog.Printf("Sending Write to replica %d out of %d.\n", leader,
		len(c.replicasByPingRank))

	c.writers[leader].WriteByte(clientproto.Gryff_WRITE)
	write.Marshal(c.writers[leader])
	c.writers[leader].Flush()
}

func (c *GryffClient) handleWriteReply(writeReply *gryffproto.WriteReply) {
	c.CompleteWrite(writeReply.RequestId, writeReply.ClientId, writeReply.OK == 1)
}

func (c *GryffClient) handleWrite1Reply(write1Reply *gryffproto.Write1Reply) {
	c.coord.HandleWrite1Reply(gryffcommon.ShiftInts(write1Reply.RequestId,
		write1Reply.ClientId), write1Reply.ClientId, write1Reply.ReplicaId,
		&write1Reply.Vt)
}

func (c *GryffClient) SendWrite2ConcurrentTimeout(write2 *gryffcommon.WriteOp) {}

func (c *GryffClient) handleWrite2Reply(write2Reply *gryffproto.Write2Reply) {
	c.coord.HandleWrite2Reply(gryffcommon.ShiftInts(write2Reply.RequestId,
		write2Reply.ClientId), write2Reply.ReplicaId, &write2Reply.T,
		write2Reply.ClientId, write2Reply.Applied)
}

/**
 * End Write related procedures
 */

/**
 * Begin Read-Modify-Write related procedures
 */
func (c *GryffClient) CompareAndSwap(key int64, oldValue int64,
	newValue int64) (bool, int64) {
	opId := c.opCount
	c.opCount++
	doneChan := make(chan bool, 1)
	c.mtx.Lock()
	c.doneChans[opId] = doneChan
	c.mtx.Unlock()

	dlog.Printf("Starting CAS(%d).\n", opId)
	rmw := newRMW(opId, c.id, key, oldValue, newValue, COMPARE_AND_SWAP, c.dep)
	c.rmwChan <- rmw
	<-doneChan
	if c.sequential {
		c.dep = emptyDep()
	}
	dlog.Printf("Finished CAS(%d).\n", opId)
	return c.success, c.readValue
}

func (c *GryffClient) handleRMW(rmw *RMWOp) {
	c.rmw.RequestId = rmw.RequestId
	c.rmw.ClientId = rmw.ClientId
	c.rmw.K = state.Key(rmw.Key)
	c.rmw.OldValue = state.Value(rmw.oldValue)
	c.rmw.NewValue = state.Value(rmw.newValue)
	c.sendRMWToNearest()
}

func (c *GryffClient) sendRMWToNearest() {
  leader := int32(c.GetShardFromKey(c.rmw.K))
	if c.forceLeader >= 0 {
		leader = int32(c.forceLeader)
	} else {
    panic("shouldn't be here, pingRank not implmeneted")
		leader = c.replicasByPingRank[0]
	}
	c.writers[leader].WriteByte(clientproto.Gryff_RMW)
	c.rmw.Marshal(c.writers[leader])
	c.writers[leader].Flush()
}

func (c *GryffClient) handleRMWReply(rmwReply *gryffproto.RMWReply) {
	c.mtx.Lock()
	dlog.Printf("Received RMWReply(%d).\n", rmwReply.RequestId)
	doneChan, ok := c.doneChans[rmwReply.RequestId]
	if ok {
		delete(c.doneChans, rmwReply.RequestId)
	}
	c.mtx.Unlock()
	if ok {
		c.success = rmwReply.OK == 1
		c.readValue = int64(rmwReply.OldValue)
		doneChan <- true
	}
}

/**
 * End Read-Modify-Write related procedures.
 */

func (c *GryffClient) run() {
	for !c.shutdown {
		select {
		case rmw := <-c.rmwChan:
			dlog.Printf("handleRMW")
			c.handleRMW(rmw)
			break
		case read1ReplyS := <-c.read1ReplyChan:
			read1Reply := read1ReplyS.(*gryffproto.Read1Reply)
			dlog.Printf("handleRead1Reply for %d", read1Reply.RequestId)
			b := time.Now()
			c.handleRead1Reply(read1Reply)
			dlog.Printf("handleRead1Reply took %dms", time.Now().Sub(b)/time.Millisecond)
			break
		case read2ReplyS := <-c.read2ReplyChan:
			dlog.Printf("handleRead2Reply")
			read2Reply := read2ReplyS.(*gryffproto.Read2Reply)
			c.handleRead2Reply(read2Reply)
			break
		case write1ReplyS := <-c.write1ReplyChan:
			dlog.Printf("handleWrite1Reply")
			write1Reply := write1ReplyS.(*gryffproto.Write1Reply)
			c.handleWrite1Reply(write1Reply)
			break
		case write2ReplyS := <-c.write2ReplyChan:
			dlog.Printf("handleWrite2Reply")
			write2Reply := write2ReplyS.(*gryffproto.Write2Reply)
			c.handleWrite2Reply(write2Reply)
			break
		case rmwReplyS := <-c.rmwReplyChan:
			dlog.Printf("handleRMWReply")
			rmwReply := rmwReplyS.(*gryffproto.RMWReply)
			c.handleRMWReply(rmwReply)
			break
		case readReplyS := <-c.readReplyChan:
			dlog.Printf("handleRead")
			readReply := readReplyS.(*gryffproto.ReadReply)
			c.handleReadReply(readReply)
			break
		case writeReplyS := <-c.writeReplyChan:
			dlog.Printf("handleWrite")
			writeReply := writeReplyS.(*gryffproto.WriteReply)
			c.handleWriteReply(writeReply)
			break
		}
	}
}
