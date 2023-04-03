package mdlin

import (
	"bufio"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"mdlinproto"
	"mysort"
	"net"
	"state"
	"sync"
	"time"
  "container/list"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const NUM_CLIENTS = uint8(2)
const NUM_OUTSTANDING_INST = 10000

const MAX_BATCH = 5000

const DEBUG_LEVEL = 0
const LEVEL0 = 0
const LEVELALL = 1

const EPOCH = 4 //10
const MAX_EPOCHS = 3

func NewPrintf(level int, str ...interface{}) {
	if level <= DEBUG_LEVEL {
		log.Println(fmt.Sprintf(str[0].(string), str[1:]...))
	}
}

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	prepareChan         chan fastrpc.Serializable
	acceptChan          chan fastrpc.Serializable
	commitChan          chan fastrpc.Serializable
	commitShortChan     chan fastrpc.Serializable
	prepareReplyChan    chan fastrpc.Serializable
	acceptReplyChan     chan fastrpc.Serializable
	prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	commitShortRPC      uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
	IsLeader            bool        // does this replica think it is the leader
	instanceSpace       []*Instance // the space of all instances (used and not yet used) //TODO change the name to orderedLog!!
  crtInstance         int32       // highest active instance number that this replica knows about NOT inclusive!
  defaultBallot       int32       // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
	Shutdown            bool
	counter             int
	flush               bool
	committedUpTo       int32 //This is inclusive!
	batchingEnabled     bool
	// Add these for single-sharded multidispatch
	nextSeqNo        map[int64]int64                    // Mapping client PID to next expected sequence number
	outstandingInst  map[int64][]*genericsmr.MDLPropose // Mapping client PID to `sorted` list of outstanding proposals received
	noProposalsReady bool
  finalAcceptChan      chan fastrpc.Serializable
  finalAcceptReplyChan chan fastrpc.Serializable
  finalAcceptRPC        uint8
  finalAcceptReplyRPC   uint8
	// Add these for multi-sharded multi-dispatch
	shardId             int
	shardAddrList       []string
	shards              []net.Conn // cache of connections to all other replicas
	shardReaders        []*bufio.Reader
	shardWriters        []*bufio.Writer
	shListener          net.Listener
	buflock             *sync.Mutex

  bufferedLog         *list.List  // the unordered requests LINKED LIST
  coordReqReplyChan   chan fastrpc.Serializable
  outstandingCR       map[*mdlinproto.Tag]*genericsmr.MDLCoordReq
  outstandingCRR      map[*mdlinproto.Tag]*mdlinproto.CoordinationResponse

  timer               *time.Timer
  epoch               int32
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

//TODO currently structured so that batching is turned off
type Instance struct {
	cmds       []state.Command
  ballot     int32
	status     InstanceStatus
	lb         *LeaderBookkeeping
	pid        int64
	seqno      int64
  pred       *mdlinproto.Tag
  cr         *genericsmr.MDLCoordReq
  epoch      int32
  // For linked list
  prev *Instance
  next *Instance
  predSetSize []int32
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.MDLPropose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
  coordinated     int8
}

func tagtostring(t mdlinproto.Tag) string {
	return fmt.Sprintf("Tag = K(%d).PID(%d).SeqNo(%d)", t.K, t.PID, t.SeqNo)
}

func NewReplica(id int, peerAddrList []string, shardsList []string, shId int,
	thrifty bool, exec bool, dreply bool, durable bool, batch bool, statsFile string) *Replica {
	r := &Replica{
		genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply, false, statsFile),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0,
		false,
		make([]*Instance, 15*1024*1024),
		0,
		-1,
		false,
		0,
		true,
		-1,
		batch,
		make(map[int64]int64),
		make(map[int64][]*genericsmr.MDLPropose),
		true,
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    0, 0,
		shId,
		shardsList,
		make([]net.Conn, len(shardsList)),
		make([]*bufio.Reader, len(shardsList)),
		make([]*bufio.Writer, len(shardsList)),
		nil,
		new(sync.Mutex),

    list.New(),
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    make(map[*mdlinproto.Tag]*genericsmr.MDLCoordReq),
    make(map[*mdlinproto.Tag]*mdlinproto.CoordinationResponse),
    time.NewTimer(EPOCH * time.Second),
    0}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(mdlinproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(mdlinproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(mdlinproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(mdlinproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(mdlinproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(mdlinproto.AcceptReply), r.acceptReplyChan)
  r.finalAcceptRPC = r.RegisterRPC(new(mdlinproto.FinalAccept), r.finalAcceptChan)
  r.finalAcceptReplyRPC = r.RegisterRPC(new(mdlinproto.FinalAcceptReply), r.finalAcceptReplyChan)

	go r.run()

	return r
}

func (r *Replica) connectToShards() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForShardConnections(done)

	//connect to peers
	for i := 0; i < r.shardId; i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.shardAddrList[i]); err == nil {
				r.shards[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.shardId))
		if _, err := r.shards[i].Write(bs); err != nil {
			NewPrintf(LEVEL0, "Write id error: %v", err)
			continue
		}
		r.shardReaders[i] = bufio.NewReader(r.shards[i])
		r.shardWriters[i] = bufio.NewWriter(r.shards[i])

		go r.shardListener(i, r.shardReaders[i])
	}
	<-done
	NewPrintf(LEVEL0, "Shard Leader %d: Done connecting to all shard leaders\n", r.shardId)
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForShardConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	var sherr error
  if len(r.shardAddrList) == 0 {
    done <- true
    return
  }
	r.shListener, sherr = net.Listen("tcp", r.shardAddrList[r.shardId])
	if sherr != nil {
		panic(sherr)
	}
	for i := r.shardId + 1; i < len(r.shardAddrList); i++ {
		conn, err := r.shListener.Accept()
		if err != nil {
			NewPrintf(LEVEL0, "Accept error: %v", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			NewPrintf(LEVEL0, "Connection establish error: %v", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.shards[id] = conn
		r.shardReaders[id] = bufio.NewReader(conn)
		r.shardWriters[id] = bufio.NewWriter(conn)

		go r.shardListener(int(id), r.shardReaders[id])
	}

	done <- true
}

func (r *Replica) shardListener(rid int, reader *bufio.Reader) {
	var msgType uint8
	var err error = nil

	for err == nil && !r.Shutdown {

		if msgType, err = reader.ReadByte(); err != nil { // received a SendMsg(code)
			break
		}

		switch uint8(msgType) {

		case mdlinproto.COORDREQ_REPLY:
			CRR := new(mdlinproto.CoordinationResponse)
			if err = CRR.Unmarshal(reader); err != nil {
				break
			}
			// r.interShardChan <- &genericsmr.RPCMessage{intershard, 0, int64(rid)}
			r.coordReqReplyChan <- fastrpc.Serializable(CRR)
			break

		default:
			panic("mdlin ERROR: received unknown message type")
		}
	}
}

// leaderId is the ID of the leader this message is being sent TO. it's an index
// msg is the actual message being sent of mdlinproto.InterShard* type
func (r *Replica) sendCoordResp(leaderId int32, msg fastrpc.Serializable) {
	r.buflock.Lock()
	defer r.buflock.Unlock()

  NewPrintf(LEVEL0, "sending to %v, we are r.shardId = %v and r.Id = %v", leaderId, r.shardId, r.Id)
	if leaderId != int32(r.shardId) {
		w := r.shardWriters[leaderId]
		w.WriteByte(mdlinproto.COORDREQ_REPLY) // to tell what kind of message this is
		msg.Marshal(w) // marshall the message and send it into the w bufio object
		w.Flush()
	} else {
		NewPrintf(LEVELALL, "SENDING MESSAGE TO SELF!!!!")
		// r.interShardChan <- &genericsmr.RPCMessage{msg, 0, int64(leaderId)}
		r.coordReqReplyChan <- msg
	}
}

// append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

// write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

// sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	r.IsLeader = true
	return nil
}

func (r *Replica) replyPrepare(replicaId int32, reply *mdlinproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyFinalAccept(replicaId int32, reply *mdlinproto.FinalAcceptReply) {
  r.SendMsg(replicaId, r.finalAcceptReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *mdlinproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* ============= */

var clockChan chan bool

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(1000 * 1000 * 5)
		clockChan <- true
	}
}

/* Main event processing loop */

func (r *Replica) run() {

	r.ConnectToPeers()

	go r.WaitForClientConnections()

	go r.executeCommands()

	if r.Id == 0 {
		r.IsLeader = true
		NewPrintf(LEVEL0, "I'm the leader")
	}

	if (r.shardId > -1) && (r.shards != nil) && r.IsLeader {
		r.connectToShards()
	}

	clockChan = make(chan bool, 1)
	if r.batchingEnabled {
		go r.clock()
	}

	onOffProposeChan := r.MDLProposeChan

	for !r.Shutdown {

		select {

    case <-r.timer.C:
      // The epoch has completed, so now we need to do processing
      NewPrintf(LEVEL0, "---------END OF EPOCH-------")
      r.processEpoch()
      r.timer.Reset(EPOCH * time.Second)
      break

		case <-clockChan:
			//activate the new proposals channel
			onOffProposeChan = r.MDLProposeChan
			break

		case propose := <-onOffProposeChan:
			NewPrintf(LEVELALL, "---------ProposalChan---------")
			r.handlePropose(propose)
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			if r.noProposalsReady {
        NewPrintf(LEVEL0, "TURNING OFF PROPOSAL CHAN")
				onOffProposeChan = nil
			}
			break
    case coordinationRequest := <-r.MDLCoordReqChan:
      NewPrintf(LEVEL0, "-----------CoordReq Chan-----------")
      r.handleCoordinationRequest(coordinationRequest)
      break
    case coordinationRReply := <-r.coordReqReplyChan:
      NewPrintf(LEVEL0, "----------CoordReqReply Chan--------")
      CRR := coordinationRReply.(*mdlinproto.CoordinationResponse)
      r.handleCoordinationRReply(CRR)
      break

		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*mdlinproto.Prepare)
			//got a Prepare message
			r.handlePrepare(prepare)
			break

    case finalAcceptS := <-r.finalAcceptChan:
      finalAccept := finalAcceptS.(*mdlinproto.FinalAccept)
      //got a FinalAccept message
      r.handleFinalAccept(finalAccept)
      break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*mdlinproto.Accept)
			//got an Accept message
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*mdlinproto.Commit)
			//got a Commit message
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*mdlinproto.CommitShort)
			//got a Commit message
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*mdlinproto.PrepareReply)
			//got a Prepare reply
			r.handlePrepareReply(prepareReply)
			break

    case finalAcceptReplyS := <-r.finalAcceptReplyChan:
      finalAcceptReply := finalAcceptReplyS.(*mdlinproto.FinalAcceptReply)
      // got a FinalAccept reply
      r.handleFinalAcceptReply(finalAcceptReply)
      break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*mdlinproto.AcceptReply)
			//got an Accept reply
			r.handleAcceptReply(acceptReply)
			break

			// case metricsRequest := <-r.MetricsChan:
			// 	// Empty reply because there are no relevant metrics
			// 	reply := &genericsmrproto.MetricsReply{}
			// 	reply.Marshal(metricsRequest.Reply)
			// 	metricsRequest.Reply.Flush()
			// 	break
		}
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

// indexOL, orderedLog, bufferedLog
func (r *Replica) processEpoch() {
  batch := list.New()
  buffI := 0 // JUST FOR PRINTING TODO
  // scan through the bufferedLog
  p := r.bufferedLog.Front()
  next := p
  for p != nil {
    next = p.Next()
    coord := p.Value.(*Instance).lb.coordinated
    committed := (p.Value.(*Instance).lb.acceptOKs+1) > (r.N>>1)
    NewPrintf(LEVEL0, "Buff log entry %v has coord = %v and committed = %v", buffI, coord, committed)
    if (coord == 1 && committed) {
      // remove ordered entries from the buffer log
      r.bufferedLog.Remove(p)
      batch.PushBack(p.Value)
    } else if (r.epoch - p.Value.(*Instance).epoch > MAX_EPOCHS) {
      // Remove stale entries from buffered log that
      // have not yet been coordinated and committed
      NewPrintf(LEVEL0, "Found stale entry in buff log at index %v with epoch %v, we are in epoch %v, REMOVING IT", buffI, p.Value.(*Instance).epoch, r.epoch)
      r.bufferedLog.Remove(p)
      // we should also send CR=fail to the successor, but
      // it will implicitly fail in this same loop within
      // a few epochs anyway, so we choose not to send it
    }
    p = next
    buffI++
  }
  n := batch.Len()
  if (n > 0) {
    NewPrintf(LEVEL0, "Issueing a final round paxos RTT for epoch %v, with %v commands", r.epoch, n)
    // If some entries were added for this epoch, then
    // (1) add them as an entry in the ordered log
    // (2) issue the bcastAccept for this entry
    b := make([]state.Command, n)
    bi := make([]int32, n)
    bp := make([]*genericsmr.MDLPropose, n)
    cmdids := make([]mdlinproto.Tag, n)
    j := 0
    for p := batch.Front(); p != nil; p = p.Next() {
      b[j] = p.Value.(*Instance).cmds[0]
      bi[j] = p.Value.(*Instance).predSetSize[0]
      bp[j] = p.Value.(*Instance).lb.clientProposals[0]
      cmdids[j] = mdlinproto.Tag{K: p.Value.(*Instance).cmds[0].K, PID: p.Value.(*Instance).pid, SeqNo: p.Value.(*Instance).seqno}
      j++
    }

    // add all ordered entries to the ordered log
    instNo := r.addEntryToOrderedLog(b, bi, bp, PREPARED)

    // do last paxos roundtrip with this whole batch you just added
    r.bcastAccept(instNo, r.defaultBallot, b, -1, -1, TRUE, bi, cmdids)
  }
  // increment the epoch
  r.epoch++
}

func (r *Replica) updateCommittedUpTo() {
	ini := r.committedUpTo
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		(r.instanceSpace[r.committedUpTo+1].status == COMMITTED) {
		r.committedUpTo++
	}
	NewPrintf(LEVEL0, "Updating commit index from %d to %d", ini, r.committedUpTo)
}

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
	NewPrintf(LEVELALL, "Inside broadcast prepare!")
	defer func() {
		if err := recover(); err != nil {
			NewPrintf(LEVEL0, "Prepare bcast failed: %v", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &mdlinproto.Prepare{r.Id, instance, ballot, ti}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.prepareRPC, args)
	}
}

var pa mdlinproto.Accept

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command, pids int64, seqnos int64, fr uint8, ps []int32, cmdids []mdlinproto.Tag) {
	defer func() {
		if err := recover(); err != nil {
			NewPrintf(LEVEL0, "Accept bcast failed: %v", err)
		}
	}()

  NewPrintf(LEVEL0, "BcastAccept with final round == %v", fr)
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = command
	pa.PIDs = pids
	pa.SeqNos = seqnos
  pa.CommandId = cmdids
	// Make a copy of the nextSeqNo map
	//expectedSeqs := make(map[int64]int64)
	//copyMap(expectedSeqs, r.nextSeqNo)
  //pa.ExpectedSeqs = expectedMap
  //TODO what other maps..?
  pa.ExpectedSeqs = r.nextSeqNo // TODO is this correct ??
  pa.Epoch = r.epoch
  pa.FinalRound = fr
  pa.PredSize = ps
	args := &pa

  NewPrintf(LEVEL0, "Broadcasting accept with message %v", pa)
	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1 //n = n//2
	}
	q := r.Id

	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.acceptRPC, args)
	}
}

var pc mdlinproto.Commit
var pcs mdlinproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command, pids int64, seqnos int64, status InstanceStatus, ps []int32) {
	defer func() {
		if err := recover(); err != nil {
			NewPrintf(LEVEL0, "Commit bcast failed: %v", err)
		}
	}()
  NewPrintf(LEVEL0, "Leader calling bcastCommit")
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	pc.PIDs = pids
	pc.SeqNos = seqnos
	pc.Status = uint8(status)
  pc.PredSize = ps
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	pcs.Status = int32(status)
	argsShort := &pcs

	//args := &mdlinproto.Commit{r.Id, instance, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id
	sent := 0

	for sent < n {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			break
		}
		if !r.Alive[q] {
			continue
		}
		sent++
		r.SendMsg(q, r.commitShortRPC, argsShort)
	}
	if r.Thrifty && q != r.Id {
		for sent < r.N-1 {
			q = (q + 1) % int32(r.N)
			if q == r.Id {
				break
			}
			if !r.Alive[q] {
				continue
			}
			sent++
			r.SendMsg(q, r.commitRPC, args)
		}
	}
}

// Client submitted a command to a server
func (r *Replica) handlePropose(propose *genericsmr.MDLPropose) {
	if !r.IsLeader {
		preply := &mdlinproto.ProposeReply{FALSE, propose.CommandId, state.NIL, 0}
		NewPrintf(LEVELALL, "I'm not the leader... responding to client with OK = false (0)")
		r.MDReplyPropose(preply, propose.Reply)
		return
	}

	// Get batch size
	batchSize := 1
	numProposals := len(r.MDLProposeChan) + 1
	if r.batchingEnabled {
		panic("There should be no batching rn")
		batchSize := numProposals + 1
		if batchSize > MAX_BATCH {
			batchSize = MAX_BATCH
		}
	}

	cmds := make([]state.Command, 0)
	proposals := make([]*genericsmr.MDLPropose, 0)

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}
	currInst := r.bufferedLog.Back() //The current last element

	found := 0
	var expectedSeqno int64
	prop := propose
	i := 1
  // i, numProposals = i is a for loop from 1 to numProposals
  // found, batchsize = we use found to bound the number of entries added to batchsize=1 (and flushing buffer)
	for found < batchSize && i <= numProposals {
		pid := prop.PID
		seqno := prop.SeqNo
		expectedSeqno = 0

		if val, ok := r.nextSeqNo[pid]; ok {
			expectedSeqno = val
		}
		if seqno != expectedSeqno {
			// Add to buffer
      panic("We shouldn't be getting OoO reqs per client")
			if _, ok := r.outstandingInst[pid]; !ok {
				r.outstandingInst[pid] = make([]*genericsmr.MDLPropose, 0)
			}
			r.outstandingInst[pid] = append(r.outstandingInst[pid], prop)
			if len(r.outstandingInst[pid]) > 1 {
				mysort.MergeSort(r.outstandingInst[pid])
			}
			NewPrintf(LEVELALL, "Out of order, (got command %d seqno %d) buffering back into channel", prop.CommandId, seqno) //TODO do we need to sort?
		} else {
			cmds = append(cmds, prop.Command)
			proposals = append(proposals, prop)
			found++
			r.nextSeqNo[pid]++

      // If no predecessor, then request is vacuously coordinated
      var coord int8 = -1
      var thisCr *genericsmr.MDLCoordReq = nil
      if (prop.Predecessor.SeqNo == -1) {
        coord = 1
      }

      // Check if coordination request from successor arrived
      // before the request arrived, if so add it
      t := &mdlinproto.Tag{K: prop.Command.K, PID: pid, SeqNo: seqno}
      if v, ok := r.outstandingCR[t]; ok {
        thisCr = v
        delete(r.outstandingCR, t)
      }
      // Check if response from this request's coordination req
      // arrived from predecessor before this req arrived.
      if v, ok := r.outstandingCRR[t]; ok {
        coord = int8(v.OK)
        delete(r.outstandingCRR, t)
      }

			r.addEntryToBuffLog(cmds, proposals, pid, seqno, coord, thisCr, &prop.Predecessor, prop.PredSize) //This seems like a bad idea TODO... the address of a message that's gonna disapear?
			// Check if any others are ready
			for true {
				NewPrintf(LEVELALL, "looking for any others that might be ready from this PID %d", pid)
				l := len(r.outstandingInst[pid])
				NewPrintf(LEVELALL, "apppears there are %d outstanding for this pid", l)
				expectedSeqno = r.nextSeqNo[pid]
				if (l > 0) && (r.outstandingInst[pid][l-1].SeqNo == expectedSeqno) {
          panic("Shouldn't be adding any buffered OoO reqs per client")
					// We found previously outstanding requests that can be replicated now
					prop = r.outstandingInst[pid][l-1]
					r.outstandingInst[pid] = r.outstandingInst[pid][:l-1]
					r.nextSeqNo[pid]++ // Giving us linearizability!
					cmds = make([]state.Command, 1)
					cmds[0] = prop.Command
					proposals = make([]*genericsmr.MDLPropose, 1)
					proposals[0] = prop
          coord = -1
          thisCr = nil
          if (prop.Predecessor.SeqNo == -1) {
            panic("This should never happen...? :D delete me")
          }

          // Check if coordination request from successor arrived
          // before the request arrived, if so add it
          t = &mdlinproto.Tag{K: prop.Command.K, PID: pid, SeqNo: expectedSeqno}
          if v, ok := r.outstandingCR[t]; ok {
            thisCr = v
            delete(r.outstandingCR, t)
          }
          // Check if response from this request's coordination req
          // arrived from predecessor before this req arrived.
          if v, ok := r.outstandingCRR[t]; ok {
            coord = int8(v.OK)
            delete(r.outstandingCRR, t)
          }

					NewPrintf(LEVELALL, "head of it's buff Q is ready, with command %d", prop.CommandId)
					r.addEntryToBuffLog(cmds, proposals, pid, expectedSeqno, coord, thisCr, &prop.Predecessor, prop.PredSize) //This seems like a bad idea TODO... the address of a message that's gonna disapear?
					found++
				} else {
					break
				}
			}
		}
		i++
		if found < batchSize && i <= numProposals {
			NewPrintf(LEVELALL, "--->Pulled out the next one")
			prop = <-r.MDLProposeChan
		}
	}

	// None of the proposals in the channel
	// are ready to be added to the log
	if found == 0 {
		NewPrintf(LEVELALL, "None of the proposals pulled out of the channel or in the buffers are ready!")
		// We won't respond to the client, since that response
		// will come when the command gets unbuffered and later executed
		r.noProposalsReady = true //TODO delete me
		return
	}

	r.noProposalsReady = false

  if (currInst == nil) {
    // If there were no entries in the buffered log at the beginning
    // then all entries added now start at the beginning of bufflog
    currInst = r.bufferedLog.Front()
  } else {
    // otherwise, start with the first entry we just added to bufferedLog
    currInst = currInst.Next()
  }
  for i := 0; i < found; i++ {
		if r.defaultBallot == -1 {
			NewPrintf(LEVEL0, "    Step2. (candidate) leader broadcasting prepares....")
			r.bcastPrepare(int32(r.bufferedLog.Len()-found+i), r.makeUniqueBallot(0), true)
		} else {
			r.recordInstanceMetadata(currInst.Value.(*Instance))
      r.recordCommands(currInst.Value.(*Instance).cmds)
      r.sync()
			NewPrintf(LEVEL0, "    Step2. Leader broadcasting Accepts with instance = %v, ballot = %v", int32(r.bufferedLog.Len()-found+i), r.defaultBallot)
      cmdids := make([]mdlinproto.Tag, 1)
      cmdids[0] = mdlinproto.Tag{K: currInst.Value.(*Instance).cmds[0].K, PID: currInst.Value.(*Instance).pid, SeqNo: currInst.Value.(*Instance).seqno}
			r.bcastAccept(int32(r.bufferedLog.Len()-found+i), r.defaultBallot, currInst.Value.(*Instance).cmds, currInst.Value.(*Instance).pid, currInst.Value.(*Instance).seqno, FALSE, currInst.Value.(*Instance).predSetSize, cmdids)
		}
    currInst = currInst.Next()
	}
}

func (r *Replica) removeEntryFromBuffLog(cmdid []mdlinproto.Tag) {
  p := r.bufferedLog.Front()
  next := p
  for (p != nil) {
    next = p.Next()
    for i:=0; i<len(cmdid); i++ {
      t := cmdid[i]
      if p.Value.(*Instance).pid == t.PID && p.Value.(*Instance).seqno == t.SeqNo && t.K == p.Value.(*Instance).cmds[0].K {
        r.bufferedLog.Remove(p)
      }
    }
    p = next
  }
}

func (r *Replica) addEntryToBuffLog(cmds []state.Command, proposals []*genericsmr.MDLPropose, pid int64,
  seqno int64, coord int8, thisCr *genericsmr.MDLCoordReq, pred *mdlinproto.Tag, predSize int32) {

	// Add entry to log
  NewPrintf(LEVEL0, "addEntryToBuffLog --> Shard Leader Creating Log Entry{%s, PID: %d, SeqNo: %d, coord: %d, thisCr: %v, pred: %v, epoch: %v, PredSize: %v",
		commandToStr(cmds[0]), pid, seqno, coord, thisCr, pred, r.epoch, predSize)

  ball := r.defaultBallot
  stat := PREPARED
  if r.defaultBallot == -1 {
    ball = r.makeUniqueBallot(0)
    stat = PREPARING
  }
  predSetSize := make([]int32, 1)
  predSetSize[0] = predSize
  e := &Instance{
      cmds,
      ball,
      stat,
      &LeaderBookkeeping{proposals, 0, 0, 0, 0, coord},
      pid,
      seqno,
      pred,
      thisCr,
      r.epoch,
      nil,
      nil,
      predSetSize}

  // Insert into linked list
  r.bufferedLog.PushBack(e)
}


func (r *Replica) addEntryToOrderedLog(cmds []state.Command, predSizes []int32, cPs []*genericsmr.MDLPropose, status InstanceStatus) int32 {
	// Add entry to log
	NewPrintf(LEVEL0, "Flushing ready entries buffLog --> orderedLog at END OF EPOCH!")

	r.instanceSpace[r.crtInstance] = &Instance{
		cmds,
		r.defaultBallot,
		status,
    &LeaderBookkeeping{cPs, 0, 0, 0, 0, int8(TRUE)}, // Need this to track acceptOKs
		-1,
		-1,
    nil,
    nil,
    r.epoch,
    nil,
    nil,
    predSizes}
	r.crtInstance++
  return r.crtInstance-1
}

// Helper to copy contents of map2 to map1
func copyMap(map1 map[int64]int64, map2 map[int64]int64) {
	for k, v := range map2 {
		map1[k] = v
	}
}

func (r *Replica) resolveShardFromKey(k state.Key) int32 {
	// Return the shardId of the shard that is responsible for this key
	return int32(state.KeyModulo(k, len(r.shards)))
}

func (r *Replica) findEntry(tag mdlinproto.Tag) (*Instance, int, int32) {
	// TODO what if instead of a linear search, we kept a map
	// that maps the CommandId to the index in the log??
  // First search the buffered linked list log
  for p := r.bufferedLog.Front(); p != nil; p = p.Next() {
    for j := 0; j < len(p.Value.(*Instance).lb.clientProposals); j++ {
      if p.Value.(*Instance).pid == tag.PID && p.Value.(*Instance).seqno == tag.SeqNo && p.Value.(*Instance).lb.clientProposals[j].Command.K == tag.K {
        return p.Value.(*Instance), j, -1
      }
    }
  }

  // Now search the ordered log
	for i := r.crtInstance - 1; i >= 0; i-- {
		if r.instanceSpace[i] == nil {
			panic("instanceSpace at this index should never be nil")
		}
		props := r.instanceSpace[i].lb.clientProposals
		for j := 0; j < len(props); j++ {
			e := props[j]
			if e.Command.K == tag.K && e.PID == tag.PID && e.SeqNo == tag.SeqNo {
				return r.instanceSpace[i], j, i
			}
		}
	}
	return nil, -1, -1
}

// Client sends a CR to the predecessor request (askee), we need to check
// if it's coordinated and committed before responding to the successor (asker)
//
// If it is not both, then we should indicate that it will be waiting for those
// flags to get set, in either the handleAcceptReply or handleCoordinationRReply
// methods, both of which can then issue the sendCoordResp messages to the asker shard.
func (r *Replica) handleCoordinationRequest(cr *genericsmr.MDLCoordReq) {
  e, _, _ := r.findEntry(cr.AskeeTag)
  if (e == nil) {
    panic("Not yet sure what to do in the case where the CR arrives before the request hmm")
    r.outstandingCR[&cr.AskeeTag] = cr //This seems like a bad idea TODO... the address of a message that's gonna disapear?
    // return
  }
  e.cr = cr // won't be nil anymore, signals acceptReply() and coordReply()
  r.checkCoordination(e)
}

func (r *Replica) checkCoordination(e *Instance) {
  //TODO is there another race condition here where it gets removed before it can tell the others to remove themselves..?
  // Need to check whether cr.AskerTag is committed and coordinated!
  coord := e.lb.coordinated
  committed := (e.lb.acceptOKs+1) > (r.N>>1)
  shardTo := e.cr.From
  if (coord == 1 && committed) {
    msg := &mdlinproto.CoordinationResponse{e.cr.AskerTag, e.cr.AskeeTag, int32(r.shardId), 1}
    r.sendCoordResp(shardTo, msg)
  } else if (coord == 0) {
    // Fail the remaining chain of outstanding requests from this client
    msg := &mdlinproto.CoordinationResponse{e.cr.AskerTag, e.cr.AskeeTag, int32(r.shardId), 0}
    r.sendCoordResp(shardTo, msg)
  }
}

// When we receive a response for the predecessor of the asker, we can coordinate
// the asker request
func (r *Replica) handleCoordinationRReply(crr *mdlinproto.CoordinationResponse) {
  e, _, _ := r.findEntry(crr.AskerTag)
  if (e == nil) {
    panic("Received CR Response before request hmm")
    r.outstandingCRR[&crr.AskerTag] = crr //This seems like a bad idea TODO... the address of a message that's gonna disapear?
    // return
  }
  if e.pred.SeqNo == -1 {
    panic("Received CR Response for request with no predecessor")
  }
  e.lb.coordinated = int8(crr.OK)
  // Check if this req's successor (asker's asker) already sent 
  // a CR for this req, but before it was coordinated itself
  if e.cr != nil {
    r.checkCoordination(e)
  }
}

func (r *Replica) readyToCommit(instance int32, ps []int32) {
	inst := r.instanceSpace[instance]
	inst.status = COMMITTED

	r.recordInstanceMetadata(inst)
	r.sync() //is this necessary?

	r.updateCommittedUpTo()

	r.bcastCommit(instance, inst.ballot, inst.cmds, inst.pid, inst.seqno, COMMITTED, ps)
}

func (r *Replica) printLog(level int) {
	for i := 0; int32(i) < r.crtInstance; i++ {
		e := r.instanceSpace[i]
		NewPrintf(level, "Log_entry@index = %d has status %d, and commands...", i, e.status)
		for _, c := range e.cmds {
			NewPrintf(level, commandToStr(c))
		}
	}
}

func commandToStr(c state.Command) string {
	var s string
	if c.Op == state.GET {
		s = fmt.Sprintf("R(%d)", c.K)
	} else {
		s = fmt.Sprintf("W(%d) = %v", c.K, c.V)
	}
	return s
}

func (r *Replica) handlePrepare(prepare *mdlinproto.Prepare) {
  // Now searching in buffered log
  NewPrintf(LEVEL0, "Replica at handlePrepare with prepare.Instance == %v", prepare.Instance)
  e := r.bufferedLog.Front()
  var inst *Instance
  var i int32
  for i = 0; i < prepare.Instance; i++ {
    if (e == nil) {
      inst = nil
      break
    }
    inst = e.Value.(*Instance)
    e = e.Next()
  }
	var preply *mdlinproto.PrepareReply

  if (inst == nil) {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		preply = &mdlinproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
	} else {
		ok := TRUE
		if prepare.Ballot < inst.ballot {
			ok = FALSE
		}
		preply = &mdlinproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
	}

	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleFinalAccept(faccept *mdlinproto.FinalAccept) {
  var fareply *mdlinproto.FinalAcceptReply
  fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, TRUE, r.defaultBallot}
	r.replyFinalAccept(faccept.LeaderId, fareply)
}

func (r *Replica) handleAccept(accept *mdlinproto.Accept) {
	var areply *mdlinproto.AcceptReply
  if accept.FinalRound == TRUE {
    //TODO when do the replicas prune their buffered logs?
    //TODO this is currently not thought out
    inst := r.instanceSpace[accept.Instance]
    if inst != nil {
      if inst.ballot > accept.Ballot {
        panic("final round sees last entry at replica with greater ballot")
      }
      panic("final round sees a last entry at the replica that isn't from the leader sending this appendEntries")
    }
    r.addEntryToOrderedLog(accept.Command, accept.PredSize, nil, ACCEPTED)
    r.removeEntryFromBuffLog(accept.CommandId)
    // TODO do we need to check if the epoch's are correct?
    areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, accept.Ballot, accept.FinalRound, accept.Epoch}
		copyMap(r.nextSeqNo, accept.ExpectedSeqs)
	  r.replyAccept(accept.LeaderId, areply)
    return
  }

  NewPrintf(LEVEL0, "New accept from leader, instance = %v", accept.Instance)
  e := r.bufferedLog.Front()
  var inst *Instance
  var i int32
  for i = 0; i < accept.Instance; i++ {
    if (e == nil) {
      inst = nil
      break
    }
    inst = e.Value.(*Instance)
    e = e.Next()
  }

	expectedSeqs := accept.ExpectedSeqs
  if (inst == nil && i < accept.Instance) {
    panic("We shouldn't be getting accept entries for entries past our log length rn, replica")
    areply = &mdlinproto.AcceptReply{i, FALSE, r.defaultBallot, accept.FinalRound, accept.Epoch}
  } else if inst == nil {
		if accept.Ballot < r.defaultBallot {
			areply = &mdlinproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot, accept.FinalRound, accept.Epoch}
		} else {
      r.addEntryToBuffLog(accept.Command, nil, accept.PIDs, accept.SeqNos, -1, nil, nil, accept.PredSize[0]) // , ACCEPTED) //TODO
			areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot, accept.FinalRound, accept.Epoch}
		}
	} else if inst.ballot > accept.Ballot {
		areply = &mdlinproto.AcceptReply{accept.Instance, FALSE, inst.ballot, accept.FinalRound, accept.Epoch}
	} else if inst.ballot < accept.Ballot {
    panic("Invalid ballot case in handleAccept() at replica")
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, inst.ballot, accept.FinalRound, accept.Epoch}
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//TODO: is this correct?
			// try the proposal in a different instance
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.MDLProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	} else {
		// reordered ACCEPT
    panic("Invalid reorder case in handleAccept() at replica")
		r.instanceSpace[accept.Instance].cmds = accept.Command
		if r.instanceSpace[accept.Instance].status != COMMITTED {
			r.instanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot, accept.FinalRound, accept.Epoch}
	}

	if areply.OK == TRUE {
    NewPrintf(LEVEL0, "Replica %v accepted this request in log", r.Id)
		r.recordInstanceMetadata(inst)
		r.recordCommands(accept.Command)
		r.sync()
		// If we are to accep the Proposal from the leader, we also need to bump up our nextSeqNo
		copyMap(r.nextSeqNo, expectedSeqs)
	}

	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *mdlinproto.Commit) {
	inst := r.instanceSpace[commit.Instance]

	if inst == nil {
    r.addEntryToOrderedLog(commit.Command, commit.PredSize, nil, COMMITTED)
	} else {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = InstanceStatus(commit.Status)
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.MDLProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *mdlinproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	if inst == nil {
    r.addEntryToOrderedLog(nil, nil, nil, COMMITTED)
	} else {
		r.instanceSpace[commit.Instance].status = InstanceStatus(commit.Status)
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.MDLProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
}

func (r *Replica) handlePrepareReply(preply *mdlinproto.PrepareReply) {

  NewPrintf(LEVEL0, "handlePrepareReply, prepare.Instance = %v", preply.Instance)
  e := r.bufferedLog.Front()
  var inst *Instance
  var i int32
  for i = 0; i < preply.Instance; i++ {
    if (e == nil) {
      inst = nil
      break
    }
    inst = e.Value.(*Instance)
    e = e.Next()
  }

  if (inst == nil) {
    panic("Got index out of bounds at leader in prepareReply")
  }

	if inst.status != PREPARING {
		// TODO: should replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		inst.lb.prepareOKs++

		if preply.Ballot > inst.lb.maxRecvBallot { //TODO CHANGE THIS THIS WILL BREAK MDL
			panic("This shouldn't be happening rn")
			inst.cmds = preply.Command
			inst.lb.maxRecvBallot = preply.Ballot
			if inst.lb.clientProposals != nil {
				// there is already a competing command for this instance,
				// so we put the client proposal back in the queue so that
				// we know to try it in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.MDLProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}

		// Don't need to change anything for MDL, just issue bcast Accept
		// as usual and let the number of accepts compete with the ISRT replies
		if inst.lb.prepareOKs+1 > r.N>>1 {
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				r.defaultBallot = inst.ballot
			}
			r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
			r.sync()
      cmdids := make([]mdlinproto.Tag, 1)
      cmdids[0] = mdlinproto.Tag{K: inst.cmds[0].K, PID: inst.pid, SeqNo: inst.seqno}
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds, inst.pid, inst.seqno, FALSE, inst.predSetSize, cmdids)
		}
	} else {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = preply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			if inst.lb.clientProposals != nil {
				// try the proposals in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.MDLProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}
	}
}

func (r *Replica) handleFinalAcceptReply(fareply *mdlinproto.FinalAcceptReply) {
}

func (r *Replica) handleAcceptReply(areply *mdlinproto.AcceptReply) {
  NewPrintf(LEVEL0, "got RESPONSE to accept %v", areply.OK)
  var inst *Instance
  if areply.FinalRound == TRUE {
    // check whether this has been a quorum of acks, incrememnet the execution index
    if areply.OK != TRUE {
      panic("Didn't get success for handleaccept reply...")
    }
    inst = r.instanceSpace[areply.Instance]
  } else {
    e := r.bufferedLog.Front()
    var i int32
    for i = 0; i < areply.Instance; i++ {
      if (e == nil) {
        inst = nil
        break
      }
      inst = e.Value.(*Instance)
      e = e.Next()
    }

    if (e == nil) {
      panic("leader got index out of bounds in accept reply")
    }
  }

	if inst.status != PREPARED && inst.status != ACCEPTED {
		// The status is COMMITTED
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if areply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {
      // Check if the successor already sent a CR for this req,
      // but before it was committed itself
      if (areply.FinalRound == TRUE) {
        NewPrintf(LEVEL0, "FINAL ROUND Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
        r.readyToCommit(areply.Instance, inst.predSetSize)
      } else {
			  NewPrintf(LEVEL0, "Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
        if (inst.cr != nil) {
          r.checkCoordination(inst)
        }
      }
      //TODO r.readyToCommit(acceptReply.Instance)
		}
	} else {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			// TODO
		}
	}
}

func (r *Replica) executeCommands() {
	i := int32(0)
	for !r.Shutdown {
		executed := false

		for i <= r.committedUpTo {
			if r.instanceSpace[i].cmds != nil {
				inst := r.instanceSpace[i]
				NewPrintf(LEVELALL, "Number of commands in this entry is %d", len(inst.cmds))
        mysort.PredSort(inst.predSetSize, inst.cmds) // SORT
				for j := 0; j < len(inst.cmds); j++ {
					// If an instands has multiple commands (a batch)
					// they will get executed in sorted order.
					// This maintains MDL
					val := inst.cmds[j].Execute(r.State)
					if inst.lb.clientProposals != nil {
						propreply := &mdlinproto.ProposeReply{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							17}

						NewPrintf(LEVEL0, "EXECUTING --> CLIENT:OK = TRUE, CommandID = %d, val = %v, key = %d, seqno = %d, PID = %dHA", inst.lb.clientProposals[j].CommandId, val, inst.lb.clientProposals[j].Command.K, inst.lb.clientProposals[j].SeqNo, inst.lb.clientProposals[j].PID)

            r.MDReplyPropose(propreply, inst.lb.clientProposals[j].Reply)
					} else {
            NewPrintf(LEVEL0, "REPLICAS EXECUTING!!")
          }
				}
				i++
				executed = true
			} else {
				break
			}
		}

		if !executed {
			time.Sleep(1000 * 1000)
		}
	}

}
