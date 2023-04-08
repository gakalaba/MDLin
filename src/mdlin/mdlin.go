package mdlin

import (
  "math"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"mdlinproto"
	"mysort"
	"state"
	"time"
  "container/list"
  "masterproto"
	"net/rpc"
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

const EPOCH = 1000
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
	instanceSpace       []*Instance // the space of all instances (used and not yet used)
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
	//shardId             int
	//shardAddrList       []string
	//shards              []net.Conn // cache of connections to all other replicas
	//shardReaders        []*bufio.Reader
	//shardWriters        []*bufio.Writer
	//shListener          net.Listener
	//buflock             *sync.Mutex new(sync.Mutex)

  bufferedLog         *list.List  // the unordered requests LINKED LIST
  coordReqReplyChan   chan fastrpc.Serializable
  coordResponseRPC    uint8
  outstandingCR       map[mdlinproto.Tag]*genericsmr.MDLCoordReq
  outstandingCRR      map[mdlinproto.Tag]*mdlinproto.CoordinationResponse

  timer               *time.Timer
  epoch               int64
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
  epoch      []int64
  // For linked list
  prev *Instance
  next *Instance
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

func NewReplica(id int, peerAddrList []string, masterAddr string, masterPort int,
	thrifty bool, exec bool, dreply bool, durable bool, batch bool, statsFile string, numShards int) *Replica {
	r := &Replica{
		genericsmr.NewReplica(id, peerAddrList, numShards, thrifty, exec, dreply, false, statsFile),
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
    list.New(),
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    0,
    make(map[mdlinproto.Tag]*genericsmr.MDLCoordReq),
    make(map[mdlinproto.Tag]*mdlinproto.CoordinationResponse),
    time.NewTimer(EPOCH * time.Millisecond),
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
  r.coordResponseRPC = r.RegisterRPC(new(mdlinproto.CoordinationResponse), r.coordReqReplyChan)

	go r.run(masterAddr, masterPort)

	return r
}

func (r *Replica) getShardsFromMaster(masterAddr string) []string {
	var args masterproto.GetShardListArgs
	var reply masterproto.GetShardListReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.GetShardList", &args, &reply)
			if err == nil {
				done = true
				break
			}
		}
	}
	r.ShardAddrList = reply.ShardList
	r.ShardId = reply.ShardId
	return reply.ShardList
}

func (r *Replica) setupShards(masterAddr string, masterPort int) {
	if !r.IsLeader {
		return
	}
	// Get the shards for multi-sharded MD-Lin
	r.getShardsFromMaster(fmt.Sprintf("%s:%d", masterAddr, masterPort))

	log.Printf("-->Shard %d leader is ready!", r.ShardId)
	r.ConnectToShards()

	var args masterproto.ShardReadyArgs
	var reply masterproto.ShardReadyReply

	for done := false; !done; {
                mcli, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", masterAddr, masterPort))
                if err == nil {
                        err = mcli.Call("Master.ShardReady", &args, &reply)
                        if err == nil {
                                done = true
                                break
                        } else {
				log.Printf("%v", err)
			}
                } else {
			log.Printf("%v", err)
		}
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

// leaderId is the ID of the leader this message is being sent TO. it's an index
// msg is the actual message being sent of mdlinproto.InterShard* type
func (r *Replica) replyCoord(replicaId int32, reply *mdlinproto.CoordinationResponse) {
	if replicaId == int32(r.ShardId) {
		r.coordReqReplyChan <- reply
	} else {
		r.SendISMsg(replicaId, r.coordResponseRPC, reply)
	}
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

func (r *Replica) run(masterAddr string, masterPort int) {
	if r.Id == 0 {
		r.IsLeader = true
		NewPrintf(LEVEL0, "I'm the leader")
	}
  if (!r.IsLeader) {
    // non leaders don't need to run epochs
    r.timer.Stop()
  }
	r.ConnectToPeers()
	r.setupShards(masterAddr, masterPort)
	go r.WaitForClientConnections()

	go r.executeCommands()

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
    if (r.Id == 0) {
      NewPrintf(LEVEL0, "Buff[%v] w/ CommandId %v has coord = %v and committed = %v", buffI, p.Value.(*Instance).lb.clientProposals[0].CommandId, coord, committed)
    }
    if (coord == 1 && committed) {
      // remove ordered entries from the buffer log
      r.bufferedLog.Remove(p)
      batch.PushBack(p.Value)
      if (p.Value.(*Instance).epoch[0] > r.epoch) {
        panic("This shouldn't happen... CC buffered entry has larger epoch than shard")
      }
      r.epoch = int64(math.Max(float64(r.epoch), float64(p.Value.(*Instance).epoch[0])))
    } else if (r.epoch - p.Value.(*Instance).epoch[0] > MAX_EPOCHS) {
      // Remove stale entries from buffered log that
      // have not yet been coordinated and committed
      
      //NewPrintf(LEVEL0, "Found stale entry in buff log at index %v with epoch %v, we are in epoch %v, REMOVING IT", buffI, p.Value.(*Instance).epoch, r.epoch)
      //r.bufferedLog.Remove(p) TODO add field called 'epochsSeen'
      
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
    bi := make([]int64, n)
    bp := make([]*genericsmr.MDLPropose, n)
    cmdids := make([]mdlinproto.Tag, n)
    j := 0
    for p := batch.Front(); p != nil; p = p.Next() {
      b[j] = p.Value.(*Instance).cmds[0]
      bi[j] = p.Value.(*Instance).epoch[0]
      bp[j] = p.Value.(*Instance).lb.clientProposals[0]
      cmdids[j] = mdlinproto.Tag{K: p.Value.(*Instance).cmds[0].K, PID: p.Value.(*Instance).pid, SeqNo: p.Value.(*Instance).seqno}
      j++
    }

    // add all ordered entries to the ordered log
    instNo := r.addEntryToOrderedLog(r.crtInstance, b, bi, bp, PREPARED)
    r.crtInstance++
    // do last paxos roundtrip with this whole batch you just added
    r.bcastFinalAccept(instNo, r.defaultBallot, cmdids, bi)
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

var fpa mdlinproto.FinalAccept

func (r *Replica) bcastFinalAccept(instance int32, ballot int32, cmdids []mdlinproto.Tag, es []int64) {
	defer func() {
		if err := recover(); err != nil {
			NewPrintf(LEVEL0, "Accept bcast failed: %v", err)
		}
	}()

  NewPrintf(LEVELALL, "BcastFinalAccept")
	fpa.LeaderId = r.Id
	fpa.Instance = instance
	fpa.Ballot = ballot
  fpa.CmdTags = cmdids
	// Make a copy of the nextSeqNo map
	//expectedSeqs := make(map[int64]int64)
	//copyMap(expectedSeqs, r.nextSeqNo)
  //pa.ExpectedSeqs = expectedMap
  //TODO what other maps..?
  //TODO this is broken cuz rn the leader isn't (but should)
  // sending the epochs.. which it needs to do because they are
  // updated values from what the replicas have since the entries
  // changed them once they got coordinated!
  fpa.ExpectedSeqs = r.nextSeqNo
  fpa.EpochSize = es
	args := &fpa

  NewPrintf(LEVELALL, "Broadcasting accept with message %v", fpa)
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
		r.SendMsg(q, r.finalAcceptRPC, args)
	}
}
var pa mdlinproto.Accept

func (r *Replica) bcastAccept(ballot int32, command []state.Command, pids int64, seqnos int64, es int64) {
	defer func() {
		if err := recover(); err != nil {
			NewPrintf(LEVEL0, "Accept bcast failed: %v", err)
		}
	}()

  // We don't replicate the coordinated bit!!!
  // A new leader has to undergo the expensive
  // procedure of re-coordinating all requests
  // TODO currently not including the predecessor
  // in the Accept message... but we should!!
  NewPrintf(LEVELALL, "BcastAccept regular")
	pa.LeaderId = r.Id
	pa.Ballot = ballot
	pa.Command = command
	pa.PIDs = pids
	pa.SeqNos = seqnos
	// Make a copy of the nextSeqNo map
	//expectedSeqs := make(map[int64]int64)
	//copyMap(expectedSeqs, r.nextSeqNo)
  //pa.ExpectedSeqs = expectedMap
  //TODO what other maps..?
  pa.ExpectedSeqs = r.nextSeqNo
  pa.Epoch = es
	args := &pa

  NewPrintf(LEVELALL, "Broadcasting accept with message %v", pa)
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

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command, pids int64, seqnos int64, status InstanceStatus, es []int64) {
	defer func() {
		if err := recover(); err != nil {
			NewPrintf(LEVEL0, "Commit bcast failed: %v", err)
		}
	}()
  NewPrintf(LEVELALL, "Leader calling bcastCommit")
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	pc.PIDs = pids
	pc.SeqNos = seqnos
	pc.Status = uint8(status)
  pc.EpochSize = es
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
			NewPrintf(LEVELALL, "Out of order, (got command %d seqno %d) buffering back into channel", prop.CommandId, seqno)
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
      t := mdlinproto.Tag{K: prop.Command.K, PID: pid, SeqNo: seqno}
      if v, ok := r.outstandingCR[t]; ok {
        NewPrintf(LEVEL0, "^^^^^^^^^^^^found an awaiting CR for %v", t)
        thisCr = v
        delete(r.outstandingCR, t)
      }
      // Check if response from this request's coordination req
      // arrived from predecessor before this req arrived.
      if v, ok := r.outstandingCRR[t]; ok {
        NewPrintf(LEVEL0, "!!!!!!!!!!!!!!found an awaiting CRR for %v", t)
        coord = int8(v.OK)
        delete(r.outstandingCRR, t)
      }

			r.addEntryToBuffLog(cmds, proposals, pid, seqno, coord, thisCr, &prop.Predecessor, r.epoch) //This seems like a bad idea TODO... the address of a message that's gonna disapear?
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
          t = mdlinproto.Tag{K: prop.Command.K, PID: pid, SeqNo: expectedSeqno}
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
					r.addEntryToBuffLog(cmds, proposals, pid, expectedSeqno, coord, thisCr, &prop.Predecessor, r.epoch) //This seems like a bad idea TODO... the address of a message that's gonna disapear?
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
		r.noProposalsReady = true
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
			NewPrintf(LEVELALL, "    Step2. (candidate) leader broadcasting prepares....")
			r.bcastPrepare(int32(r.bufferedLog.Len()-found+i+1), r.makeUniqueBallot(0), true)
		} else {
			r.recordInstanceMetadata(currInst.Value.(*Instance))
      r.recordCommands(currInst.Value.(*Instance).cmds)
      r.sync()
			NewPrintf(LEVELALL, "    Step2. Leader broadcasting Accepts with instance = %v, ballot = %v", int32(r.bufferedLog.Len()-found+i), r.defaultBallot)
      cmdids := make([]mdlinproto.Tag, 1)
      cmdids[0] = mdlinproto.Tag{K: currInst.Value.(*Instance).cmds[0].K, PID: currInst.Value.(*Instance).pid, SeqNo: currInst.Value.(*Instance).seqno}
			r.bcastAccept(r.defaultBallot, currInst.Value.(*Instance).cmds, currInst.Value.(*Instance).pid, currInst.Value.(*Instance).seqno, currInst.Value.(*Instance).epoch[0])
		}
    currInst = currInst.Next()
	}
}

func (r *Replica) addEntryToBuffLog(cmds []state.Command, proposals []*genericsmr.MDLPropose, pid int64,
  seqno int64, coord int8, thisCr *genericsmr.MDLCoordReq, pred *mdlinproto.Tag, ep int64) {

	// Add entry to log
  NewPrintf(LEVEL0, "addEntryToBuffLog --> Shard Leader Creating Log Entry{%s, PID: %d, SeqNo: %d, coord: %d, thisCr: %v, pred: %v, epoch: %v",
		commandToStr(cmds[0]), pid, seqno, coord, thisCr, pred, r.epoch)

  ball := r.defaultBallot
  stat := PREPARED
  if r.defaultBallot == -1 {
    ball = r.makeUniqueBallot(0)
    stat = PREPARING
  }
  thisEpoch := make([]int64, 1)
  thisEpoch[0] = ep
  e := &Instance{
      cmds,
      ball,
      stat,
      &LeaderBookkeeping{proposals, 0, 0, 0, 0, coord},
      pid,
      seqno,
      pred,
      thisCr,
      thisEpoch,
      nil,
      nil}

  // Insert into linked list
  r.bufferedLog.PushBack(e)
}


func (r *Replica) addEntryToOrderedLog(index int32, cmds []state.Command, epochSizes []int64, cPs []*genericsmr.MDLPropose, status InstanceStatus) int32 {
	// Add entry to log
	NewPrintf(LEVEL0, "Flushing ready entries buffLog --> orderedLog at END OF EPOCH!")

	r.instanceSpace[index] = &Instance{
		cmds,
		r.defaultBallot,
		status,
    &LeaderBookkeeping{cPs, 0, 0, 0, 0, int8(TRUE)}, // Need this to track acceptOKs
		-1,
		-1,
    nil,
    nil,
    epochSizes,
    nil,
    nil}
  return index
}

// Helper to copy contents of map2 to map1
func copyMap(map1 map[int64]int64, map2 map[int64]int64) {
	for k, v := range map2 {
		map1[k] = v
	}
}

func (r *Replica) resolveShardFromKey(k state.Key) int32 {
	// Return the shardId of the shard that is responsible for this key
	return int32(state.KeyModulo(k, len(r.Shards)))
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
// methods, both of which can then issue the replyCoord messages to the asker shard.
func (r *Replica) handleCoordinationRequest(cr *genericsmr.MDLCoordReq) {
  NewPrintf(LEVEL0, "##########Received coord req for %v from shard %v", cr.AskeeTag, cr.From)
  e, _, _ := r.findEntry(cr.AskeeTag)
  if (e == nil) {
    NewPrintf(LEVEL0, "Coordination Request arrived before the predecessor did")
    r.outstandingCR[cr.AskeeTag] = cr //This seems like a bad idea TODO... the address of a message that's gonna disapear?
    return
  }
  e.cr = cr // won't be nil anymore, signals acceptReply() and coordReply()
  OK, CC := r.checkCoordination(e)
  if OK {
    shardTo := e.cr.From
    // Send this req's epoch to the successor
    msg := &mdlinproto.CoordinationResponse{e.cr.AskerTag, e.cr.AskeeTag, e.epoch[0], int32(r.ShardId), CC}
    r.replyCoord(shardTo, msg)
  }
}

func (r *Replica) checkCoordination(e *Instance) (bool, uint8) {
  //TODO is there another race condition here where it gets removed before it can tell the others to remove themselves..?
  // Need to check whether cr.AskerTag is committed and coordinated!
  coord := e.lb.coordinated
  committed := (e.lb.acceptOKs+1) > (r.N>>1)
  if (coord == 1 && committed) {
    return true, 1
  }
  if (coord == 0) {
    return true, 0
  }
  return false, 0
}

// When we receive a response for the predecessor of the asker, we can coordinate
// the asker request
func (r *Replica) handleCoordinationRReply(crr *mdlinproto.CoordinationResponse) {
  NewPrintf(LEVEL0, "@@@@@@@@@@@Received coord ReSPONSE for %v with OK = %v", crr.AskerTag, crr.OK)
  e, _, _ := r.findEntry(crr.AskerTag)
  if (e == nil) {
    NewPrintf(LEVEL0, "Coordination Response arrived before the request did")
    r.outstandingCRR[crr.AskerTag] = crr //This seems like a bad idea TODO... the address of a message that's gonna disapear?
    return
  }
  if e.pred.SeqNo == -1 {
    panic("Received CR Response for request with no predecessor")
  }
  e.lb.coordinated = int8(crr.OK)
  OK, CC := r.checkCoordination(e)
  if (OK && CC==1) {
    // Set my epoch to the max of sender and mine
    e.epoch[0] = int64(math.Max(float64(e.epoch[0]), float64(crr.AskeeEpoch))) + 1
    // So that incoming reqs in real time are ordered after you
    r.epoch = int64(math.Max(float64(r.epoch), float64(e.epoch[0])))
  }
  // Check if this req's successor (asker's asker) already sent 
  // a CR for this req, but before it was coordinated itself
  if (OK && e.cr != nil) {
    shardTo := e.cr.From
    msg := &mdlinproto.CoordinationResponse{e.cr.AskerTag, e.cr.AskeeTag, e.epoch[0], int32(r.ShardId), CC}
    r.replyCoord(shardTo, msg)
  }
}

func (r *Replica) readyToCommit(instance int32) {
	inst := r.instanceSpace[instance]
	inst.status = COMMITTED

	r.recordInstanceMetadata(inst)
	r.sync() //is this necessary?

	r.updateCommittedUpTo()

	r.bcastCommit(instance, inst.ballot, inst.cmds, inst.pid, inst.seqno, COMMITTED, inst.epoch)
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
  NewPrintf(LEVELALL, "Replica at handlePrepare with prepare.Instance == %v", prepare.Instance)
	var preply *mdlinproto.PrepareReply

	ok := TRUE
	if r.defaultBallot > prepare.Ballot {
    ok = FALSE
	}
	preply = &mdlinproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *mdlinproto.Accept) {
  var areply *mdlinproto.AcceptReply
  if (accept.Ballot < r.defaultBallot) {
    t := mdlinproto.Tag{K: -1, PID: -1, SeqNo: -1}
    areply = &mdlinproto.AcceptReply{FALSE, r.defaultBallot, t}
  } else {
    t := mdlinproto.Tag{K: accept.Command[0].K, PID: accept.PIDs, SeqNo: accept.SeqNos}
    // could add predecessor Req to Accept message type so that new elected leader can issue coordReq!
    r.addEntryToBuffLog(accept.Command, nil, accept.PIDs, accept.SeqNos, -1, nil, nil, accept.Epoch)
    areply = &mdlinproto.AcceptReply{TRUE, r.defaultBallot, t}
  }

  if areply.OK == TRUE {
    NewPrintf(LEVEL0, "Replica %v added this request to buffLog", r.Id)
    copyMap(r.nextSeqNo, accept.ExpectedSeqs)
  }
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleFinalAccept(faccept *mdlinproto.FinalAccept) {
  var fareply *mdlinproto.FinalAcceptReply

  NewPrintf(LEVELALL, "New FINAL Accept from leader, instance = %v", faccept.Instance)
  inst := r.instanceSpace[faccept.Instance]
  if inst != nil {
    panic("No failures happening yet, so we shouldn't be hitting this case")
    if inst.ballot > faccept.Ballot {
      fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, FALSE, inst.ballot}
    } else if inst.ballot < faccept.Ballot {
      inst.ballot = faccept.Ballot
      inst.status = ACCEPTED
      fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, TRUE, faccept.Ballot}
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
      r.instanceSpace[faccept.Instance].cmds = nil //accept.Command
      if r.instanceSpace[faccept.Instance].status != COMMITTED {
        r.instanceSpace[faccept.Instance].status = ACCEPTED
      }
      fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, TRUE, r.defaultBallot}
    }
  } else {
    if faccept.Ballot < r.defaultBallot {
      fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, FALSE, r.defaultBallot}
    } else {
      p := r.bufferedLog.Front()
      next := p
      n := len(faccept.CmdTags)
      b := make([]state.Command, n)
      bi := make([]int64, n)
      j := 0
      for (p != nil) {
        next = p.Next()
        for i, e := range faccept.CmdTags {
          if p.Value.(*Instance).pid == e.PID && p.Value.(*Instance).seqno == e.SeqNo && p.Value.(*Instance).cmds[0].K == e.K {
            r.bufferedLog.Remove(p)
            b[j] = p.Value.(*Instance).cmds[0]
            bi[j] = faccept.EpochSize[i]
            j++
          }
        }
        p = next
      }
      if j == n {
        r.addEntryToOrderedLog(faccept.Instance, b, bi, nil, ACCEPTED)
        fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, TRUE, faccept.Ballot}
      } else {
        panic("This replica didn't have all the entries buffered that the leader sent out in FinalAccept")
        fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, FALSE, faccept.Ballot}
      }
    }
  }

	if fareply.OK == TRUE {
    NewPrintf(LEVELALL, "Replica %v accepted this request in OrderedLog", r.Id)
		r.recordInstanceMetadata(r.instanceSpace[faccept.Instance])
		r.recordCommands(r.instanceSpace[faccept.Instance].cmds)
		r.sync()
		// If we are to accep the Proposal from the leader, we also need to bump up our nextSeqNo
		copyMap(r.nextSeqNo, faccept.ExpectedSeqs)
	}

	r.replyFinalAccept(faccept.LeaderId, fareply)
}

func (r *Replica) handleCommit(commit *mdlinproto.Commit) {
	inst := r.instanceSpace[commit.Instance]

	if inst == nil {
    r.addEntryToOrderedLog(commit.Instance, commit.Command, commit.EpochSize, nil, COMMITTED)
	} else {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = InstanceStatus(commit.Status)
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
    r.instanceSpace[commit.Instance].epoch = commit.EpochSize
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.MDLProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

  //TODO right now the replicas don't increment their local epoch values
  // but really they don't need to... the new leader can reconstruct the
  // new epoch value from the latest committed value and then after the
  // procedure of processing the remaining buffered entries across a quorum
	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *mdlinproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	NewPrintf(LEVEL0, "Replica %d is getting handleCommitShort", r.Id)
	if inst == nil {
    r.addEntryToOrderedLog(commit.Instance, nil, nil, nil, COMMITTED)
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

  NewPrintf(LEVELALL, "handlePrepareReply, prepare.Instance = %v", preply.Instance)
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

		if preply.Ballot > inst.lb.maxRecvBallot {
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
			r.bcastAccept(inst.ballot, inst.cmds, inst.pid, inst.seqno, inst.epoch[0])
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

func (r *Replica) handleAcceptReply(areply *mdlinproto.AcceptReply) {
  NewPrintf(LEVELALL, "got RESPONSE to (regular) accept %v", areply.OK)
  if areply.OK != TRUE {
    panic("Replica didn't accept buffered appendEntries?")
  }

  inst,_,_ := r.findEntry(areply.IdTag)
  if inst == nil {
    panic("Got handleAcceptReply for entry we don't have in our logs")
  }
  inst.lb.acceptOKs++
  if inst.lb.acceptOKs+1 > r.N>>1 {
    NewPrintf(LEVEL0, "Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
    OK, CC := r.checkCoordination(inst)
    if OK && (CC==1) {
      // So that incoming reqs in real time are ordered after you
      r.epoch = int64(math.Max(float64(r.epoch), float64(inst.epoch[0])))
    }
    if (OK && inst.cr != nil) {
      shardTo := inst.cr.From
      // Send this req's epoch+1 to the successor
      msg := &mdlinproto.CoordinationResponse{inst.cr.AskerTag, inst.cr.AskeeTag, inst.epoch[0], int32(r.ShardId), CC}
      r.replyCoord(shardTo, msg)
    }
  }
}

func (r *Replica) handleFinalAcceptReply(fareply *mdlinproto.FinalAcceptReply) {
  NewPrintf(LEVELALL, "got RESPONSE to FINAL accept %v", fareply.OK)
  inst := r.instanceSpace[fareply.Instance]

	if inst.status != PREPARED && inst.status != ACCEPTED {
		// The status is COMMITTED
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if fareply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {
      // Check if the successor already sent a CR for this req,
      // but before it was committed itself
      NewPrintf(LEVELALL, "FINAL ROUND Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
      r.readyToCommit(fareply.Instance)
		}
	} else {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if fareply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = fareply.Ballot
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
        mysort.EpochSort(inst.epoch, inst.cmds) // SORT
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
