package mdlin

import (
  "math"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"genericsmr"
	"genericsmrproto"
	"io"
	"dlog"
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
const LEVEL0 = 1
const LEVELALL = 3

const MAX_EPOCHS = 3
const EPOCH_LENGTH = 500

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
  epochBatching       bool
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
	//buflock             *sync.Mutex // new(sync.Mutex)

  bufferedLog         map[mdlinproto.Tag]*Instance  // the unordered requests LINKED LIST
  readyBuff           *list.List
  coordReqReplyChan   chan fastrpc.Serializable
  coordResponseRPC    uint8
  outstandingCR       map[mdlinproto.Tag]*genericsmr.MDLCoordReq
  outstandingCRR      map[mdlinproto.Tag]*mdlinproto.CoordinationResponse

  //ticker               *time.Ticker
  //timer               *time.Timer
  epochlen            int
  epoch               int64
  seen                map[mdlinproto.Tag]*Instance
  printMap		map[int32]int
  totalEpochs	      int
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
  absoluteEpoch int
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

func NewReplica(id int, peerAddrList []string, masterAddr string, masterPort int, thrifty bool,
	exec bool, dreply bool, durable bool, batch bool, epBatch bool, statsFile string, numShards int, epochLength int) *Replica {
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
    epBatch,
		make(map[int64]int64),
		make(map[int64][]*genericsmr.MDLPropose),
		true,
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    0, 0,
    make(map[mdlinproto.Tag]*Instance, genericsmr.CHAN_BUFFER_SIZE),
    list.New(),
    make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
    0,
    make(map[mdlinproto.Tag]*genericsmr.MDLCoordReq),
    make(map[mdlinproto.Tag]*mdlinproto.CoordinationResponse),
    //time.NewTicker(time.Duration(epochLength) * time.Microsecond),
    //time.NewTimer(time.Duration(epochLength) * time.Microsecond),
    epochLength,
    0,
    make(map[mdlinproto.Tag]*Instance),
    make(map[int32]int),
    0}
        dlog.Printf("first round batching = %v, 2nd round = %v\n", batch, epBatch)

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
	dlog.Printf("H\n")
	if !r.IsLeader {
		return
	}
	dlog.Printf("HH\n")
	// Get the shards for multi-sharded MD-Lin
	r.getShardsFromMaster(fmt.Sprintf("%s:%d", masterAddr, masterPort))

	dlog.Printf("-->Shard %d leader is ready!", r.ShardId)
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

func (r *Replica) batchClock(proposeDone *(chan bool)) {
  for !r.Shutdown {
    dlog.Printf("batchClock sleeping... %v\n", time.Now().UnixMilli())
    time.Sleep(time.Duration(r.epochlen) * time.Microsecond)
    (*proposeDone) <- true
    //dlog.Printf("!!!Pulled of proposeDone\n")
  }
}
func (r *Replica) epochClock(proposeChan *(chan bool), proposeDone *(chan bool)) {
  for !r.Shutdown {
	  dlog.Printf("batchClock sleeping... %v\n", time.Now().UnixMilli())
    time.Sleep(time.Duration(r.epochlen) * time.Microsecond)
    (*proposeChan) <- true
    <-(*proposeDone)
    dlog.Printf("!!!Pulled of epochDone\n")
  }
}

/* Main event processing loop */

func (r *Replica) run(masterAddr string, masterPort int) {
	if r.Id == 0 {
		r.IsLeader = true
		//NewPrintf(LEVEL0, "I'm the leader")
	}
	r.ConnectToPeers()
	dlog.Printf("about to call setupshards\n")
	r.setupShards(masterAddr, masterPort)
	dlog.Printf("ANJA\n")
	go r.WaitForClientConnections()

	go r.executeCommands()
	proposeChan := r.MDLProposeChan
	proposeDone := make(chan bool, 1)
	if r.batchingEnabled && r.IsLeader {
		proposeChan = nil
		dlog.Printf("proposeChan = nil\n");
		go r.batchClock(&proposeDone)
	}

	var epochChan chan bool = nil
	epochDone := make(chan bool, 1)
	if r.epochBatching && r.IsLeader {
		epochChan = make(chan bool, 1)
		dlog.Printf("IS THIS THING ON????\n")
		go r.epochClock(&epochChan, &epochDone)
	}

	for !r.Shutdown {
		//dlog.Printf("A\n")
		//if proposeChan == nil {
		//	dlog.Printf("propose han = nil\n")
		//} else {
		//	dlog.Printf("ProposeChan has length = %v\n", len(proposeChan))
		//}
		select {
		case <-proposeDone:
			proposeChan = r.MDLProposeChan
			break
		case proposal := <-proposeChan:
			NewPrintf(LEVELALL, "---------ProposalChan---------")
			dlog.Printf("handling propose at time %v\n", time.Now().UnixMilli())
			dlog.Printf("1\n")
			r.handlePropose(proposal)
			if r.batchingEnabled {
				proposeChan = nil
				dlog.Printf("main thread set propose=nil, proposeDone <- true.... %v\n", time.Now().UnixMilli())
			}
			break
		case <-epochChan:
			dlog.Printf("2\n")
			r.processEpoch()
			epochDone <- true
			break
		case prepareS := <-r.prepareChan:
			dlog.Printf("3\n")
			prepare := prepareS.(*mdlinproto.Prepare)
			//got a Prepare message
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			dlog.Printf("4\n")
			accept := acceptS.(*mdlinproto.Accept)
			//got an Accept message
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			dlog.Printf("5\n")
			commit := commitS.(*mdlinproto.Commit)
			//got a Commit message
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			dlog.Printf("6\n")
			commit := commitS.(*mdlinproto.CommitShort)
			//got a Commit message
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			dlog.Printf("7\n")
			prepareReply := prepareReplyS.(*mdlinproto.PrepareReply)
			//got a Prepare reply
			r.handlePrepareReply(prepareReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			dlog.Printf("8\n")
			acceptReply := acceptReplyS.(*mdlinproto.AcceptReply)
			//got an Accept reply
			r.handleAcceptReply(acceptReply)
			break
		case finalAcceptS := <-r.finalAcceptChan:
			dlog.Printf("9\n")
			finalAccept := finalAcceptS.(*mdlinproto.FinalAccept)
			//got a FinalAccept message
			r.handleFinalAccept(finalAccept)
			break
		case coordinationRequest := <-r.MDLCoordReqChan:
			dlog.Printf("10\n")
			//NewPrintf(LEVELALL, "-----------CoordReq Chan-----------")
			r.handleCoordinationRequest(coordinationRequest)
			break
		case coordinationRReply := <-r.coordReqReplyChan:
			dlog.Printf("11\n")
			//NewPrintf(LEVELALL, "----------CoordReqReply Chan--------")
			CRR := coordinationRReply.(*mdlinproto.CoordinationResponse)
			r.handleCoordinationRReply(CRR)
			break
		case finalAcceptReplyS := <-r.finalAcceptReplyChan:
			dlog.Printf("12\n")
			finalAcceptReply := finalAcceptReplyS.(*mdlinproto.FinalAcceptReply)
			// got a FinalAccept reply
			r.handleFinalAcceptReply(finalAcceptReply)
			break

			// case metricsRequest := <-r.MetricsChan:
			// 	// Empty reply because there are no relevant metrics
			// 	reply := &genericsmrproto.MetricsReply{}
			// 	reply.Marshal(metricsRequest.Reply)
			// 	metricsRequest.Reply.Flush()
			// 	break
		//default:
		//	break
		}
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

// indexOL, orderedLog, bufferedLog
func (r *Replica) processEpoch() {
  // TODO garbage collection :)
  // create an entry from the readyBuff
  dlog.Printf("------------Epoch Beginning---------%v\n", time.Now().UnixMilli())
  start := time.Now()
  p := r.readyBuff.Front()
  next := p
  n := r.readyBuff.Len()
  if (n <= 0) {
	  return
  }
  b := make([]state.Command, n) // Command to execute
  bi := make([]int64, n) // Epoch to sort by
  bp := make([]*genericsmr.MDLPropose, n) // Client we are responding to
  cmdids := make([]mdlinproto.Tag, n)
  j := 0
  //NewPrintf(LEVEL0, "There are %v ready entries to add!", n)
  oldLen := len(r.bufferedLog)
  for p != nil {
    next = p.Next()
    // remove ordered entries from the buffer log
    r.readyBuff.Remove(p)
    r.epoch = int64(math.Max(float64(r.epoch), float64(p.Value.(*Instance).epoch[0])))
    b[j] = p.Value.(*Instance).cmds[0]
    bi[j] = p.Value.(*Instance).epoch[0]
    bp[j] = p.Value.(*Instance).lb.clientProposals[0]
    cmdids[j] = mdlinproto.Tag{K: p.Value.(*Instance).cmds[0].K, PID: p.Value.(*Instance).pid, SeqNo: p.Value.(*Instance).seqno}
    delete(r.bufferedLog, cmdids[j])
    j++
    //NewPrintf(LEVEL0, "ProcessEpoch: adding entry with CommandId %v, Seqno %v", p.Value.(*Instance).lb.clientProposals[0].CommandId, p.Value.(*Instance).seqno)
    dlog.Printf("Ordering CommandID %v PID %v\n", p.Value.(*Instance).seqno, p.Value.(*Instance).pid)
    p = next
  }
  // increment the epoch
  // add all ordered entries to the ordered log
  instNo := r.addEntryToOrderedLog(r.crtInstance, b, bi, bp, PREPARED)
  r.crtInstance++
  // do last paxos roundtrip with this whole batch you just added
  //NewPrintf(LEVEL0, "Issueing a final round paxos RTT for epoch %v, with %v commands", r.epoch, n)
  dlog.Printf("BCASTFinal!!Accept for instNo %v at %v\n", instNo, time.Now().UnixMilli())
  r.bcastFinalAccept(instNo, r.defaultBallot, cmdids, bi)

  r.epoch++
  r.totalEpochs++
  if (r.readyBuff.Len() != 0) {
    panic("readyBuff should be empty after end of processing epoch!")
  }
  end := time.Now()
  dlog.Printf("------------Epoch End---------%v, it took %v nano\n", time.Now().UnixMilli(), end.Sub(start).Nanoseconds())
  if (len(r.bufferedLog) != oldLen-n) {
	  panic ("didn't take out the right amount of elements in buffLog")
  }
}

// indexOL, orderedLog, bufferedLog
func (r *Replica) processCCEntry() {
  // TODO garbage collection :)
  // create an entry from the readyBuff
  //dlog.Printf("------------Epoch Beginning---------%v\n", time.Now().UnixMilli())
  p := r.readyBuff.Back()
  r.readyBuff.Remove(p)
  if (r.readyBuff.Len() != 0) {
    panic("Should only be adding one thing at a time")
  }

  b := make([]state.Command, 1) // Command to execute
  bi := make([]int64, 1) // Epoch to sort by
  bp := make([]*genericsmr.MDLPropose, 1) // Client we are responding to
  cmdids := make([]mdlinproto.Tag, 1)
  j := 0
  //NewPrintf(LEVEL0, "There are %v ready entries to add!", n)
  b[j] = p.Value.(*Instance).cmds[0]
  bi[j] = p.Value.(*Instance).epoch[0]
  bp[j] = p.Value.(*Instance).lb.clientProposals[0]
  cmdids[j] = mdlinproto.Tag{K: p.Value.(*Instance).cmds[0].K, PID: p.Value.(*Instance).pid, SeqNo: p.Value.(*Instance).seqno}
  delete(r.bufferedLog, cmdids[j])
  instNo := r.addEntryToOrderedLog(r.crtInstance, b, bi, bp, PREPARED)
  r.crtInstance++
  // do last paxos roundtrip with this whole batch you just added
  //NewPrintf(LEVEL0, "Issueing a final round paxos RTT for epoch %v, with %v commands", r.epoch, n)
  dlog.Printf("|-------------|BCASTFinal!!Accept for instNo %v at %v\n", instNo, time.Now().UnixMilli())
  r.bcastFinalAccept(instNo, r.defaultBallot, cmdids, bi)
}

func (r *Replica) updateCommittedUpTo() {
	//ini := r.committedUpTo
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		(r.instanceSpace[r.committedUpTo+1].status == COMMITTED) {
		r.committedUpTo++
	}
	//dlog.Printf("updated index from %v to %v at %v\n", ini, r.committedUpTo, time.Now().UnixMilli())
	//NewPrintf(LEVEL0, "Updating commit index from %d to %d", ini, r.committedUpTo)
}

func (r *Replica) bcastPrepare(instance []mdlinproto.Tag, ballot int32, toInfinity bool) {
	//NewPrintf(LEVELALL, "Inside broadcast prepare!")
	defer func() {
		if err := recover(); err != nil {
			//NewPrintf(LEVEL0, "Prepare bcast failed: %v", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &mdlinproto.Prepare{r.Id, ballot, ti, instance}

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
			//NewPrintf(LEVEL0, "Accept bcast failed: %v", err)
		}
	}()

  //NewPrintf(LEVELALL, "BcastFinalAccept")
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

  //NewPrintf(LEVELALL, "Broadcasting accept with message %v", fpa)
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

func (r *Replica) bcastAccept(ballot int32, command []state.Command, pids []int64, seqnos []int64, es int64, removethis []int32) {
	defer func() {
		if err := recover(); err != nil {
			//NewPrintf(LEVEL0, "Accept bcast failed: %v", err)
		}
	}()

  // We don't replicate the coordinated bit!!!
  // A new leader has to undergo the expensive
  // procedure of re-coordinating all requests
  // TODO currently not including the predecessor
  // in the Accept message... but we should!!
  //NewPrintf(LEVELALL, "BcastAccept regular")
	pa.LeaderId = r.Id
	pa.Ballot = ballot
	pa.Command = command
	pa.PIDs = pids
	pa.SeqNos = seqnos
	pa.CommandId = removethis
	// Make a copy of the nextSeqNo map
	//expectedSeqs := make(map[int64]int64)
	//copyMap(expectedSeqs, r.nextSeqNo)
  //pa.ExpectedSeqs = expectedMap
  //TODO what other maps..?
  pa.ExpectedSeqs = r.nextSeqNo
  pa.Epoch = es
	args := &pa

  //NewPrintf(LEVELALL, "Broadcasting accept with message %v", pa)
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
			//NewPrintf(LEVEL0, "Commit bcast failed: %v", err)
		}
	}()
  //NewPrintf(LEVELALL, "Leader calling bcastCommit")
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
		//NewPrintf(LEVELALL, "I'm not the leader... responding to client with OK = false (0)")
		r.MDReplyPropose(preply, propose.Reply)
		return
	}
	r.printMap[propose.CommandId] = int(time.Now().UnixMilli())
	dlog.Printf("Calling handlePropose at time %v\n", r.printMap[propose.CommandId])

	// Get batch size
  batchSize := 1
	numProposals := len(r.MDLProposeChan) + 1
	dlog.Printf("length of the MDLProposeChan = %v\n", len(r.MDLProposeChan)+1)
	if r.batchingEnabled {
		batchSize = numProposals //TODO +1?
		if batchSize > MAX_BATCH {
			batchSize = MAX_BATCH
		}
	}
	dlog.Printf("batchsize = %v\n", batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.MDLPropose, batchSize)
  pid := make([]int64, batchSize)
  seqno := make([]int64, batchSize)
  prepareTags := make([]mdlinproto.Tag, batchSize)
  cmdIds := make([]int32, batchSize)

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	found := 0
	var expectedSeqno int64
	prop := propose
	i := 1
  // i, numProposals = i is a for loop from 1 to numProposals
  // found, batchsize = we use found to bound the number of entries added to batchsize=1 (and flushing buffer)
	for found < batchSize && i <= numProposals {
		pid[found] = prop.PID
		seqno[found] = prop.SeqNo
		expectedSeqno = 0

		if val, ok := r.nextSeqNo[prop.PID]; ok {
			expectedSeqno = val
		}
		if prop.SeqNo != expectedSeqno {
			// Add to buffer
      panic("We shouldn't be getting OoO reqs per client")
			if _, ok := r.outstandingInst[prop.PID]; !ok {
				r.outstandingInst[prop.PID] = make([]*genericsmr.MDLPropose, 0)
			}
			r.outstandingInst[prop.PID] = append(r.outstandingInst[prop.PID], prop)
			if len(r.outstandingInst[prop.PID]) > 1 {
				mysort.MergeSort(r.outstandingInst[prop.PID])
			}
      //TODO update other state maps, like CR, CRR, pred, etc.
			//NewPrintf(LEVELALL, "Out of order, (got command %d seqno %d) buffering back into channel", prop.CommandId, seqno)
		} else {
      cmds[found] = prop.Command
			//NewPrintf(LEVEL0, "In order, has command %d, seqno %d", prop.CommandId, seqno)
			proposals[found] = prop
      cmdIds[found] = prop.CommandId
			found++
			r.nextSeqNo[prop.PID]++

      // If no predecessor, then request is vacuously coordinated
      var coord int8 = -1
      var thisCr *genericsmr.MDLCoordReq = nil
      if (prop.Predecessor.SeqNo == -1) {
        coord = 1
      }

      // Check if coordination request from successor arrived
      // before the request arrived, if so add it
      t := mdlinproto.Tag{K: prop.Command.K, PID: prop.PID, SeqNo: prop.SeqNo}
      prepareTags[found-1] = t
      recvCoordReq := false
      if v, ok1 := r.outstandingCR[t]; ok1 {
        //NewPrintf(LEVEL0, "^^^^^^^^^^^^found an awaiting CR for %v", t)
        thisCr = v
        delete(r.outstandingCR, t)
        recvCoordReq = true
      }
      // Check if response from this request's coordination req
      // arrived from predecessor before this req arrived.
      if v, ok2 := r.outstandingCRR[t]; ok2 {
        //NewPrintf(LEVEL0, "!!!!!!!!!!!!!!found an awaiting CRR for %v", t)
        coord = int8(v.OK)
        delete(r.outstandingCRR, t)
      }

      newEntry := r.addEntryToBuffLog(cmds[found-1], proposals[found-1], pid[found-1], seqno[found-1], coord, thisCr, &prop.Predecessor, r.epoch) //This seems like a bad idea TODO... the address of a message that's gonna disapear?
      if !recvCoordReq {
        r.seen[t] = newEntry
      }
			// Check if any others are ready
			for found < batchSize {
				//NewPrintf(LEVELALL, "looking for any others that might be ready from this PID %d", pid)
        pID := prop.PID
				l := len(r.outstandingInst[pID])
				//NewPrintf(LEVELALL, "apppears there are %d outstanding for this pid", l)
				expectedSeqno = r.nextSeqNo[pID]
				if (l > 0) && (r.outstandingInst[pID][l-1].SeqNo == expectedSeqno) {
          panic("Shouldn't be adding any buffered OoO reqs per client")
					// We found previously outstanding requests that can be replicated now
					prop = r.outstandingInst[pID][l-1]
					r.outstandingInst[pID] = r.outstandingInst[pID][:l-1]
					r.nextSeqNo[pID]++ // Giving us linearizability!
					cmds[found] = prop.Command
					proposals[found] = prop
          cmdIds[found] = prop.CommandId
          coord = -1
          thisCr = nil
          if (prop.Predecessor.SeqNo == -1) {
            panic("This should never happen...? :D delete me")
          }

          // Check if coordination request from successor arrived
          // before the request arrived, if so add it
          t = mdlinproto.Tag{K: prop.Command.K, PID: pID, SeqNo: expectedSeqno}
          prepareTags[found] = t
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

					//NewPrintf(LEVELALL, "head of it's buff Q is ready, with command %d", prop.CommandId)
          found++
				} else {
					break
				}
			}
		}
		i++
		if found < batchSize && i <= numProposals {
			//NewPrintf(LEVELALL, "--->Pulled out the next one")
			prop = <-r.MDLProposeChan
		}
	}

	dlog.Printf("found = %v\n", found)
	// None of the proposals in the channel
	// are ready to be added to the log
	if found == 0 {
		//NewPrintf(LEVELALL, "None of the proposals pulled out of the channel or in the buffers are ready!")
		// We won't respond to the client, since that response
		// will come when the command gets unbuffered and later executed
		r.noProposalsReady = true
		return
	}

	r.noProposalsReady = false

  // Resize all the arrays to hold the actual amount we found
  prepareTags = append([]mdlinproto.Tag(nil), prepareTags[:found]...)
  cmds = append([]state.Command(nil), cmds[:found]...)
  pid = append([]int64(nil), pid[:found]...)
  seqno = append([]int64(nil), seqno[:found]...)
  cmdIds = append([]int32(nil), cmdIds[:found]...)
  dlog.Printf("ended up finding %d entries for this batch", found)
	//NewPrintf(LEVEL0, "handlePropose: CurrInst Pushed back entry with CommandId %v, Seqno %v", p.Value.(*Instance).lb.clientProposals[0].CommandId, p.Value.(*Instance).seqno)
	if r.defaultBallot == -1 {
		dlog.Printf("BCASTPrepare at time %v\n", time.Now().UnixMilli())
		//NewPrintf(LEVELALL, "    Step2. (candidate) leader broadcasting prepares....")
		r.bcastPrepare(prepareTags, r.makeUniqueBallot(0), true)
	} else {
		//NewPrintf(LEVELALL, "    Step2. (candidate) leader broadcasting accepts!....")
    for i := 0; i < found; i++ {
		  r.recordInstanceMetadata(r.bufferedLog[prepareTags[i]])
      cmdRecord := make([]state.Command, 1)
      cmdRecord[0] = cmds[i]
      r.recordCommands(cmdRecord)
      r.sync()
    }
		dlog.Printf("BCASTAccept for at time %v\n", time.Now().UnixMilli())
		r.bcastAccept(r.defaultBallot, cmds, pid, seqno, r.epoch, cmdIds)
	}
}

func (r *Replica) addEntryToBuffLog(cmds state.Command, proposals *genericsmr.MDLPropose, pid int64,
  seqno int64, coord int8, thisCr *genericsmr.MDLCoordReq, pred *mdlinproto.Tag, ep int64) *Instance {

	// Add entry to log
  //NewPrintf(LEVEL0, "addEntryToBuffLog --> Shard Leader Creating Log Entry{%s, PID: %d, SeqNo: %d, coord: %d, thisCr: %v, pred: %v, epoch: %v",
		//commandToStr(cmds[0]), pid, seqno, coord, thisCr, pred, r.epoch)
  if (r.Id == 0) {
	  //NewPrintf(LEVEL0, "------>with commandId %v", proposals[0].CommandId)
  }

  ball := r.defaultBallot
  stat := PREPARED
  if r.defaultBallot == -1 {
    ball = r.makeUniqueBallot(0)
    stat = PREPARING
  }
  thisEpoch := make([]int64, 1)
  thisEpoch[0] = ep
  com := make([]state.Command, 1)
  com[0] = cmds
  props := make([]*genericsmr.MDLPropose, 1)
  props[0] = proposals
  e := &Instance{
      com,
      ball,
      stat,
      &LeaderBookkeeping{props, 0, 0, 0, 0, coord},
      pid,
      seqno,
      pred,
      thisCr,
      thisEpoch,
      r.totalEpochs}

  t := mdlinproto.Tag{K: cmds.K, PID: pid, SeqNo: seqno}
  // Insert into map
  r.bufferedLog[t] = e
  return e
}


func (r *Replica) addEntryToOrderedLog(index int32, cmds []state.Command, epochSizes []int64, cPs []*genericsmr.MDLPropose, status InstanceStatus) int32 {
	// Add entry to log
	//NewPrintf(LEVEL0, "Flushing ready entries buffLog --> orderedLog at END OF EPOCH!")

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
    0}
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

func (r *Replica) findEntryInBuffLog(tag mdlinproto.Tag) *Instance {
  if e, ok := r.bufferedLog[tag]; ok {
    return e
  }
  return nil
}
/*
func (r *Replica) findEntryInOrderedLog(tag mdlinproto.Tag) *Instance {
  // Search the ordered log
  	log.Printf("Inside LONg func at time %v looking for commandId %v\n", time.Now().UnixMilli(), tag.SeqNo)
	for i := r.crtInstance - 1; i >= 0; i-- {
		if r.instanceSpace[i] == nil {
			panic("instanceSpace at this index should never be nil")
		}
		props := r.instanceSpace[i].lb.clientProposals
		for j := 0; j < len(props); j++ {
			e := props[j]
			if e.Command.K == tag.K && e.PID == tag.PID && e.SeqNo == tag.SeqNo {
				log.Printf("Which just finished at %v\n", time.Now().UnixMilli())
				return r.instanceSpace[i]
			}
		}
	}
	log.Printf("Which didin't find anything and JUST finished%v\n", time.Now().UnixMilli())
	return nil
}*/

// Client sends a CR to the predecessor request (askee), we need to check
// if it's coordinated and committed before responding to the successor (asker)
//
// If it is not both, then we should indicate that it will be waiting for those
// flags to get set, in either the handleAcceptReply or handleCoordinationRReply
// methods, both of which can then issue the replyCoord messages to the asker shard.
func (r *Replica) handleCoordinationRequest(cr *genericsmr.MDLCoordReq) {
  //NewPrintf(LEVEL0, "##########Received coord req for %v from shard %v", cr.AskeeTag, cr.From)
  var OK bool
  var CC uint8
  e := r.findEntryInBuffLog(cr.AskeeTag)
  if (e == nil) {
    var in bool
    e, in = r.seen[cr.AskeeTag]
    if (!in) {
      //NewPrintf(LEVEL0, "Coordination Request arrived before the predecessor did")
      dlog.Printf("handleCoordinationRequest from client for req not yet here\n")
      r.outstandingCR[cr.AskeeTag] = cr //This seems like a bad idea TODO... the address of a message that's gonna disapear?
      return
    }
    dlog.Printf("handleCoordinationRequest from client for that was already COMMITTED\n")
    OK = true
    CC = 1
  } else {
    OK, CC = r.checkCoordination(e)
  }
  delete(r.seen, cr.AskeeTag)
  e.cr = cr // won't be nil anymore, signals acceptReply() and coordReply()
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

// When we receive a response from the predecessor of the asker, we can coordinate
// the asker request
func (r *Replica) handleCoordinationRReply(crr *mdlinproto.CoordinationResponse) {
  //NewPrintf(LEVEL0, "@@@@@@@@@@@Received coord ReSPONSE for %v with OK = %v", crr.AskerTag, crr.OK)
  e := r.findEntryInBuffLog(crr.AskerTag) // only need to search buffLog, since it's not in orderedLog yet
  if (e == nil) {
    dlog.Printf("Getting coord resp from predecessor to sucessor that hasn't arrived yet! crr = %v\n", crr)
    //NewPrintf(LEVEL0, "Coordination Response arrived before the request did")
    r.outstandingCRR[crr.AskerTag] = crr //This seems like a bad idea TODO... the address of a message that's gonna disapear?
    return
  }
  if e.pred.SeqNo == -1 {
    panic("Received CR Response for request with no predecessor")
  }
  e.lb.coordinated = int8(crr.OK)
  e.epoch[0] = crr.AskeeEpoch // **we do this so that if you're coordinated before being accepted, the accept logic works out - this explains line 1428
  OK, CC := r.checkCoordination(e)
  if (OK && CC==1) {
    // Set my epoch to the max of sender and mine
    pred_epoch := e.epoch[0]
    e.epoch[0] = int64(math.Max(float64(r.epoch), float64(pred_epoch + 1)))
    // So that incoming reqs in real time are ordered after you
    r.epoch = int64(math.Max(float64(r.epoch), float64(e.epoch[0]))) + 1
    // Now Add me to the orderedLog
    //NewPrintf(LEVEL0, "Pushingback seqno %v", e.seqno)
    r.readyBuff.PushBack(e)
    dlog.Printf("Proposal with CommandId %v PID %v got CC (in handleCoordReply) at %v\n", e.lb.clientProposals[0].CommandId, e.pid, time.Now().UnixMilli())
  }
  // Check if this req's successor (asker's asker) already sent 
  // a CR for this req, but before it was coordinated itself
  if (OK && e.cr != nil) {
    shardTo := e.cr.From
    msg := &mdlinproto.CoordinationResponse{e.cr.AskerTag, e.cr.AskeeTag, e.epoch[0], int32(r.ShardId), CC}
    r.replyCoord(shardTo, msg)
  }
  if (!r.epochBatching && OK && CC==1) {
    r.processCCEntry()
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

//func (r *Replica) printLog(level int) {
	//for i := 0; int32(i) < r.crtInstance; i++ {
		//e := r.instanceSpace[i]
		//NewPrintf(level, "Log_entry@index = %d has status %d, and commands...", i, e.status)
		//for _, c := range e.cmds {
			//NewPrintf(level, commandToStr(c))
		//}
	//}
//}

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
  //NewPrintf(LEVELALL, "Replica at handlePrepare with prepare.Instance == %v", prepare.Instance)
	var preply *mdlinproto.PrepareReply

	ok := TRUE
	if r.defaultBallot > prepare.Ballot {
    ok = FALSE
	}
	preply = &mdlinproto.PrepareReply{prepare.Instance, r.defaultBallot, ok, make([]state.Command, 0)}
	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *mdlinproto.Accept) {
	dlog.Printf("Start of handleAccept for with %v requests from PID %v at %v\n", len(accept.Command), accept.PIDs, time.Now().UnixMilli())
  var areply *mdlinproto.AcceptReply
  if (accept.Ballot < r.defaultBallot) {
    //t := mdlinproto.Tag{K: -1, PID: -1, SeqNo: -1}
    areply = &mdlinproto.AcceptReply{FALSE, r.defaultBallot, nil}
  } else {
    // could add predecessor Req to Accept message type so that new elected leader can issue coordReq!
    t := make([]mdlinproto.Tag, len(accept.Command))
    for i := 0; i < len(accept.Command); i++ {
      t[i] = mdlinproto.Tag{K: accept.Command[i].K, PID: accept.PIDs[i], SeqNo: accept.SeqNos[i]}
      r.addEntryToBuffLog(accept.Command[i], nil, accept.PIDs[i], accept.SeqNos[i], -1, nil, nil, accept.Epoch)
    }
    areply = &mdlinproto.AcceptReply{TRUE, r.defaultBallot, t}
  }

  if areply.OK == TRUE {
    //NewPrintf(LEVEL0, "Replica %v added this request to buffLog", r.Id)
    copyMap(r.nextSeqNo, accept.ExpectedSeqs)
  }
	r.replyAccept(accept.LeaderId, areply)
	dlog.Printf("|------>END of handleAccept at time %v\n", time.Now().UnixMilli())
}

func (r *Replica) handleFinalAccept(faccept *mdlinproto.FinalAccept) {
  var fareply *mdlinproto.FinalAcceptReply

  //NewPrintf(LEVELALL, "New FINAL Accept from leader, instance = %v", faccept.Instance)
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
      n := len(faccept.CmdTags)
      b := make([]state.Command, n)
      bi := make([]int64, n)
      result := true
      for i, k := range faccept.CmdTags {
        if v, ok := r.bufferedLog[k]; !ok {
          panic("This replica didn't have all the entries buffered that the leader sent out in FinalAccept")
          result = false
          break
        } else {
          b[i] = v.cmds[0]
          bi[i] = faccept.EpochSize[i]
          delete(r.bufferedLog, k)
        }
      }
      if result {
        r.addEntryToOrderedLog(faccept.Instance, b, bi, nil, ACCEPTED)
        fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, TRUE, faccept.Ballot}
      } else {
        panic("This replica didn't have all the entries buffered that the leader sent out in FinalAccept")
        fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, FALSE, faccept.Ballot}
      }
    }
  }

	if fareply.OK == TRUE {
    //NewPrintf(LEVELALL, "Replica %v accepted this request in OrderedLog", r.Id)
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

	//NewPrintf(LEVEL0, "Replica %d is getting handleCommitShort", r.Id)
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

  //NewPrintf(LEVELALL, "handlePrepareReply, prepare.Instance = %v", preply.Instance)
  inst, ok := r.bufferedLog[preply.Instance[0]]
  if !ok {
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
      b := inst.ballot
      e := inst.epoch[0]
      numacks := inst.lb.prepareOKs
      cmds := make([]state.Command, len(preply.Instance))
      pids := make([]int64, len(preply.Instance))
      seqnos := make([]int64, len(preply.Instance))
      cmdIds := make([]int32, len(preply.Instance))
      for i := 0; i < len(preply.Instance); i++ {
        inst = r.bufferedLog[preply.Instance[i]]
        inst.lb.prepareOKs = numacks
			  inst.status = PREPARED
			  inst.lb.nacks = 0
			  if inst.ballot > r.defaultBallot {
				  r.defaultBallot = inst.ballot
			  }
			  r.recordInstanceMetadata(r.bufferedLog[preply.Instance[i]])
			  r.sync()
        cmds[i] = inst.cmds[0]
        pids[i] = inst.pid
        seqnos[i] = inst.seqno
        cmdIds[i] = inst.lb.clientProposals[0].CommandId
      }
			dlog.Printf("BCASTAccept for CommandId = %d of batchSize %v PID %v at %v\n", inst.seqno, len(preply.Instance), inst.pid, time.Now().UnixMilli())
			r.bcastAccept(b, cmds, pids, seqnos, e, cmdIds)
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
  //NewPrintf(LEVELALL, "got RESPONSE to (regular) accept %v", areply.OK)
  if areply.OK != TRUE {
    panic("Replica didn't accept buffered appendEntries?")
  }

  inst := r.findEntryInBuffLog(areply.IdTag[0])
  if inst == nil {
    // Assuming this isn't something nefarious, and if it's not in the 
    // bufflog, then it must have already been placed in the orderedLog
    //NewPrintf(LEVEL0, "Got handleAcceptReply for entry already in orderedLog")
    return
    // With batching this is also safe, since if this is true, then the entry must
    // have had a quorum, which means the other batched entries had a quroum, which
    // we recorded as soon as we saw it. so at this point there's nothing else for
    // this function to do anyway for the other batched entries even if they ARE
    // still in the buff log
  }
  if inst.lb.acceptOKs+1 > r.N>>1 {
	  return
  }
  inst.lb.acceptOKs++
  if inst.lb.acceptOKs+1 > r.N>>1 {
    dlog.Printf("Quorum reached at time %v for commandID %v which has batchsize = %v\n", time.Now().UnixMilli(), areply.IdTag[0], len(areply.IdTag))
    numacks := inst.lb.acceptOKs
    // This is for batching, we go through all the requests just replicated by a replica
    for i := 0; i < len(areply.IdTag); i++ {
      inst = r.bufferedLog[areply.IdTag[i]]
      inst.lb.acceptOKs = numacks
      //NewPrintf(LEVEL0, "Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
      OK, CC := r.checkCoordination(inst)
      if OK && (CC==1) {
        // So that incoming reqs in real time are ordered after you
        pred_epoch := inst.epoch[0]
        inst.epoch[0] = int64(math.Max(float64(r.epoch), float64(pred_epoch+1)))
        r.epoch = int64(math.Max(float64(r.epoch), float64(inst.epoch[0]))) + 1
        // Now add me to the orderedLog and remove from buffLog
        //NewPrintf(LEVEL0, "Pushingback Seqno %v", inst.seqno)
        r.readyBuff.PushBack(inst)
        dlog.Printf("Proposal with CommandId %v PID %v got CC (in handleAccept) at %v\n", inst.lb.clientProposals[0].CommandId, inst.pid, time.Now().UnixMilli())
      }
      if (OK && inst.cr != nil) {
        shardTo := inst.cr.From
        // Send this req's epoch to the successor
        msg := &mdlinproto.CoordinationResponse{inst.cr.AskerTag, inst.cr.AskeeTag, inst.epoch[0], int32(r.ShardId), CC}
        r.replyCoord(shardTo, msg)
      }
      // I know this is ugly, i just did this so that we send the coord message first!
      if !r.epochBatching && OK && (CC==1) {
        dlog.Printf("Anjd issueing finalAccept round for that request\n")
        r.processCCEntry()
      }
    }
  }
}

func (r *Replica) handleFinalAcceptReply(fareply *mdlinproto.FinalAcceptReply) {
  //NewPrintf(LEVELALL, "got RESPONSE to FINAL accept %v", fareply.OK)
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
      //NewPrintf(LEVELALL, "FINAL ROUND Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
      //dlog.Printf("--->Committing instNo %v at time %v\n", fareply.Instance, time.Now().UnixMilli())
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
		//executed := false

		for i <= r.committedUpTo {
			if r.instanceSpace[i].cmds != nil {
				inst := r.instanceSpace[i]
				//NewPrintf(LEVELALL, "Number of commands in this entry is %d", len(inst.cmds))
        if r.epochBatching {
				  startT := time.Now()
				  mysort.EpochSort(inst.epoch, inst.cmds) // SORT
				  endT := time.Now()
				  dlog.Printf("===>The sorting took %v nanoseconds\n", endT.Sub(startT).Nanoseconds())
        }
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

						//NewPrintf(LEVEL0, "EXECUTING --> CLIENT:OK = TRUE, CommandID = %d, val = %v, key = %d, seqno = %d, PID = %dHA", inst.lb.clientProposals[j].CommandId, val, inst.lb.clientProposals[j].Command.K, inst.lb.clientProposals[j].SeqNo, inst.lb.clientProposals[j].PID)

						//dlog.Printf("Proposal with CommandId = %d PID %v RESPONDed at time %v\n", inst.lb.clientProposals[j].CommandId, inst.pid, time.Now().UnixMilli())
						//x := r.printMap[inst.lb.clientProposals[j].CommandId]
						//delete(r.printMap, inst.lb.clientProposals[j].CommandId)
						//deltaT := int(time.Now().UnixMilli()) - x
						//dlog.Printf("Proposal with CommandId = %d PID %v took %v milliseconds\n", inst.lb.clientProposals[j].CommandId, inst.lb.clientProposals[j].PID, deltaT)
            r.MDReplyPropose(propreply, inst.lb.clientProposals[j].Reply)
					} else {
            //NewPrintf(LEVEL0, "REPLICAS EXECUTING!!")
          }
				}
				i++
				//executed = true
			} else {
				break
			}
		}

		//if !executed {
		//	time.Sleep(1000 * 1000)
		//}
	}

}
