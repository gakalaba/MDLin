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
	"runtime"
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

const DEBUG_LEVEL = -1
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
  printMap		map[int64]int
  statsmap	[]int
  totalEpochs	      int
  fanout		int
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
  epoch      []int64
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
	exec bool, dreply bool, durable bool, batch bool, epBatch bool, statsFile string, numShards int, epochLength int, fanout int) *Replica {
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
    make(chan fastrpc.Serializable, 3*genericsmr.CHAN_BUFFER_SIZE),
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
    make(map[int64]int),
    make([]int, 2),
    0,
    fanout}
        dlog.Printf("FANOUT = %v value passed in = %v\n", r.fanout, fanout)
	r.statsmap[0] = 0
	r.statsmap[1] = 1
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
  dlog.Printf("finalAccept RPC = %v, Reply = %v\n", r.finalAcceptRPC, r.finalAcceptReplyRPC)
  r.coordResponseRPC = r.RegisterRPC(new(mdlinproto.CoordinationResponse), r.coordReqReplyChan)

	go r.run(masterAddr, masterPort)

	dlog.Printf("GO PMAPRICOS %v\n", runtime.GOMAXPROCS(0))
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
		dlog.Printf("Sending response to same shard!")
		r.coordReqReplyChan <- reply
	} else {
		dlog.Printf("Sending response to DIFFERENT shard: %v", reply)
		r.SendISMsg(replicaId, r.coordResponseRPC, reply)
	}
}

/* ============= */

func (r *Replica) batchClock(proposeDone *(chan bool)) {
  for !r.Shutdown {
    time.Sleep(time.Duration(r.epochlen) * time.Microsecond)
    (*proposeDone) <- true
    //dlog.Printf("!!!Pulled of proposeDone\n")
  }
}
func (r *Replica) epochClock(proposeChan *(chan bool), proposeDone *(chan bool)) {
  for !r.Shutdown {
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
			dlog.Printf("1\n")
			r.handlePropose(proposal)
			if r.batchingEnabled {
				proposeChan = nil
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
			dlog.Printf("7 handlePrepareReply\n")
			prepareReply := prepareReplyS.(*mdlinproto.PrepareReply)
			//got a Prepare reply
			r.handlePrepareReply(prepareReply)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			dlog.Printf("8 handleAcceptRepl\n")
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
			dlog.Printf("10 handleCoordinationRequest\n")
			//NewPrintf(LEVELALL, "-----------CoordReq Chan-----------")
			r.handleCoordinationRequest(coordinationRequest)
			break
		case coordinationRReply := <-r.coordReqReplyChan:
			dlog.Printf("11 CoordinationRESponse\n")
			//NewPrintf(LEVELALL, "----------CoordReqReply Chan--------")
			CRR := coordinationRReply.(*mdlinproto.CoordinationResponse)
			r.handleCoordinationRReply(CRR)
			break
		case finalAcceptReplyS := <-r.finalAcceptReplyChan:
			dlog.Printf("12 finalAcceptReply\n")
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
  ps := make([]int64, n)
  sn := make([]int64, n)
  j := 0
  naught_count := 0
  //NewPrintf(LEVEL0, "There are %v ready entries to add!", n)
  oldLen := len(r.bufferedLog)
  dlog.Printf("initial bufferedLog length = %v", oldLen)
  for p != nil {
    next = p.Next()
    // remove ordered entries from the buffer log
    r.readyBuff.Remove(p)
    r.epoch = int64(math.Max(float64(r.epoch), float64(p.Value.(*Instance).epoch[0])))
    b[j] = p.Value.(*Instance).cmds[0]
    bi[j] = p.Value.(*Instance).epoch[0]
    bp[j] = p.Value.(*Instance).lb.clientProposals[0]

    if (p.Value.(*Instance).lb.clientProposals[0].Predecessor.PID != -1) {
	    dlog.Printf("middle j = %v", j)
	    cmdids[j] = mdlinproto.Tag{K: p.Value.(*Instance).cmds[0].K, PID: p.Value.(*Instance).lb.clientProposals[0].PID, SeqNo: p.Value.(*Instance).lb.clientProposals[0].SeqNo}
	    dlog.Printf("len before delete === %v", len(r.bufferedLog))
	    delete(r.bufferedLog, cmdids[j])
	    dlog.Printf("len after delete === %v", len(r.bufferedLog))
	    ps[j] = -1
	    sn[j] = -1
    } else {
	    naught_count++
	    dlog.Printf("naught j = %v", j)
	    ps[j] = p.Value.(*Instance).lb.clientProposals[0].PID
	    sn[j] = p.Value.(*Instance).lb.clientProposals[0].SeqNo
    }
    j++
    dlog.Printf("Ordering CommandID %v PID %v\n", p.Value.(*Instance).lb.clientProposals[0].SeqNo, p.Value.(*Instance).lb.clientProposals[0].PID)
    p = next
  }
  dlog.Printf("after the loop, j = %v, len(bufferedlog) = %v", j, len(r.bufferedLog))
  // increment the epoch
  // add all ordered entries to the ordered log

  instNo := r.addEntryToOrderedLog(r.crtInstance, b, bi, bp, PREPARED)
  r.crtInstance++
  // do last paxos roundtrip with this whole batch you just added
  //NewPrintf(LEVEL0, "Issueing a final round paxos RTT for epoch %v, with %v commands", r.epoch, n)
  r.bcastFinalAccept(instNo, r.defaultBallot, -1, cmdids, b, bi)

  r.epoch++
  r.totalEpochs++
  if (r.readyBuff.Len() != 0) {
    panic("readyBuff should be empty after end of processing epoch!")
  }
  if (len(r.bufferedLog) != oldLen-n+naught_count) {
	  dlog.Printf("didn't take out the right amount of elements in buffLog, len(r.bufferedLog) = %v, oldLen-n+naught_count = %v, oldLen %v, n %v, naught_count %v", len(r.bufferedLog), oldLen-n+naught_count, oldLen, n, naught_count)
	  panic ("didn't take out the right amount of elements in buffLog, len(r.bufferedLog) = %v, oldLen-n+naught_count = %v")
  }
}

// indexOL, orderedLog, bufferedLog
func (r *Replica) processCCEntry(cmds []state.Command, tags []mdlinproto.Tag) {
  // Remove from bufflo
  ep := r.bufferedLog[tags[0]].epoch[0]
  proposals := r.bufferedLog[tags[0]].lb.clientProposals
  delete(r.bufferedLog, tags[0])
  // Get the lamport clocks ordered
  ep = int64(math.Max(float64(r.epoch), float64(ep + 1)))
  r.epoch = int64(math.Max(float64(r.epoch), float64(ep))) + 1
  eps := make([]int64, 1)
  eps[0] = ep
  // Add to ordered log
  instNo := r.addEntryToOrderedLog(r.crtInstance, cmds, eps, proposals, ACCEPTED)
  r.crtInstance++
  // Add to seen map
  if (proposals[0].Timestamp == 1) {
    dlog.Printf("Adding t = %v to seen!\n", tags[0])
    r.seen[tags[0]] = r.instanceSpace[instNo]
  }
  // do last paxos roundtrip with this whole batch you just added
  //NewPrintf(LEVEL0, "Issueing a final round paxos RTT for epoch %v, with %v commands", r.epoch, n)
  dlog.Printf("calling bcastFinalAccept, seen SIZE = %v, bufferedLog = %v\n", len(r.seen), r.bufferedLog)
  r.bcastFinalAccept(instNo, r.defaultBallot, proposals[0].CommandId, tags, cmds, eps)
}

func (r *Replica) updateCommittedUpTo() {
	//ini := r.committedUpTo
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		(r.instanceSpace[r.committedUpTo+1].status == COMMITTED) {
		r.committedUpTo++
	}
	//NewPrintf(LEVEL0, "Updating commit index from %d to %d", ini, r.committedUpTo)
}

func (r *Replica) bcastPrepare(instance []mdlinproto.Tag, ballot int32, toInfinity bool) {
	//NewPrintf(LEVELALL, "Inside broadcast prepare!")
	defer func() {
		if err := recover(); err != nil {
			//NewPrintf(LEVEL0, "Prepare bcast failed: %v", err)
		}
	}()
        if (len(instance) == 0) {
	  dlog.Printf("empty bcastPrepare, not sending any RPCs")
          return
        }
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

func (r *Replica) bcastFinalAccept(instance int32, ballot int32, cmdID int32, cmdids []mdlinproto.Tag, command []state.Command, es []int64) {
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
	fpa.Command = command
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

func (r *Replica) bcastAccept(ballot int32, command []state.Command, cmdTags []mdlinproto.Tag , es []int64) {
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
        if (len(command) == 0) {
          return
        }
	pa.LeaderId = r.Id
	pa.Ballot = ballot
	pa.Command = command
	pa.CmdTags = cmdTags
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
	dlog.Printf("got handlePropose for CommandID %v at time %v\n", propose.CommandId, time.Now().UnixNano())
	if !r.IsLeader {
		preply := &mdlinproto.ProposeReply{FALSE, propose.CommandId, state.NIL, 0}
		//NewPrintf(LEVELALL, "I'm not the leader... responding to client with OK = false (0)")
		r.MDReplyPropose(preply, propose.Reply)
		return
	}

	// Get batch size
	batchSize := 1
	numProposals := len(r.MDLProposeChan) + 1
	dlog.Printf("length of the MDLProposeChan = %v\n", len(r.MDLProposeChan)+1)
	dlog.Printf("Batched %d, LENGTH of propose channel = %v\n", batchSize, len(r.ProposeChan))
	if r.batchingEnabled {
		batchSize = numProposals //TODO +1?
		if batchSize > MAX_BATCH {
			batchSize = MAX_BATCH
		}
	}
	dlog.Printf("batchsize = %v\n", batchSize)

	cmds := make([]state.Command, batchSize)
	prepareTags := make([]mdlinproto.Tag, batchSize)
	eps := make([]int64, batchSize)

	cmdsFA := make([]state.Command, batchSize)
	prepareTagsFA := make([]mdlinproto.Tag, batchSize)
	epsFA := make([]int64, batchSize)

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	found := 0
	foundFA := 0
	var expectedSeqno int64
	prop := propose
	i := 1
	ep := r.epoch
        // We need a loop like this to lag behind pulling values off the channel,
        // since in contrast to paxos, we might pull things off the channel that
        // aren't usable
	// i, numProposals = i is a for loop from 1 to numProposals
	// found, batchsize = we use found to bound the number of entries added to batchsize=1 (and flushing buffer)
	for found < batchSize && i <= numProposals {
		expectedSeqno = 0
		if val, ok := r.nextSeqNo[prop.PID]; ok {
			expectedSeqno = val
		}
		dlog.Printf("map = %v, prop.PID = %v, prop.SeqNo = %v, expectedSeqno = %v, predecessor = %v\n", r.nextSeqNo, prop.PID, prop.SeqNo, expectedSeqno, prop.Predecessor)
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
		} else {
			var coord int8 = -1
			t := mdlinproto.Tag{K: prop.Command.K, PID: prop.PID, SeqNo: prop.SeqNo}
                        // Coordination responses that arrived before we did
			if v, ok2 := r.outstandingCRR[t]; ok2 {
				coord = int8(v.OK)
				ep = v.AskeeEpoch
				delete(r.outstandingCRR, t)
			}
			dlog.Printf("t = %v, coord = %v, naught = %v\n", t, coord, prop.Predecessor.SeqNo == -1)
			if (prop.Predecessor.SeqNo == -1) {
                          // I should also be able to delete anything in the seen map that has the same PID and smaller SeqNo
			  coord = int8(1)
                          cmdsFA[foundFA] = prop.Command
                          prepareTagsFA[foundFA] = t
			  epsFA[foundFA] = ep
                          r.addEntryToBuffLog(prop.Command, prop, coord, ep, prop.PID, prop.SeqNo)
			  foundFA++
                        } else {
			  cmds[found-foundFA] = prop.Command
			  prepareTags[found-foundFA] = t
			  eps[found-foundFA] = ep
			  r.addEntryToBuffLog(prop.Command, prop, coord, ep, prop.PID, prop.SeqNo)
			}
			r.nextSeqNo[prop.PID]++
			found++
			ep = r.epoch

			// Check if any others are ready
			/*for found < batchSize {
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
					if (prop.Predecessor.SeqNo == -1) {
						panic("This should never happen...? :D delete me")
					}
					// Check if coordination request from successor arrived
					// before the request arrived, if so add it
					t = mdlinproto.Tag{K: prop.Command.K, PID: pID, SeqNo: expectedSeqno}
					prepareTags[found] = t
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
			}*/
		}
		i++
		if found < batchSize && i <= numProposals {
			//NewPrintf(LEVELALL, "--->Pulled out the next one")
			prop = <-r.MDLProposeChan
		}
	}

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
	//prepareTagsFA = append([]mdlinproto.Tag(nil), prepareTagsFA[:foundFA]...)
	//cmdsFA = append([]state.Command(nil), cmdsFA[:foundFA]...)

	//prepareTags = append([]mdlinproto.Tag(nil), prepareTags[:found-foundFA]...)
	cmds = append([]state.Command(nil), cmds[:found-foundFA]...)
	//dlog.Printf("ended up finding %d entries for this batch, %d of which WERE naught ones", found)
	if r.defaultBallot == -1 {
		dlog.Printf("bcasting nonnaughts, prepareTags = %v, found = %v, foundFA = %v", prepareTags, found, foundFA)
		if ((found-foundFA) > 0) {
			r.bcastPrepare(prepareTags, r.makeUniqueBallot(0), true)
		}
		dlog.Printf("bcasting naughts, prepareTagsFA = %v", prepareTagsFA)
		if (foundFA > 0) {
			r.bcastPrepare(prepareTagsFA, r.makeUniqueBallot(0), true)
		}
	} else {
		//NewPrintf(DEBUG_LEVEL, "    Step2. (candidate) leader broadcasting accepts!....")
		var e *Instance = nil
		if (len(prepareTagsFA) > 0) {
                  e = r.bufferedLog[prepareTagsFA[0]]
		  r.recordInstanceMetadata(e)
                  cmdRecord := make([]state.Command, 1)
                  cmdRecord[0] = e.cmds[0]
                  r.recordCommands(cmdRecord)
		  r.sync()
                }
		dlog.Printf("found = %v, foundFA = %v", found, foundFA)
		if ((found-foundFA) > 0) {
			dlog.Printf("nonNaughts prepareTags = %v", prepareTags)
			r.bcastAccept(r.defaultBallot, cmds, prepareTags, eps)
		}
		if (foundFA > 0) {
			r.processCCEntry(cmdsFA, prepareTagsFA)
		}
	}
	dlog.Printf("finished handlePropose %v\n", time.Now().UnixNano())
}

func (r *Replica) addEntryToBuffLog(cmds state.Command, proposals *genericsmr.MDLPropose, coord int8, ep int64, pid int64, seqno int64) *Instance {

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
  com := make([]state.Command, 1)
  com[0] = cmds
  props := make([]*genericsmr.MDLPropose, 1)
  props[0] = proposals
  eps := make([]int64, 1)
  eps[0] = ep
  e := &Instance{
      com,
      ball,
      stat,
      &LeaderBookkeeping{props, 0, 0, 0, 0, coord},
      eps}

  t := mdlinproto.Tag{K: cmds.K, PID: pid, SeqNo: seqno}
  // Insert into map
  r.bufferedLog[t] = e
  return e
}


func (r *Replica) addEntryToOrderedLog(index int32, cmds []state.Command, epochSizes []int64, cPs []*genericsmr.MDLPropose, status InstanceStatus) int32 {
	// Add entry to log
	r.instanceSpace[index] = &Instance{
		cmds,
		r.defaultBallot,
		status,
    &LeaderBookkeeping{cPs, 0, 0, 0, 0, int8(TRUE)}, // Need this to track acceptOKs
    epochSizes}
  return index
}

// Helper to copy contents of map2 to map1
func copyMap(map1 map[int64]int64, map2 []mdlinproto.Tag) {
	for _, t := range map2 {
		map1[t.PID] = t.SeqNo+1
	}
}

func (r *Replica) resolveShardFromKey(k state.Key) int32 {
	// Return the shardId of the shard that is responsible for this key
	return int32(state.KeyModulo(k, len(r.Shards)))
}

// Client sends a CR to the predecessor request (askee), we need to check
// if it's coordinated and committed before responding to the successor (asker)
//
// If it is not both, then we should indicate that it will be waiting for those
// flags to get set, in either the handleAcceptReply or handleCoordinationRReply
// methods, both of which can then issue the replyCoord messages to the asker shard.

// either we have cr AND we have seen
// OR we're in neither
// cr = nil && seen = V --> ok
// cr = V && seen = nil

func (r *Replica) handleCoordinationRequest(cr *genericsmr.MDLCoordReq) {
  e := r.findEntry(cr.AskeeTag)
  OK, coord, ts := r.checkCoordination(e)
  r.outstandingCR[cr.AskeeTag] = cr
  dlog.Printf("Successor %v looking for predecessor here %v", cr.AskerTag, cr.AskeeTag)
  dlog.Printf("the predecessor OK, coord, ts = %v, %v, %v\n", OK, coord, ts)
  if (e != nil && OK) {
    // Naught requests cannot respond to successors until they are committed
    if (e.lb.clientProposals[0].Predecessor.PID == -1 && e.status != COMMITTED) {
      dlog.Printf("naught request not responding yet...")
      return
    }
    r.replyToSuccessorIfExists(cr.AskeeTag, ts, coord)
  }
}

// There are 3 possible states for a request - 
// 1. it has never arrived
// 2. it is in the buffered log
// 3. it is in the ordered log (either already committed or not yet committed)
func (r *Replica) findEntry(t mdlinproto.Tag) *Instance {
  var e *Instance
  var in bool
  // instance is in r.bufferedLog
  e, in = r.bufferedLog[t]
  if (in) {
    dlog.Printf("entry in bufflog")
    return e
  }
  // instance is in r.seen
  e, in = r.seen[t]
  if (in) {
    dlog.Printf("entry in seen")
    return e
  }
  // instance hasn't arrived yet
  dlog.Printf("entry hasn't arrived yet")
  return nil
}

// We check the coordination and commit status of an instance
// Return (committed ^ coordinated, coordination value, timestamp)
func (r *Replica) checkCoordination(e *Instance) (bool, int8, int64) {
  if (e == nil) {
    return false, 0, 0
  }
  coord := e.lb.coordinated
  committed := (e.lb.acceptOKs+1) > (r.N>>1)
  ts := e.epoch[0]
  // if the instance has not yet been coordinated, it cannot be committed^coordinate
  if coord == -1 {
    return false, 0, 0
  } else {
    // check whether it is also committed
    return committed, coord, ts
  }
}

// When we receive a response from the predecessor of the asker, we can coordinate
// the asker request
func (r *Replica) handleCoordinationRReply(crr *mdlinproto.CoordinationResponse) {
  dlog.Printf("Predecessor %v responding to successor here %v", crr.AskeeTag, crr.AskerTag)
  var e *Instance
  var in bool
  // assert that the instance is not in the orderedLog
  e, in = r.seen[crr.AskerTag]
  if (in) {
    panic("Assert instance should not be in orderedLog for handleCoordinationReply")
  }
  e, in = r.bufferedLog[crr.AskerTag]
  // if the request hasn't arrived yet, then add it to the map
  if (!in) {
    r.outstandingCRR[crr.AskerTag] = crr
  } else {
    // Update my status
    e.lb.coordinated = int8(crr.OK)
    e.epoch[0] = crr.AskeeEpoch
    dlog.Printf("status getting updated to coord = %v, epoch = %v\n", crr.OK, crr.AskeeEpoch)
    // if NOW i'm committed and coordinated, then I must add myself 
    // to the ordered log AND reply to my successors (if any exist)
    OK, coord, ts := r.checkCoordination(e)
    if OK {
      if (!r.epochBatching && coord==1) {
	t := make([]mdlinproto.Tag, 1)
	t[0] = crr.AskerTag
        r.processCCEntry(e.cmds, t)
      }
      r.replyToSuccessorIfExists(crr.AskerTag, ts, coord)
    }
    // Otherwise, this will be handled by handleAcceptReply()
  }
}

// t: tag of the request r on this shard, shard_r. We want to reply to r's successor s on shard_s
func (r *Replica) replyToSuccessorIfExists(t mdlinproto.Tag, ts int64, coord int8) {
  succ, _ := r.outstandingCR[t]
  dlog.Printf("inside replyToSuccessor... outstandingCR = %v, t = %v, ts = %v, coord = %v\n", r.outstandingCR, t, ts, coord)
  if (succ == nil) {
    dlog.Printf("the sucessor was nil??")
    return
  }
  dlog.Printf("responding now\n")
  shardTo := succ.From
  msg := &mdlinproto.CoordinationResponse{succ.AskerTag, succ.AskeeTag, ts, int32(r.ShardId), uint8(coord)}
  r.replyCoord(shardTo, msg)
  delete(r.outstandingCR, t)
  if (coord == 1) {
    delete(r.seen, t)
  } else {
    _, in := r.seen[t]
    if in {
      panic("request shouldn't be in r.seen if value wasn't coordinated and thus never added to orderedLog")
    }
  }
  dlog.Printf("oustandingCR = %v, SIZE(r.seen) = %v", r.outstandingCR, len(r.seen))
}

func (r *Replica) readyToCommit(instance int32) {
	inst := r.instanceSpace[instance]
	inst.status = COMMITTED

	r.recordInstanceMetadata(inst)
	r.sync() //is this necessary?

	r.updateCommittedUpTo()

	r.bcastCommit(instance, inst.ballot, inst.cmds, inst.lb.clientProposals[0].PID, inst.lb.clientProposals[0].SeqNo, COMMITTED, inst.epoch)
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
  //NewPrintf(LEVELALL, "Replica at handlePrepare with prepare.Instance == %v", prepare.Instance)
	var preply *mdlinproto.PrepareReply

	dlog.Printf("got Instance = %v", prepare.Instance)
	ok := TRUE
	if r.defaultBallot > prepare.Ballot {
    ok = FALSE
	}
	preply = &mdlinproto.PrepareReply{prepare.Instance, r.defaultBallot, ok}
	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *mdlinproto.Accept) {
  var areply *mdlinproto.AcceptReply
  if (accept.Ballot < r.defaultBallot) {
    //t := mdlinproto.Tag{K: -1, PID: -1, SeqNo: -1}
    areply = &mdlinproto.AcceptReply{FALSE, r.defaultBallot, nil}
  } else {
	  dlog.Printf("the accept RPC looks like this %v", accept)
    // could add predecessor Req to Accept message type so that new elected leader can issue coordReq!
    for i := 0; i < len(accept.Command); i++ {
      r.addEntryToBuffLog(accept.Command[i], nil, -1, accept.Epoch[i], accept.CmdTags[i].PID, accept.CmdTags[i].SeqNo)
    }
    areply = &mdlinproto.AcceptReply{TRUE, r.defaultBallot, accept.CmdTags}
  }

  if areply.OK == TRUE {
    //NewPrintf(LEVEL0, "Replica %v added this request to buffLog", r.Id)
    copyMap(r.nextSeqNo, accept.CmdTags)
  }
	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleFinalAccept(faccept *mdlinproto.FinalAccept) {
  var fareply *mdlinproto.FinalAcceptReply

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
      result := true
      for i, k := range faccept.CmdTags {
        if _, ok := r.bufferedLog[k]; !ok {
	  if (faccept.CmdTags[i].PID == -1 || faccept.CmdTags[i].SeqNo == -1) {
		  panic("This replica didn't have all the entries buffered that the leader sent out in FinalAccept")
		  result = false
		  break
	  }
        } else {
          delete(r.bufferedLog, k)
        }
      }
      if result {
        r.addEntryToOrderedLog(faccept.Instance, faccept.Command, faccept.EpochSize, nil, ACCEPTED) //TODO For now we're not replicating predecessors or pids/seqnos.. this wouldn't work in event of failover
        fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, TRUE, faccept.Ballot}
      } else {
        panic("This replica didn't have all the entries buffered that the leader sent out in FinalAccept")
        fareply = &mdlinproto.FinalAcceptReply{faccept.Instance, FALSE, faccept.Ballot}
      }
    }
  }

	if fareply.OK == TRUE {
    //NewPrintf(DEBUG_LEVEL, "Replica %v accepted this request in OrderedLog", r.Id)
		r.recordInstanceMetadata(r.instanceSpace[faccept.Instance])
		r.recordCommands(r.instanceSpace[faccept.Instance].cmds)
		r.sync()
		// If we are to accep the Proposal from the leader, we also need to bump up our nextSeqNo
		copyMap(r.nextSeqNo, faccept.CmdTags)
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
	// Because we've grouped together naught requests and others, we gotta do this
	dlog.Printf("inside handlePrepareReply, got prepply.Instance = %v, bufferedLog = %v", preply.Instance, r.bufferedLog)
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
			e := inst.epoch
                        naught := (inst.lb.clientProposals[0].Predecessor.PID == -1)
			dlog.Printf("naught = %v, inst = %v", naught, inst)
			numacks := inst.lb.prepareOKs

			inst.lb.prepareOKs = numacks
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				r.defaultBallot = inst.ballot
			}
			r.recordInstanceMetadata(inst)
			r.sync()
			if (!naught) {
				dlog.Printf("here.... of... the size of shit in bcastAccept is %v", len(inst.cmds))
				dlog.Printf("preply.Instance = %v", preply.Instance)
				r.bcastAccept(b, inst.cmds, preply.Instance, e)
			} else {
				dlog.Printf("here??, inst = %v", inst)
				r.processCCEntry(inst.cmds, preply.Instance)
			}
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
  inst, in := r.bufferedLog[areply.IdTag[0]]
  if !in {
    // Assuming this isn't something nefarious, and if it's not in the 
    // bufflog, then it must have already been placed in the orderedLog
    //NewPrintf(LEVEL0, "Got handleAcceptReply for entry already in orderedLog")
    //TODO check in seen? is it already committed?
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
    numacks := inst.lb.acceptOKs
    // This is for batching, we go through all the requests just replicated by a replica
    for i := 0; i < len(areply.IdTag); i++ {
      inst = r.bufferedLog[areply.IdTag[i]]
      inst.lb.acceptOKs = numacks
      //NewPrintf(LEVEL0, "Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
      OK, coord, ts := r.checkCoordination(inst)
      if OK {
        if (!r.epochBatching && coord==1) {
          r.processCCEntry(inst.cmds, areply.IdTag)
        }
        r.replyToSuccessorIfExists(areply.IdTag[i], ts, coord)
      }
    }
    dlog.Printf("This Command %v got accepted at time %v\n", inst.lb.clientProposals[0].CommandId, time.Now().UnixNano())
    // Otherwise, this will be handled by handleCoordinationRReply()

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
      r.readyToCommit(fareply.Instance)
      t := mdlinproto.Tag{K: inst.cmds[0].K, PID: inst.lb.clientProposals[0].PID, SeqNo: inst.lb.clientProposals[0].SeqNo}
      _, in := r.seen[t]
      _, crin := r.outstandingCR[t]
      // ASSERT
      if (!in && crin && ((int(inst.lb.clientProposals[0].SeqNo) + 1) % r.fanout != 0)) {
        panic("Request should be in r.seen from r.handleFinalAcceptReply, was supposed to be added from processCCEntry")
      }
      dlog.Printf("handleFinalAccept on %v calling reply to successor", t)
      r.replyToSuccessorIfExists(t, inst.epoch[0], inst.lb.coordinated)
      dlog.Printf("This Command %v got FINAL accepted at time %v\n", inst.lb.clientProposals[0].CommandId, time.Now().UnixNano())
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
        //if r.epochBatching {
	//			  mysort.EpochSort(inst.epoch, inst.cmds) // SORT
        //}
				for j := 0; j < len(inst.cmds); j++ {
					// If an instands has multiple commands (a batch)
					// they will get executed in sorted order.
					// This maintains MDL
					val := inst.cmds[j].Execute(r.State)
					if inst.lb != nil && inst.lb.clientProposals != nil {
						propreply := &mdlinproto.ProposeReply{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							17}

						dlog.Printf("EXECUTING --> CLIENT:OK = TRUE, CommandID = %d, val = %v, key = %d, seqno = %d, PID = %dHA", inst.lb.clientProposals[j].CommandId, val, inst.lb.clientProposals[j].Command.K, inst.lb.clientProposals[j].SeqNo, inst.lb.clientProposals[j].PID)

            r.MDReplyPropose(propreply, inst.lb.clientProposals[j].Reply)
					} else {
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
