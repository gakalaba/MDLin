package paxos

import (
	"dlog"
	//"sync"
	"encoding/binary"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"sort"
	"paxosproto"
	"state"
	//"runtime"
	//"os"
	//"runtime/pprof"
	"time"
  "net/rpc"
  "fmt"
  "masterproto"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 120000
const EPOCH_LENGTH = 500

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
	crtInstance         int32       // highest active instance number that this replica knows about
	defaultBallot       int32       // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
	Shutdown            bool
	counter             int
	flush               bool
	committedUpTo       int32
	before		    time.Time
	after		    time.Time
	batchingEnabled     bool
	batchSize	    int
	handleProposeCommands []state.Command
	handleProposeProposals []*genericsmr.Propose
	ringIndexStart int64
	ringIndexEnd int64
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type Instance struct {
	cmds   []state.Command
	ballot int32
	status InstanceStatus
	batchSize int32
	startIndex int64
	lb     *LeaderBookkeeping
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
}

func NewReplica(id int, peerAddrList []string, masterAddr string, masterPort int, thrifty bool,
    exec bool, dreply bool, beacon bool, durable bool, statsFile string, batch bool, batchSize int) *Replica {
	// Passing in 3rd argument (numShards) as 0 to genericsmr.NewReplica()
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, 0, thrifty, exec, dreply, false, statsFile),
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
		time.Now(),
		time.Now(),
		batch,
		batchSize,
		make([]state.Command, MAX_BATCH),
		make([]*genericsmr.Propose, MAX_BATCH),
		0,
		0,}


	log.Printf("BatchingEnabled = %v\n", r.batchingEnabled)
	r.Beacon = beacon
	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(paxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(paxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(paxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(paxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(paxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(paxosproto.AcceptReply), r.acceptReplyChan)
	go r.run(masterAddr, masterPort)

	//dlog.Printf("GO PMAPRICOS %v\n", runtime.GOMAXPROCS(0))
	return r
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command, batchSize int32, startIndex int64) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := int32(0); i < batchSize; i++ {
		cmds[(startIndex + int64(i)) % MAX_BATCH].Marshal(io.Writer(r.StableStore))
	}
}

//sync with the stable store
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

func (r *Replica) replyPrepare(replicaId int32, reply *paxosproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *paxosproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

/* ============= */
/* Main event processing loop */

func (r *Replica) batchClock(proposeDone *(chan bool)) {
  for !r.Shutdown {
    time.Sleep(time.Duration(r.batchSize) * time.Microsecond)
    (*proposeDone) <- true
  }
}

func InsertSorted(s []int, ss []int, b int, e int) ([]int, []int) {
	i := sort.SearchInts(s, e)
	    s = append(s, 0)
	    ss = append(ss, 0)
	        copy(s[i+1:], s[i:])
	        copy(ss[i+1:], ss[i:])
		    s[i] = e
		    ss[i] = b
		        return s, ss
}

func (r *Replica) run(masterAddr string, masterPort int) {

	r.ConnectToPeers()
	if r.Id == 0 {
		r.IsLeader = true
	}
	r.setupShards(masterAddr, masterPort)
	log.Println("Waiting for client connections")

	go r.WaitForClientConnections()
	time.Sleep(6000 * 1000 * 1000)
	//if r.Exec && r.IsLeader {
		go r.executeCommands()
	//}

	//slowClockChan := make(chan bool, 1)
	//go r.SlowClock(slowClockChan)

	//if r.Beacon {
	//	go r.StopAdapting()
	//}
	proposeChan := r.ProposeChan
	proposeDone := make(chan bool, 1)
	if r.batchingEnabled {
		log.Printf("batching neabledddddddddddddddddddddddddddddn\n")
		proposeChan = nil
		go r.batchClock(&proposeDone)
	}

	t_avg := int64(0)
	t_v := int64(0)
	t_median := make([]int, 0)
	t_copy := make([]int, 0)
	bs_median := make([]int, 0)
	bs_copy := make([]int, 0)
	bs_avg := 0
	bs_v := 0
	for !r.Shutdown {
		select {
		case <-proposeDone:
			proposeChan = r.ProposeChan
			break
		case proposal := <-proposeChan:
			//got a Propose from a client
				start := time.Now()
				bs_v = r.handlePropose(proposal)
				end := time.Now()
				t_v = end.Sub(start).Microseconds()
				t_avg++
				bs_avg++
				bs_median, t_copy = InsertSorted(bs_median, t_copy, int(t_v), bs_v)
				t_median, bs_copy = InsertSorted(t_median, bs_copy, bs_v, int(t_v))

				log.Printf("bs = %v, t = %v", bs_v, t_v)
				log.Printf("handlePropose p50 length = %v microseconds, BS = %v", t_median[int(len(t_median)/2)], bs_copy[int(len(t_median)/2)])
				log.Printf("handlePropose p90 length = %v microseconds, BS = %v", t_median[int(float64(len(t_median))*0.9)], bs_copy[int(float64(len(t_median))*0.9)])
				log.Printf("handlePropose p99 length = %v microseconds, BS = %v", t_median[int(float64(len(t_median))*0.99)], bs_copy[int(float64(len(t_median))*0.99)])
				//log.Printf("handlePropose avg length = %v microseconds", t_v/t_avg)
				//log.Printf("handlePropose avg batchsize = %v", bs_v/bs_avg)
				log.Printf("handlePropose p50 batchsize = %v, t = %v", bs_median[int(len(bs_median)/2)], t_copy[int(len(bs_median)/2)])
				log.Printf("handlePropose p90 batchsize = %v, t = %v", bs_median[int(float64(len(bs_median))*0.9)], t_copy[int(float64(len(bs_median))*0.9)])
				log.Printf("handlePropose p99 batchsize = %v, t = %v", bs_median[int(float64(len(bs_median))*0.99)], t_copy[int(float64(len(bs_median))*0.99)])
				if (t_v > 15000) {
					log.Printf("++++++++++++++++++++++")
				}
			if r.batchingEnabled {
				proposeChan = nil
			}
			break
		case prepareS := <-r.prepareChan:
			prepare := prepareS.(*paxosproto.Prepare)
			//got a Prepare message
			dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*paxosproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
			commit := commitS.(*paxosproto.Commit)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
			commit := commitS.(*paxosproto.CommitShort)
			//got a Commit message
			dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
			prepareReply := prepareReplyS.(*paxosproto.PrepareReply)
			//got a Prepare reply
			dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*paxosproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
			r.handleAcceptReply(acceptReply)
			break

		//case beacon := <-r.BeaconChan:
		//	dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
		//	r.ReplyBeacon(beacon)
		//	break

		//case <-slowClockChan:
		//	if r.Beacon {
		//		for q := int32(0); q < int32(r.N); q++ {
		//			if q == r.Id {
		//				continue
		//			}
		//			r.SendBeacon(q)
		//		}
		//	}
		//	break
		//default:
		//	break
		}
	}
}

func (r *Replica) setupShards(masterAddr string, masterPort int) {
  if r.Id != 0 {
    return
  }
  // Multi-paxos doesn't need intershard communication
  var args masterproto.ShardReadyArgs
  var reply masterproto.ShardReadyReply

  done := false
  for !done {
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

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) updateCommittedUpTo() {
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		r.instanceSpace[r.committedUpTo+1].status == COMMITTED {
		r.committedUpTo++
	}
}

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &paxosproto.Prepare{r.Id, instance, ballot, ti}

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

var pa paxosproto.Accept

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command, batchSize int32, startIndex int64) {
	/*defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()*/
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = command
	pa.CommandCount = batchSize
	pa.StartIndex = startIndex
	args := &pa
	//args := &paxosproto.Accept{r.Id, instance, ballot, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

var pc paxosproto.Commit
var pcs paxosproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command, batchSize int32) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	pc.CommandCount = batchSize
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	argsShort := &pcs

	//args := &paxosproto.Commit{r.Id, instance, command}

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

func (r *Replica) handlePropose(propose *genericsmr.Propose) int {

	//A := time.Now()

	//dlog.Printf("got handlePropose for CommandID %v at time %v\n", propose.CommandId, time.Now().UnixNano())
	//dlog.Printf("Proposal with op %d\n", propose.Command.Op)
	if !r.IsLeader {
		preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return -1
	}


	//B := time.Now()
	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	//C1 := time.Now()
	instNo := r.crtInstance
	r.crtInstance++

	//C2 := time.Now()
	batchSize := int64(len(r.ProposeChan) + 1)
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}
	end := r.ringIndexEnd
	ringCapacity := MAX_BATCH - (r.ringIndexStart - end)
	dlog.Printf("ring capacity = %v; start = %v, end = %v", ringCapacity, r.ringIndexStart, end)
	if batchSize > ringCapacity {
		batchSize = ringCapacity
	}
	if batchSize <= 0 {
		return 0
	}
	dlog.Printf("Got HandlePropose with Batchsize = %d\n", batchSize)

	//C3 := time.Now()
	//cmds := make([]state.Command, batchSize)
	//proposals := make([]*genericsmr.Propose, batchSize)

	//C4 := time.Now()
	prop := propose
	i := int64(0)

	//D := time.Now()
	for i < (batchSize) {
		dlog.Printf("b_%v from PID %v, CommandId = %v", i, prop.Timestamp, prop.CommandId)
		r.handleProposeCommands[(r.ringIndexStart + i) % MAX_BATCH] = prop.Command
		r.handleProposeProposals[(r.ringIndexStart + i) % MAX_BATCH] = prop
		i++
		if i < (batchSize) {
			prop = <-r.ProposeChan
		}
	}
	oldRingIndex := r.ringIndexStart
	r.ringIndexStart += batchSize

	//E1 := time.Now()
	//var E2, E3, E4, E5 time.Time
	if r.defaultBallot == -1 {
		r.instanceSpace[instNo] = &Instance{
			r.handleProposeCommands,
			r.makeUniqueBallot(0),
			PREPARING,
			int32(batchSize),
			oldRingIndex,
			&LeaderBookkeeping{r.handleProposeProposals, 0, 0, 0, 0}}
		r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
		//dlog.Printf("Classic round for instance %d\n", instNo)
	} else {
		r.instanceSpace[instNo] = &Instance{
			r.handleProposeCommands,
			r.defaultBallot,
			PREPARED,
			int32(batchSize),
			oldRingIndex,
			&LeaderBookkeeping{r.handleProposeProposals, 0, 0, 0, 0}}

		//E2 = time.Now()
		r.recordInstanceMetadata(r.instanceSpace[instNo])
		//E3 = time.Now()
		r.recordCommands(r.handleProposeCommands, r.instanceSpace[instNo].batchSize, oldRingIndex)
		//E4 = time.Now()
		r.sync()
		//E5 = time.Now()

		//log.Printf("bcastAccepting %v proposals", len(cmds))
		r.bcastAccept(instNo, r.defaultBallot, r.handleProposeCommands, int32(batchSize), oldRingIndex)
		//dlog.Printf("Fast round for instance %d\n", instNo)
	}
	/*F := time.Now()

	log.Printf("A = %v", B.Sub(A).Microseconds())
	log.Printf("B = %v", C1.Sub(B).Microseconds())
	log.Printf("C = %v", D.Sub(C1).Microseconds())
	log.Printf("C1 = %v", C2.Sub(C1).Microseconds())
	log.Printf("C2 = %v", C3.Sub(C2).Microseconds())
	log.Printf("C3 = %v", C4.Sub(C3).Microseconds())
	log.Printf("C4 = %v", D.Sub(C4).Microseconds())
	log.Printf("D = %v", E1.Sub(D).Microseconds())
	log.Printf("E = %v", F.Sub(E1).Microseconds())
	log.Printf("E1 = %v", E2.Sub(E1).Microseconds())
	log.Printf("E2 = %v", E3.Sub(E2).Microseconds())
	log.Printf("E3 = %v", E4.Sub(E3).Microseconds())
	log.Printf("E4 = %v", E5.Sub(E4).Microseconds())
	log.Printf("E5 = %v", F.Sub(E5).Microseconds())*/
	return int(batchSize)
	//dlog.Printf("finished handlePropose %v\n", time.Now().UnixNano())
}

func (r *Replica) handlePrepare(prepare *paxosproto.Prepare) {
	inst := r.instanceSpace[prepare.Instance]
	var preply *paxosproto.PrepareReply

	if inst == nil {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, nil}
	} else {
		ok := TRUE
		if prepare.Ballot < inst.ballot {
			ok = FALSE
		}
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
	}

	r.replyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handleAccept(accept *paxosproto.Accept) {
	inst := r.instanceSpace[accept.Instance]
	var areply *paxosproto.AcceptReply

	if inst == nil {
		if accept.Ballot < r.defaultBallot {
			areply = &paxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
		} else {
			r.instanceSpace[accept.Instance] = &Instance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				accept.CommandCount,
				accept.StartIndex,
				nil}
			/*for i := int32(0); i < accept.CommandCount; i++ {
				dlog.Printf("Replica got HANDLE ACCEPT: PID = %v; CommandId = %v", accept.PID[(int64(i) + accept.StartIndex) % MAX_BATCH], accept.CommandId[(int64(i) + accept.StartIndex) % MAX_BATCH])
			}*/
			areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
		}
	} else if inst.ballot > accept.Ballot {
		areply = &paxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
	} else if inst.ballot < accept.Ballot {
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//TODO: is this correct?
			// try the proposal in a different instance
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	} else {
		// reordered ACCEPT
		r.instanceSpace[accept.Instance].cmds = accept.Command
		r.instanceSpace[accept.Instance].batchSize = accept.CommandCount
		if r.instanceSpace[accept.Instance].status != COMMITTED {
			r.instanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
	}

	if areply.OK == TRUE {
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommands(accept.Command, accept.CommandCount, accept.StartIndex)
		r.sync()
	}

	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *paxosproto.Commit) {
	inst := r.instanceSpace[commit.Instance]

	//dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{
			commit.Command,
			commit.Ballot,
			COMMITTED,
			commit.CommandCount,
			0,
			nil}
	} else {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		r.instanceSpace[commit.Instance].batchSize = commit.CommandCount
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command, commit.CommandCount, 0)
}

func (r *Replica) handleCommitShort(commit *paxosproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	//dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{nil,
			commit.Ballot,
			COMMITTED,
			commit.Count,
			0,
			nil}
	} else {
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
}

func (r *Replica) handlePrepareReply(preply *paxosproto.PrepareReply) {
	inst := r.instanceSpace[preply.Instance]

	if inst.status != PREPARING {
		// TODO: should replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		inst.lb.prepareOKs++

		if preply.Ballot > inst.lb.maxRecvBallot {
			inst.cmds = preply.Command
			inst.lb.maxRecvBallot = preply.Ballot
			if inst.lb.clientProposals != nil {
				// there is already a competing command for this instance,
				// so we put the client proposal back in the queue so that
				// we know to try it in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.ProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}

		if inst.lb.prepareOKs+1 > r.N>>1 {
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				r.defaultBallot = inst.ballot
			}
			r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
			r.sync()
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds, inst.batchSize, inst.startIndex)
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
					r.ProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}
	}
}

func (r *Replica) handleAcceptReply(areply *paxosproto.AcceptReply) {

	inst := r.instanceSpace[areply.Instance]

	if inst.status != PREPARED && inst.status != ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if areply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {
			inst = r.instanceSpace[areply.Instance]
			inst.status = COMMITTED
			/*if inst.lb.clientProposals != nil {
				// give client the all clear
				if len(inst.cmds) > 1 {
					dlog.Printf("inst.cmds = %v\n", inst.cmds)
					panic("what AAAHHHH")
				}
				for i := 0; i < len(inst.cmds); i++ {
					if !r.NeedsWaitForExecute(&inst.cmds[i]) {
						dlog.Printf("AAA\n")
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.lb.clientProposals[i].CommandId,
							state.NIL,
							inst.lb.clientProposals[i].Timestamp}
						r.ReplyProposeTS(propreply, inst.lb.clientProposals[i].Reply)
					}
				}
			}*/
			/*for i := int32(0); i < areply.CommandCount; i++ {
				dlog.Printf("Replica got HANDLE ACCEPT REPLY: PID = %v; CommandId = %v", areply.PID[(int64(i)+inst.startIndex) % MAX_BATCH], areply.CommandId[(int64(i)+inst.startIndex) % MAX_BATCH])
			}*/

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
			r.sync() //is this necessary?

			r.updateCommittedUpTo()

			r.bcastCommit(areply.Instance, inst.ballot, inst.cmds, inst.batchSize)
			//dlog.Printf("This Command %v got accepted at time %v\n", inst.lb.clientProposals[0].CommandId, time.Now().UnixNano())
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
				offset := int32(inst.startIndex)
				for j := int32(0); j < inst.batchSize; j++ {
					val := inst.cmds[(offset + j) % MAX_BATCH].Execute(r.State)
					r.ringIndexEnd++
					if (r.IsLeader && (r.ringIndexEnd > r.ringIndexStart)) {
						dlog.Printf("start = %v, end = %v", r.ringIndexStart, r.ringIndexEnd)
						panic("PRODUCER CoNSUMER BORKE")
					}
					//dlog.Printf("command %v got EXECUTED at time %v\n", inst.lb.clientProposals[j].CommandId, time.Now().UnixNano())
					if inst.lb != nil && inst.lb.clientProposals != nil {
						dlog.Printf("BBB PID = %v Command = %v\n", inst.lb.clientProposals[(offset + j) % MAX_BATCH].Timestamp, inst.lb.clientProposals[(offset + j) % MAX_BATCH].CommandId)
						propreply := &genericsmrproto.ProposeReplyTS{
							TRUE,
							inst.lb.clientProposals[(offset + j) % MAX_BATCH].CommandId,
							val,
							inst.lb.clientProposals[(offset + j) % MAX_BATCH].Timestamp}
						r.ReplyProposeTS(propreply, inst.lb.clientProposals[(offset + j) % MAX_BATCH].Reply)
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
