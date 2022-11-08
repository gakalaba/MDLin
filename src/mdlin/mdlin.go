package mdlin

import (
	"encoding/binary"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"mdlinproto"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const NUM_CLIENTS = uint8(2)

const MAX_BATCH = 5000

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	prepareChan         chan *genericsmr.RPCMessage
	acceptChan          chan *genericsmr.RPCMessage
	commitChan          chan *genericsmr.RPCMessage
	commitShortChan     chan *genericsmr.RPCMessage
	prepareReplyChan    chan *genericsmr.RPCMessage
	acceptReplyChan     chan *genericsmr.RPCMessage
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
	batchingEnabled     bool
  // Add these for multidispatch
  nextSeqNo           map[int64]int64 // Mapping client PID to next expected sequence number
  //buffIS              []*Instance // in memory log of buffered instances (instances that came out of order from clients)
  //crtBuffIS           int32 // index into buffIS
  noProposalsReady    bool
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
	lb     *LeaderBookkeeping
  pid    []int64
  seqno  []int64
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.MDLPropose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
}

func NewReplica(id int, peerAddrList []string, thrifty bool,
	durable bool, batch bool) *Replica {
	r := &Replica{
		genericsmr.NewReplica(id, peerAddrList, thrifty),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, 3*genericsmr.CHAN_BUFFER_SIZE),
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
    // make([]*Instance, 15*1024*1024),
    // 0
    true}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(mdlinproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(mdlinproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(mdlinproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(mdlinproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(mdlinproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(mdlinproto.AcceptReply), r.acceptReplyChan)

	go r.run()

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

func (r *Replica) replyPrepare(replicaId int32, reply *mdlinproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
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
    log.Println("I'm the leader")
	}

	clockChan = make(chan bool, 1)
	if r.batchingEnabled {
		go r.clock()
	}

	onOffProposeChan := r.MDLProposeChan

	for !r.Shutdown {

		select {

		case <-clockChan:
			//activate the new proposals channel
      log.Println("---------clockChan---------")
			onOffProposeChan = r.MDLProposeChan
			break

		case propose := <-onOffProposeChan:
      log.Println("---------ProposalChan---------")
      log.Printf("len = %d", len(r.MDLProposeChan))
      log.Printf("CommandId = %d, Command = %d, Timestamp = %d, SeqNo = %d, PID = %d", propose.CommandId, propose.Command, propose.Timestamp, propose.SeqNo, propose.PID)
			//got a Propose from a client
      preply := &mdlinproto.ProposeReply{FALSE, propose.CommandId, state.Value(propose.PID), propose.Timestamp, propose.SeqNo}
			r.MDReplyPropose(preply, propose.Reply)


      // UNCOMMENT r.handlePropose(propose)
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			if (r.batchingEnabled && !r.noProposalsReady)  {
				onOffProposeChan = nil
			}
			break

		case prepareS := <-r.prepareChan:
      log.Println("---------PrepareChan---------")
			prepare := prepareS.Message.(*mdlinproto.Prepare)
			//got a Prepare message
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan:
      log.Println("---------AcceptChan---------")
			accept := acceptS.Message.(*mdlinproto.Accept)
			//got an Accept message
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
      log.Println("---------CommitChan---------")
			commit := commitS.Message.(*mdlinproto.Commit)
			//got a Commit message
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
      log.Println("---------CommitShortChan---------")
			commit := commitS.Message.(*mdlinproto.CommitShort)
			//got a Commit message
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
      log.Println("---------PrepareReplyChan---------")
			prepareReply := prepareReplyS.Message.(*mdlinproto.PrepareReply)
			//got a Prepare reply
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
      log.Println("---------AcceptReplyChan---------")
			acceptReply := acceptReplyS.Message.(*mdlinproto.AcceptReply)
			//got an Accept reply
			r.handleAcceptReply(acceptReply)
			break

		case metricsRequest := <-r.MetricsChan:
      log.Println("---------MetricsChan---------")
			// Empty reply because there are no relevant metrics
			reply := &genericsmrproto.MetricsReply{}
			reply.Marshal(metricsRequest.Reply)
			metricsRequest.Reply.Flush()
			break
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
  log.Println("Inside broadcast prepare!")
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
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

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command, pids []int64, seqnos []int64, expectedMap map[int64]int64) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = command
  pa.PIDs = pids
  pa.SeqNos = seqnos
  pa.ExpectedSeqs = expectedMap
	args := &pa
	//args := &mdlinproto.Accept{r.Id, instance, ballot, command}

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

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command, pids []int64, seqnos []int64) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
  pc.PIDs = pids
  pc.SeqNos = seqnos
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
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
    log.Println("I'm not the leader, sending client a FALSE reply")
		preply := &mdlinproto.ProposeReply{FALSE, propose.CommandId, state.NIL, 0, -1}
    log.Println("Responding to client with OK = false (0) because this isn't the leader")
		r.MDReplyPropose(preply, propose.Reply)
		return
	}

  // Get batch size
	batchSize := 1
  numProposals := len(r.MDLProposeChan)+1
	if r.batchingEnabled {
		batchSize := numProposals + 1
		if batchSize > MAX_BATCH {
			batchSize = MAX_BATCH
		}
	}

  cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.MDLPropose, batchSize)
  pids := make([]int64, batchSize)
  seqnos := make([]int64, batchSize)

  found := 0
  var expectedSeqno int64
  prop := propose
  i := 1
  for (found < batchSize && i <= numProposals) {
    pid := prop.PID
    seqno := prop.SeqNo
    expectedSeqno = 0
    if val, ok := r.nextSeqNo[pid]; ok {
      expectedSeqno = val
    }
    log.Printf("found = %d, pid = %d, seqno = %d, expectedSeqNo = %d, len channel = %d", found, pid, seqno, expectedSeqno, numProposals)
    if (seqno != expectedSeqno) {
      // Add to buffer
      log.Println("Out of order, buffering back into channel")
      r.MDLProposeChan <- prop
    } else {
      cmds[found] = prop.Command
      proposals[found] = prop
      pids[found] = pid
      seqnos[found] = seqno
      found++
      r.nextSeqNo[pid]++
      //log.Println("We made an entry")
    }
    i++
    if (found < batchSize && i <= numProposals) {
      log.Println("Pulled out the next one")
      prop = <-r.MDLProposeChan
    }
  }

  // None of the proposals in the channel 
  // are ready to be added to the log
  if (found == 0) {
    log.Println("None of the proposals pulled out of the channel were ready!")
    r.noProposalsReady = true
    preply := &mdlinproto.ProposeReply{FALSE, -1, state.NIL, 0, -1} // TODO which expected Seqno will this be?
		log.Println("Responding to client with OK = false (0) because no proposals were ready")
    r.MDReplyPropose(preply, propose.Reply)
    // TODO consider replying to client that it's out of order
    return
  }

  r.noProposalsReady = false
	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	instNo := r.crtInstance
	r.crtInstance++

	if r.defaultBallot == -1 {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.makeUniqueBallot(0),
			PREPARING,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0},
      pids,
      seqnos}
    log.Println("(candidate) leader broadcasting prepares....")
		r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
	} else {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.defaultBallot,
			PREPARED,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0},
      pids,
      seqnos}

		r.recordInstanceMetadata(r.instanceSpace[instNo])
		r.recordCommands(cmds)
		r.sync()
    log.Println("Leader broadcasting Accepts")

    // Make a copy of the nextSeqNo map
    expectedSeqs := make(map[int64]int64)
    copyMap(expectedSeqs, r.nextSeqNo)
		r.bcastAccept(instNo, r.defaultBallot, cmds, pids, seqnos, expectedSeqs)
	}
}

// Helper to copy contents of map2 to map1
func copyMap(map1 map[int64]int64, map2 map[int64]int64) {
  for k, v := range map2 {
    map1[k] = v
  }
}

func (r *Replica) handlePrepare(prepare *mdlinproto.Prepare) {
	inst := r.instanceSpace[prepare.Instance]
	var preply *mdlinproto.PrepareReply

	if inst == nil {
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

func (r *Replica) handleAccept(accept *mdlinproto.Accept) {
  //TODO for now.... are we making any of the decisions based on the nextSeqnos???
	inst := r.instanceSpace[accept.Instance]
	var areply *mdlinproto.AcceptReply

  expectedSeqs := accept.ExpectedSeqs
	if inst == nil {
		if accept.Ballot < r.defaultBallot {
			areply = &mdlinproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
		} else {
			r.instanceSpace[accept.Instance] = &Instance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				nil,
        accept.PIDs,
        accept.SeqNos}
			areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
		}
	} else if inst.ballot > accept.Ballot {
		areply = &mdlinproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
	} else if inst.ballot < accept.Ballot {
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
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
		r.instanceSpace[accept.Instance].cmds = accept.Command
		if r.instanceSpace[accept.Instance].status != COMMITTED {
			r.instanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &mdlinproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
	}

	if areply.OK == TRUE {
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
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
		r.instanceSpace[commit.Instance] = &Instance{
			commit.Command,
			commit.Ballot,
			COMMITTED,
			nil,
      commit.PIDs,
      commit.SeqNos}
	} else {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = COMMITTED
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
		r.instanceSpace[commit.Instance] = &Instance{nil,
			commit.Ballot,
			COMMITTED,
			nil,
      nil,
      nil}
	} else {
		r.instanceSpace[commit.Instance].status = COMMITTED
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
	inst := r.instanceSpace[preply.Instance]

	if inst.status != PREPARING {
		// TODO: should replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		inst.lb.prepareOKs++

		if preply.Ballot > inst.lb.maxRecvBallot { //TODO CHANGE THIS THIS WILL BREAK MDL
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

		if inst.lb.prepareOKs+1 > r.N>>1 {
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				r.defaultBallot = inst.ballot
			}
			r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
			r.sync()
      expectedSeqs := make(map[int64]int64)
      copyMap(expectedSeqs, r.nextSeqNo)
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds, inst.pid, inst.seqno, expectedSeqs)
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
			if inst.lb.clientProposals != nil && state.AllBlindWrites(inst.cmds) {
				// give client the all clear
				for i := 0; i < len(inst.cmds); i++ {
					propreply := &mdlinproto.ProposeReply{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp,
            inst.seqno[i]+1}
					//_, filename, line, _ := runtime.Caller(1)
          //log.Printf("[error] %s:%d %v", filename, line, err)
          log.Println("Responding to client with OK = true (1) in handleAcceptReply")
          r.MDReplyPropose(propreply, inst.lb.clientProposals[i].Reply)
				}
			}

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
			r.sync() //is this necessary?

			r.updateCommittedUpTo()

			r.bcastCommit(areply.Instance, inst.ballot, inst.cmds, inst.pid, inst.seqno)
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
				for j := 0; j < len(inst.cmds); j++ {
					val := inst.cmds[j].Execute(r.State)
					if inst.lb != nil && !state.AllBlindWrites(inst.cmds) {
						propreply := &mdlinproto.ProposeReply{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							inst.lb.clientProposals[j].Timestamp,
              inst.seqno[j]+1}
						log.Println("Responding to client with OK = true (1) in executeCommand")
            r.MDReplyPropose(propreply, inst.lb.clientProposals[j].Reply)
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
