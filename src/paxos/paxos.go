package paxos

import (
	"encoding/binary"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"paxosproto"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

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
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose // What are these?
	maxRecvBallot   int32                 // highest term
	prepareOKs      int                   // number of prepare OKs
	acceptOKs       int                   // number of accept OKs
	nacks           int                   // number of fails on an AcceptReply and PrepareReply
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
	}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(paxosproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(paxosproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(paxosproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(paxosproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(paxosproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(paxosproto.AcceptReply), r.acceptReplyChan)

	go r.run()

	return r
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
		cmds[i].Marshal(io.Writer(r.StableStore)) //Write the commands {Op, K, V} (log entries) to stable storage (file)
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

func (r *Replica) replyPrepare(replicaId int32, reply *paxosproto.PrepareReply) {
	r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *paxosproto.AcceptReply) {
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
	}

	clockChan = make(chan bool, 1)
	if r.batchingEnabled {
		go r.clock()
	}

	onOffProposeChan := r.ProposeChan

	for !r.Shutdown {

		select {

		case <-clockChan:
			//activate the new proposals channel
			onOffProposeChan = r.ProposeChan
			break

		case propose := <-onOffProposeChan:
      log.Println("ProposeChan")
			//got a Propose from a client
			r.handlePropose(propose)
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			//**This is a golang specific hack!!
			if r.batchingEnabled {
				onOffProposeChan = nil
			}
			break

		case prepareS := <-r.prepareChan:
			log.Println("PrepareChan")
      prepare := prepareS.Message.(*paxosproto.Prepare) // Casting Message interface to concrete type *paxosproto.Prepare
			//got a Prepare message
			r.handlePrepare(prepare)
			break

		case acceptS := <-r.acceptChan: // follower got something from leader's bCastAccept
    log.Println("AcceptChan")
			accept := acceptS.Message.(*paxosproto.Accept)
			//got an Accept message
			r.handleAccept(accept)
			break

		case commitS := <-r.commitChan:
      log.Println("CommitChan")
			commit := commitS.Message.(*paxosproto.Commit)
			//got a Commit message
			r.handleCommit(commit)
			break

		case commitS := <-r.commitShortChan:
      log.Println("CommitShortChan")
			commit := commitS.Message.(*paxosproto.CommitShort)
			//got a Commit message
			r.handleCommitShort(commit)
			break

		case prepareReplyS := <-r.prepareReplyChan:
      log.Println("PrepareReplyChan")
			prepareReply := prepareReplyS.Message.(*paxosproto.PrepareReply)
			//got a Prepare reply
			r.handlePrepareReply(prepareReply)
			break

		case acceptReplyS := <-r.acceptReplyChan:
      log.Println("AcceptReplyChan")
			acceptReply := acceptReplyS.Message.(*paxosproto.AcceptReply)
			//got an Accept reply
			r.handleAcceptReply(acceptReply)
			break

		case metricsRequest := <-r.MetricsChan:
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

var pa paxosproto.Accept //why is this a global...?

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = command
	args := &pa
	//args := &paxosproto.Accept{r.Id, instance, ballot, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	q := r.Id

	for sent := 0; sent < n; { // send to all nodes that are alive (except self)
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

var pc paxosproto.Commit
var pcs paxosproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	argsShort := &pcs

	//args := &paxosproto.Commit{r.Id, instance, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1 //If thrifty, "Use only as many messages as strictly required for inter-replica communication." In this case we send to ourselves and N/2, so a Q total
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
		r.SendMsg(q, r.commitShortRPC, argsShort) //doing short commit
	}
	if r.Thrifty && q != r.Id { //we only sent to N/2 including ourselves, so send remaining messages
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

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
  log.Println("handle proipose!")
	if !r.IsLeader {
		preply := &genericsmrproto.ProposeReply{FALSE, -1, state.NIL, 0}
    log.Println("not the elader")
		r.ReplyPropose(preply, propose.Reply)
		return
	}
	// We are the leader
	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}

	instNo := r.crtInstance //get next empty index in our log
	r.crtInstance++

	batchSize := 1
	if r.batchingEnabled {
		batchSize := len(r.ProposeChan) + 1
		if batchSize > MAX_BATCH {
			batchSize = MAX_BATCH
		}
	}

	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command //Store the command the client sent
	proposals[0] = propose    //Store the proposal message from client

  log.Printf("The command id was %d", propose.CommandId)
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan // pull everything out of the proposal chan (well, as much as we clocked batchSize at above)
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	if r.defaultBallot == -1 {
		// the instanceSpace is our log, we add the entry to OUR log
		r.instanceSpace[instNo] = &Instance{
			cmds, // a single instance in the log might have batched commands!!
			r.makeUniqueBallot(0),
			PREPARING,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0}}
    log.Println("Bcasp Prepare")
		r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
	} else {
		r.instanceSpace[instNo] = &Instance{
			cmds,
			r.defaultBallot,
			PREPARED,
			&LeaderBookkeeping{proposals, 0, 0, 0, 0}} // This is where clientProposals is SET to a non-nil value

		r.recordInstanceMetadata(r.instanceSpace[instNo])
		r.recordCommands(cmds)
		r.sync() //we record the entry in our stable storage log
    log.Println("Bcasp Accept")

		r.bcastAccept(instNo, r.defaultBallot, cmds) // and then we replicate it at a majority
	}
}

func (r *Replica) handlePrepare(prepare *paxosproto.Prepare) {
	inst := r.instanceSpace[prepare.Instance]
	var preply *paxosproto.PrepareReply

	if inst == nil {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
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
			areply = &paxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot} // REJECT if proposed entry is from old term
		} else {
			r.instanceSpace[accept.Instance] = &Instance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				nil}
			areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot} // ACCEPT
		}
	} else if inst.ballot > accept.Ballot { //The entry i have wherever the thing being proposed will go is from a more recent term
		areply = &paxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot} // REJECT

		/// difference between following 2 cases?
	} else if inst.ballot < accept.Ballot {
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot} // ACCEPT

		// If the instance we're overwriting was one that had been proposed from a
		// leader, those are pending client requests... retry them
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//TODO: is this correct?
			// try the proposal in a different instance
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	} else { // proposed thing is equal term to term of whati have in the log there
		// reordered ACCEPT
		r.instanceSpace[accept.Instance].cmds = accept.Command //overwrite the entry
		if r.instanceSpace[accept.Instance].status != COMMITTED {
			r.instanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot} // ACCEPT
	}

	if areply.OK == TRUE { // append to our stable log
		r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
		r.recordCommands(accept.Command)
		r.sync()
	}

	r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *paxosproto.Commit) {
	inst := r.instanceSpace[commit.Instance]

	if inst == nil {
		r.instanceSpace[commit.Instance] = &Instance{
			commit.Command,
			commit.Ballot,
			COMMITTED,
			nil}
	} else {
		r.instanceSpace[commit.Instance].cmds = commit.Command
		r.instanceSpace[commit.Instance].status = COMMITTED
		r.instanceSpace[commit.Instance].ballot = commit.Ballot

		// If we're committing something and we've somehow never resubmitted the
		// clientProposals pending in it's channel we should retry them
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.ProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
}

// main difference is we dont include the command in the log entry? this is safe due to quorum intersection property
func (r *Replica) handleCommitShort(commit *paxosproto.CommitShort) {
	inst := r.instanceSpace[commit.Instance]

	if inst == nil { // i wasn't a part of hte consensus group, but it's being committed so i'll add it for sure
		r.instanceSpace[commit.Instance] = &Instance{nil,
			commit.Ballot,
			COMMITTED,
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

	r.recordInstanceMetadata(r.instanceSpace[commit.Instance]) // AKA update the COMMIT Instance.status
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
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds)
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
		inst.lb.acceptOKs++               //how is this not atomic
		if inst.lb.acceptOKs+1 > r.N>>1 { //IF WE HAVE A QUORUM OF ACCEPTS then update the commit index
			inst = r.instanceSpace[areply.Instance]
			inst.status = COMMITTED
			if inst.lb.clientProposals != nil && state.AllBlindWrites(inst.cmds) {
				// give client the all clear... we don't need to wait for value,
				// these are WRITES otherwise the execute will send the values
				// ProposeReply struct:=
				// OK        uint8
				// CommandId int32
				// Value     state.Value
				// Timestamp int64
				for i := 0; i < len(inst.cmds); i++ {
					propreply := &genericsmrproto.ProposeReply{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL, // these are all writes, so we don't give a value back
						inst.lb.clientProposals[i].Timestamp}
					r.ReplyPropose(propreply, inst.lb.clientProposals[i].Reply) //we pass in the client wire bufio
				}
			}

			r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
			r.sync() //is this necessary?

			r.updateCommittedUpTo() // This directly impacts the background executeCommands function

			r.bcastCommit(areply.Instance, inst.ballot, inst.cmds) // so that the other replicas can also execute this command!!!
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
					// the following check lets us know the main handle function didn't notify the client for this instance
					// which might happen if the commands were all writes or if we're the leader
					if inst.lb != nil && !state.AllBlindWrites(inst.cmds) {
						propreply := &genericsmrproto.ProposeReply{
							TRUE,
							inst.lb.clientProposals[j].CommandId,
							val,
							inst.lb.clientProposals[j].Timestamp}
            log.Println("Responding to client with OK = true (1) in executeCommand, we executed command %d", inst.lb.clientProposals[0].CommandId)
						r.ReplyPropose(propreply, inst.lb.clientProposals[j].Reply)
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
