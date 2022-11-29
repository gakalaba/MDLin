package mdlin

import (
	"encoding/binary"
	"genericsmr"
	"genericsmrproto"
	"io"
	"log"
	"mdlinproto"
	"mysort"
	"state"
	"time"
  "net"
  "bufio"
  "fastrpc"
  "fmt"
  "sync"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const NUM_CLIENTS = uint8(2)
const NUM_OUTSTANDING_INST = 10000

const MAX_BATCH = 5000

type Replica struct {
	*genericsmr.Replica // extends a generic Paxos replica
	prepareChan         chan *genericsmr.RPCMessage
	acceptChan          chan *genericsmr.RPCMessage
	commitChan          chan *genericsmr.RPCMessage
	commitShortChan     chan *genericsmr.RPCMessage
	prepareReplyChan    chan *genericsmr.RPCMessage
	acceptReplyChan     chan *genericsmr.RPCMessage
	reorderChan         chan *genericsmr.RPCMessage
  reorderReplyChan    chan *genericsmr.RPCMessage
  prepareRPC          uint8
	acceptRPC           uint8
	commitRPC           uint8
	commitShortRPC      uint8
	prepareReplyRPC     uint8
	acceptReplyRPC      uint8
  reorderRPC          uint8
  reorderReplyRPC     uint8
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
  // Add these for multi-sharded multi-dispatch
  shardId       int
  shardAddrList []string
  shards        []net.Conn // cache of connections to all other replicas
  shardReaders  []*bufio.Reader
  shardWriters  []*bufio.Writer
  interShardChan chan *genericsmr.RPCMessage
  interShardReplyChan chan *genericsmr.RPCMessage
  shListener net.Listener
  propagatedbd       []mdlinproto.Tag
  buflock *sync.Mutex
  num_conflicts int64
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
  REORDERING
  REORDERED
	COMMITTED
)

type Instance struct {
	cmds   []state.Command
	ballot int32
	status InstanceStatus
	lb     *LeaderBookkeeping
	pid    int64
	seqno  int64
  version state.Version
  bd []mdlinproto.Tag // for now it's Key.Version.PID.Index
  initial_ld []mdlinproto.Tag
  new_ld [][]mdlinproto.Tag
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.MDLPropose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	reorderOKs      int
  nacks           int
}

func tagtostring(t mdlinproto.Tag) string {
  return fmt.Sprintf("Tag = %d.%d.%d.CommandID(%d)", t.K, t.Version, t.PID, t.CommandId)
}

func printDeps(deps []mdlinproto.Tag, s string) {
  if (deps == nil || len(deps) == 0) {
    log.Printf("   %s: -->nil", s)
  } else {
    log.Printf("   %s:", s)
  }
  for _, d := range deps {
    log.Println("   ->", tagtostring(d))
  }
}

func NewReplica(id int, peerAddrList []string, shardsList []string, shId int,
	thrifty bool, durable bool, batch bool) *Replica {
	r := &Replica{
		genericsmr.NewReplica(id, peerAddrList, thrifty),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		make(chan *genericsmr.RPCMessage, 3*genericsmr.CHAN_BUFFER_SIZE),
    make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
    make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0,
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
    shId,
    shardsList,
    make([]net.Conn, len(shardsList)),
    make([]*bufio.Reader, len(shardsList)),
    make([]*bufio.Writer, len(shardsList)),
    make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
    make(chan *genericsmr.RPCMessage, genericsmr.CHAN_BUFFER_SIZE),
    nil,
    make([]mdlinproto.Tag, 0),
    new(sync.Mutex),
    0}

	r.Durable = durable

	r.prepareRPC = r.RegisterRPC(new(mdlinproto.Prepare), r.prepareChan)
	r.acceptRPC = r.RegisterRPC(new(mdlinproto.Accept), r.acceptChan)
	r.commitRPC = r.RegisterRPC(new(mdlinproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(mdlinproto.CommitShort), r.commitShortChan)
	r.prepareReplyRPC = r.RegisterRPC(new(mdlinproto.PrepareReply), r.prepareReplyChan)
	r.acceptReplyRPC = r.RegisterRPC(new(mdlinproto.AcceptReply), r.acceptReplyChan)
  r.reorderRPC = r.RegisterRPC(new(mdlinproto.Reorder), r.reorderChan)
  r.reorderReplyRPC = r.RegisterRPC(new(mdlinproto.ReorderReply), r.reorderReplyChan)

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
			log.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.shardReaders[i] = bufio.NewReader(r.shards[i])
		r.shardWriters[i] = bufio.NewWriter(r.shards[i])

		go r.shardListener(i, r.shardReaders[i])
	}
	<-done
  log.Printf("Shard Leader %d: Done connecting to all shard leaders\n", r.shardId)
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForShardConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

  var sherr error
	r.shListener, sherr = net.Listen("tcp", r.shardAddrList[r.shardId])
  if (sherr != nil) {
    panic(sherr)
  }
	for i := r.shardId + 1; i < len(r.shardAddrList); i++ {
		conn, err := r.shListener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			log.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.shards[id] = conn
		r.shardReaders[id] = bufio.NewReader(conn)
		r.shardWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true

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

		case mdlinproto.INTERSHARD:
      intershard := new(mdlinproto.InterShard)
			if err = intershard.Unmarshal(reader); err != nil {
				break
			}
      //log.Println(intershard)
      r.interShardChan <- &genericsmr.RPCMessage{intershard, 0, int64(rid)}
			break

    case mdlinproto.INTERSHARD_REPLY:
      intershardreply := new(mdlinproto.InterShardReply)
      if err = intershardreply.Unmarshal(reader); err != nil {
        break
      }
      //log.Println(intershardreply)
      r.interShardReplyChan <- &genericsmr.RPCMessage{intershardreply, 0, int64(rid)}
      break

		default:
			panic("mdlin ERROR: received unknown message type")
		}
	}
}

// leaderId is the ID of the leader this message is being sent TO. it's an index
// msg is the actual message being sent of mdlinproto.InterShard* type
func (r *Replica) sendInterShardMsg(leaderId int64, msg fastrpc.Serializable, code int) {
  r.buflock.Lock()
  defer r.buflock.Unlock()

  if (leaderId != int64(r.shardId)) {
    w := r.shardWriters[leaderId]
    if (code == 0) {
      w.WriteByte(mdlinproto.INTERSHARD) // to tell what kind of message this is
    } else {
      w.WriteByte(mdlinproto.INTERSHARD_REPLY)
    }
    msg.Marshal(w) // marshall the message and send it into the w bufio object
    w.Flush()
  } else {
    log.Println("SENDING MESSAGE TO SELF!!!!")
    if (code == 0) {
      r.interShardChan <- &genericsmr.RPCMessage{msg, 0, int64(leaderId)}
    } else {
      r.interShardReplyChan <- &genericsmr.RPCMessage{msg, 0, int64(leaderId)}
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

func (r *Replica) replyAccept(replicaId int32, reply *mdlinproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyReorder(replicaId int32, reply *mdlinproto.ReorderReply) {
  r.SendMsg(replicaId, r.reorderReplyRPC, reply)
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

		case <-clockChan:
			//activate the new proposals channel
			log.Println("---------clockChan---------")
			onOffProposeChan = r.MDLProposeChan
			break

		case propose := <-onOffProposeChan:
			log.Println("---------ProposalChan---------")
			r.handlePropose(propose)
			//deactivate the new proposals channel to prioritize the handling of protocol messages
			if r.batchingEnabled && !r.noProposalsReady {
				onOffProposeChan = nil
			}
			break

    case ismsg := <-r.interShardChan:
      log.Println("----------InterShardChan--------")
      ismessage := ismsg.Message.(*mdlinproto.InterShard)
      r.handleInterShard(ismessage, ismsg.From)
      break

    case ismsgrep := <-r.interShardReplyChan:
      log.Println("-----InterShardReplyChan-----")
      isrmessage := ismsgrep.Message.(*mdlinproto.InterShardReply)
      r.handleInterShardReply(isrmessage, ismsgrep.From)

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

    case reorderS := <-r.reorderChan:
      log.Println("--------ReorderChan-------")
      reorder := reorderS.Message.(*mdlinproto.Reorder)
      //got a reorder message
      r.handleReorder(reorder)
      break

    case reorderReplyS := <-r.reorderReplyChan:
      log.Println("------ReorderReplyChan-----")
      reorderReply := reorderReplyS.Message.(*mdlinproto.ReorderReply)
      //got a reorder reply
      r.handleReorderReply(reorderReply)
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
  ini := r.committedUpTo
	for r.instanceSpace[r.committedUpTo+1] != nil &&
		(r.instanceSpace[r.committedUpTo+1].status == COMMITTED ||
     r.instanceSpace[r.committedUpTo+1].status == REORDERED) {
		r.committedUpTo++
    if len(r.propagatedbd) > 0 {
      r.propagatedbd = r.propagatedbd[1:]
    }
	}
  log.Printf("Updating commit index from %d to %d", ini, r.committedUpTo)
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

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command, pids int64, seqnos int64, expectedMap map[int64]int64, bd []mdlinproto.Tag) {
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
  pa.BatchDeps = bd
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

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command, pids int64, seqnos int64, status InstanceStatus) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
  var s string
  if (status == REORDERED) {
    s = "REORDERED"
  } else {
    s = "COMMITTED"
  }
  log.Printf("Calling bcastCOmmit with status %s", s)
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	pc.PIDs = pids
	pc.SeqNos = seqnos
  pc.Status = uint8(status)
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
		log.Println("I'm not the leader, sending client a FALSE reply")
		preply := &mdlinproto.ProposeReply{FALSE, propose.CommandId, state.NIL, 0, r.num_conflicts}
		log.Println("Responding to client with OK = false (0) because this isn't the leader")
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
  var versions state.Version

	for r.instanceSpace[r.crtInstance] != nil {
		r.crtInstance++
	}
	firstInst := r.crtInstance //The current available spot

	found := 0
	var expectedSeqno int64
	prop := propose
	i := 1
	for found < batchSize && i <= numProposals {
		pid := prop.PID
		seqno := prop.SeqNo
		expectedSeqno = 0
		if val, ok := r.nextSeqNo[pid]; ok {
			expectedSeqno = val
		}
		if seqno != expectedSeqno {
			// Add to buffer
			if _, ok := r.outstandingInst[pid]; !ok {
				r.outstandingInst[pid] = make([]*genericsmr.MDLPropose, 0)
			}
			r.outstandingInst[pid] = append(r.outstandingInst[pid], prop)
			if len(r.outstandingInst[pid]) > 1 {
				mysort.MergeSort(r.outstandingInst[pid])
			}
			log.Printf("Out of order, (got command %d seqno %d) buffering back into channel", prop.CommandId, seqno) //TODO do we need to sort?
		} else {
			cmds = append(cmds, prop.Command)
			proposals = append(proposals, prop)
      versions = handleVersion(prop.Command)
			found++
			r.nextSeqNo[pid]++
      r.addEntryToLog(cmds, proposals, pid, seqno, versions, prop.BatchDeps)
      // Check if any others are ready
			for true {
				log.Printf("looking for any others that might be ready from this PID %d", pid)
				l := len(r.outstandingInst[pid])
				log.Printf("apppears there are %d outstanding for this pid", l)
				expectedSeqno = r.nextSeqNo[pid]
				if (l > 0) && (r.outstandingInst[pid][l-1].SeqNo == expectedSeqno) {
					// We found previously outstanding requests that can be replicated now
					prop = r.outstandingInst[pid][l-1]
					r.outstandingInst[pid] = r.outstandingInst[pid][:l-1]
					r.nextSeqNo[pid]++ // Giving us linearizability!
          versions = handleVersion(prop.Command)
          cmds = make([]state.Command, 1)
          cmds[0] = prop.Command
          proposals = make([]*genericsmr.MDLPropose, 1)
          proposals[0] = prop
					log.Printf("head of it's buff Q is ready, with command %d", prop.CommandId)
					r.addEntryToLog(cmds, proposals, pid, expectedSeqno, versions, prop.BatchDeps)
					found++
				} else {
          break
        }
			}
		}
		i++
		if found < batchSize && i <= numProposals {
			log.Println("--->Pulled out the next one")
			prop = <-r.MDLProposeChan
		}
	}

	// None of the proposals in the channel
	// are ready to be added to the log
	if found == 0 {
		log.Println("None of the proposals pulled out of the channel or in the buffers are ready!")
		// We won't respond to the client, since that response
		// will come when the command gets unbuffered and later executed
		r.noProposalsReady = false //TODO delete me
		return
	}

	r.noProposalsReady = false

	for instNo := firstInst; instNo < r.crtInstance; instNo++ {
    e := r.instanceSpace[instNo]
    r.askShardsForDeps(e.bd, instNo) // TODO fix this later, just making sync for prints for now
		if r.defaultBallot == -1 {

			log.Println("    Step2. (candidate) leader broadcasting prepares....")
			r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
		} else {
			r.recordInstanceMetadata(r.instanceSpace[instNo])
			r.recordCommands(cmds)
			r.sync()
			log.Println("    Step2. Leader broadcasting Accepts")

			// Make a copy of the nextSeqNo map
			expectedSeqs := make(map[int64]int64)
			copyMap(expectedSeqs, r.nextSeqNo)
			r.bcastAccept(instNo, r.defaultBallot, e.cmds, e.pid, e.seqno, expectedSeqs, e.bd)
		}
	}
}

func (r *Replica) addEntryToLog(cmds []state.Command, proposals []*genericsmr.MDLPropose, pids int64,
              seqnos int64, versions state.Version, batchdeps []mdlinproto.Tag) {

  // Add entry to log
  log.Printf("Step 2. Shard Leader Creating Log Entry{%s, ver: %d, CommandId: %d, PID: %d, SeqNo: %d",
                      commandToStr(cmds[0]), versions, proposals[0].CommandId, pids, seqnos)
  printDeps(batchdeps, "bd")
  var initial_logdeps []mdlinproto.Tag
  if len(r.propagatedbd) == 0 {
    initial_logdeps = nil
  } else {
    initial_logdeps = r.propagatedbd //FIXME do we need a lock around this????
  }
  new_logdeps := make([][]mdlinproto.Tag, 0)
  r.propagatedbd = append(r.propagatedbd, batchdeps...) // propagate more bds
  if r.defaultBallot == -1 {
    r.instanceSpace[r.crtInstance] = &Instance{
      cmds,
      r.makeUniqueBallot(0),
      PREPARING,
      &LeaderBookkeeping{proposals, 0, 0, 0, 0, 0},
      pids,
      seqnos,
      versions,
      batchdeps,
      initial_logdeps,
      new_logdeps}
  } else {
    r.instanceSpace[r.crtInstance] = &Instance{
      cmds,
      r.defaultBallot,
      PREPARED,
      &LeaderBookkeeping{proposals, 0, 0, 0, 0, 0},
      pids,
      seqnos,
      versions,
      batchdeps,
      initial_logdeps,
      new_logdeps}
  }
  r.crtInstance++
}

// Helper to copy contents of map2 to map1
func copyMap(map1 map[int64]int64, map2 map[int64]int64) {
	for k, v := range map2 {
		map1[k] = v
	}
}

func handleVersion(c state.Command) state.Version {
  if state.IsRead(&c) {
    return state.GetVersion(&c)
  }
  return state.IncrVersion(&c)
}

func (r *Replica) askShardsForDeps(deps []mdlinproto.Tag, myInstance int32) {
  log.Println("Step 3. Send out req. to shard leaders for log dependencies")
  var shardTo int64
  for _, d:= range deps {
    //TODO 1. can optimize this if you cache deps previously asked for!
    //TODO 2. can also optimize if you combine messages to per shard... rn we'll be 
    //        sending n messages to a shard for n dependencies to the same shard
    shardTo = r.resolveShardFromKey(d.K)
    if (shardTo == -1) {
      panic("We couldn't find the shard responsible for this key??")
    }
    // Need to send inter shard RPC to ask for log dependencies with versions
    msg := &mdlinproto.InterShard{myInstance, d.CommandId}
    e := r.instanceSpace[myInstance]
    log.Printf("The len of this entry's proposals is %d", len(e.lb.clientProposals))
    log.Printf("      Step3. Shard %d commanId %d asking Shard %d for commandId %d, AskerInstance %d", r.shardId, e.lb.clientProposals[0].CommandId, shardTo, d.CommandId, myInstance)
    r.sendInterShardMsg(shardTo, msg, 0)
  }
}

func (r *Replica) resolveShardFromKey(k state.Key) int64 {
  // Return the shardId of the shard that is responsible for this key
  return state.KeyModulo(k, len(r.shards))
}

func (r *Replica) findEntry(commandId int32) (*Instance, int, int32) {
  // TODO what if instead of a linear search, we kept a map
  // that maps the CommandId to the index in the log??
  for i:=r.crtInstance-1; i>=0; i-- {
    if (r.instanceSpace[i] == nil) {
      panic("instanceSpace at this index should never be nil")
    }
    props := r.instanceSpace[i].lb.clientProposals
    for j:=0; j<len(props); j++ {
      if props[j].CommandId == commandId {
        return r.instanceSpace[i], j, i
      }
    }
  }
  return nil, -1, -1
}

var pis mdlinproto.InterShard
func (r *Replica) handleInterShard(ismessage *mdlinproto.InterShard, from int64) {
  log.Printf("*Shard %d recv messge from Shard %d for ld of commandId %d, AskerInstance %d*", r.shardId, from, ismessage.AskeeCommandId, ismessage.AskerInstance)
  // First, find the dependency, this is based on the ismessage.AskeeCommandId
  e, _, _ := r.findEntry(ismessage.AskeeCommandId)
  // If it exists, then reply to the shard
  if e != nil {
    //TODO it's not always safe to do this for uncommitted entries... but does that slow shit down?
    //RN using initial_ld... but should it be new?
    printDeps(e.initial_ld, "*initial ld")
    msg := &mdlinproto.InterShardReply{ismessage.AskerInstance, ismessage.AskeeCommandId, e.initial_ld} //TODO is this supposed to be initial or new
    r.sendInterShardMsg(from, msg, 1)
  } else {
    // If it doesn't exist, then put this message back in the channel to process later
    log.Println("     *batch dependency didn't arrive yet, adding back to proposal channel")
    r.interShardChan <- &genericsmr.RPCMessage{ismessage, 0, from}
    //TODO consider having a clock for these so that we can make progress on 
    //assigning versions without needlessly asking for them before they're ready
  }
}

func (r *Replica) handleInterShardReply(ismessage *mdlinproto.InterShardReply, from int64) {
  log.Printf("Step 4. Receiving deps from shard %d. AskerInstance: %d, AskeeCommandID: %d, ", from, ismessage.AskerInstance, ismessage.AskeeCommandId)
  // We have to identify the entry that wanted this information
  e := r.instanceSpace[ismessage.AskerInstance]
  if (e.bd == nil) {
    panic("We should never be getting a reply for an entry with bd=nil, since we shouldn't have issued a message")
  }
  // Add the dependencies/versions to new_ld
  e.new_ld = append(e.new_ld, ismessage.LogDependencies)
  printDeps(ismessage.LogDependencies, "  Step4. Received this ld")
  if (len(e.new_ld) < len(e.bd)) {
    // We need to await the other dependency versions before we can detect conflicts
    // Not safe to do it early, since it might not be a holistic ordering
    log.Printf("     Step4. Received ld for commandId %d, buttt we're still not ready. We have %d acks out of %d", e.lb.clientProposals[0].CommandId, len(e.new_ld), len(e.bd))
    return
  }

  if (e.lb.acceptOKs+1 <= r.N>>1) {
    // Don't detect conflicts yet, this logic will happen inside of handleAcceptReply
    log.Printf("     Step4. We got all our intershard dependencies for command %d, but the log entry hasn't been replicated at a majority, NOT detecting confs", e.lb.clientProposals[0].CommandId)
    return
  }
  log.Printf("Calling detectConflict from handleISReply for commandId %d...", e.lb.clientProposals[0].CommandId)
  r.detectConflicts(ismessage.AskerInstance)
}

func (r *Replica) detectConflicts(my_index int32) {
  //TODO - if i'm a read and i'm being swapped with a read with no writes between us
  //TODO - if i'm a write being swapped, but my dependencies came from a read behind only reads
  e := r.instanceSpace[my_index]
  // Detect conflicts
  my_k := e.cmds[0].K //TODO remove when batching enabled
  my_ver := e.version
  my_pid := e.pid
  new_index := int32(-1)
  var iindex int32
  for _, deps := range e.new_ld {
    for _, d := range deps {
      if (r.resolveShardFromKey(d.K) != int64(r.shardId)) {
        continue
      }
      log.Printf("     DC: Shard %d looking for commandID %d", r.shardId, d.CommandId)
      e, _, iindex = r.findEntry(d.CommandId)
      if (e == nil) {
        log.Printf("      DC: This command hasn't arrived at us yet! so we should detect conflicts later...")

        panic("not sure what happened.... this shouldn't happen because if we got a response it's because the InterShardReply at the other shard happened because we gave the version to them")
      }
      log.Printf("      DC: d.K(%d) == my_k(%d) && d.ver(%d) >= my_ver(%d) && d.PID(%d) > my_pid(%d) && d.index(%d) > my_index(%d)",
            d.K, my_k, e.version, my_ver, d.PID, my_pid, iindex, my_index)
      if d.K == my_k && e.version >= my_ver && d.PID > my_pid && iindex > my_index {
        // conflict detected
        log.Println("     DC: !!!!======***=======!!!!CONF DETECTED")
        log.Printf("      DC: d.K(%d) == my_k(%d) && d.ver(%d) >= my_ver(%d) && d.PID(%d) > my_pid(%d) && d.index(%d) > my_index(%d)",
                      d.K, my_k, e.version, my_ver, d.PID, my_pid, iindex, my_index)
        if (new_index == -1 || iindex > new_index) {
          new_index = iindex
        }
      }
    }
  }
  if (new_index == -1) {
    // No conflicts found!
    log.Println("     !!!!!!!!!!!!NO CONFLICTS FOUND!!")
    r.readyToCommit(my_index)
  } else if new_index >= r.crtInstance {
      panic("somehow we have a dependency that hasn't arrived yet... thsi shouldn't happen")
  } else {
    log.Printf("     Trying to swap me %d with %d, my_old_index == %d my_new_index == %d",
        r.instanceSpace[my_index].lb.clientProposals[0].CommandId, r.instanceSpace[new_index].lb.clientProposals[0].CommandId, my_index, new_index)
    r.num_conflicts++
    r.bcastReorder(my_index, new_index)
  }
}

func (r *Replica) readyToCommit(instance int32) {
  inst := r.instanceSpace[instance]
  inst.status = COMMITTED
	if inst.lb.clientProposals != nil && state.AllBlindWrites(inst.cmds) {
		// give client the all clear
		for i := 0; i < len(inst.cmds); i++ {
			propreply := &mdlinproto.ProposeReply{
				TRUE,
				inst.lb.clientProposals[i].CommandId,
				state.NIL,
				inst.lb.clientProposals[i].Timestamp,
        r.num_conflicts}
			log.Println("Responding to client with OK = true (1) in handleAcceptReply")
			r.MDReplyPropose(propreply, inst.lb.clientProposals[i].Reply)
		}
	}

	r.recordInstanceMetadata(inst)
	r.sync() //is this necessary?

	r.updateCommittedUpTo()

	r.bcastCommit(instance, inst.ballot, inst.cmds, inst.pid, inst.seqno, COMMITTED)
}

func (r *Replica) printLog() {
  for i := 0; int32(i) < r.crtInstance; i++ {
    e := r.instanceSpace[i]
    log.Printf("Log_entry@index = %d has status %d, and commands...", i, e.status)
    for _, c := range e.cmds {
      log.Println(commandToStr(c))
    }
  }
}

func commandToStr(c state.Command) string {
  var s string
  if (c.Op == state.GET) {
    s = fmt.Sprintf("R(%d)", c.K)
  } else {
    s = fmt.Sprintf("W(%d) = %v", c.K, c.V)
  }
  return s
}

func (r *Replica) reorderInLog(oldInstance int32, newInstance int32) {
  log.Println("I think this function reorderInLog line 1007 is buggy")
  newE := r.instanceSpace[newInstance]
  k := r.instanceSpace[oldInstance].cmds[0].K
  mypid := r.instanceSpace[oldInstance].pid
  for i := oldInstance; i < r.crtInstance; i++ {
    e := r.instanceSpace[i]
    for j, c := range e.cmds {
      //FIXME
      if c.K == k && e.pid == mypid {
        e.status = REORDERING
        newE.cmds = append(newE.cmds, c)
        if r.IsLeader {
          newE.lb.clientProposals = append(newE.lb.clientProposals, e.lb.clientProposals[j])
        }
      }
    }
  }
  log.Println("After the reordering the log looks like...")
  r.printLog()
}

var pr mdlinproto.Reorder
func (r *Replica) bcastReorder(oldInstance int32, newInstance int32) {
  log.Println("BroadCasting Reorder...")
	defer func() {
		if err := recover(); err != nil {
			log.Println("Reorder bcast failed:", err)
		}
	}()
  // Actually reorder the entry, by adding it to the commands of whatever 
  // it's supposed to be ordered after
  r.reorderInLog(oldInstance, newInstance)
	pr.LeaderId = r.Id
	pr.OldInstance = oldInstance
	pr.NewInstance = newInstance
  args := &pr

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
		r.SendMsg(q, r.reorderRPC, args)
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
				accept.SeqNos,
        accept.Versions,
        accept.BatchDeps,
        nil,
        nil}
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
			InstanceStatus(commit.Status),
			nil,
			commit.PIDs,
			commit.SeqNos,
      commit.Versions,
      commit.BatchDeps,
      nil,
      nil}
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
		r.instanceSpace[commit.Instance] = &Instance{nil,
			commit.Ballot,
			InstanceStatus(commit.Status),
			nil,
			-1,
			-1,
      -1,
      nil,
      nil,
      nil}
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
	inst := r.instanceSpace[preply.Instance]

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
			expectedSeqs := make(map[int64]int64)
			copyMap(expectedSeqs, r.nextSeqNo)
			r.bcastAccept(preply.Instance, inst.ballot, inst.cmds, inst.pid, inst.seqno, expectedSeqs, inst.bd)
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
    // The status is COMMITTED or REORDERING
		// we've move on, these are delayed replies, so just ignore
    log.Printf("hello? %d", inst.lb.clientProposals[0].CommandId)
		return
	}

	if areply.OK == TRUE {
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {
      log.Printf("Quorum! for commandId %d", inst.lb.clientProposals[0].CommandId)
      // Check if reordering happened yet!
      if len(inst.new_ld) >= len(inst.bd) {
        log.Printf("Calling detectConflict from handleAcceptReply for commandId %d...", inst.lb.clientProposals[0].CommandId)
        r.detectConflicts(areply.Instance)
      }
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

func (r *Replica) handleReorder(reorder *mdlinproto.Reorder) {
  var rreply *mdlinproto.ReorderReply
  log.Printf("Replica received reorder message, reordering! from %d to %d", reorder.OldInstance, reorder.NewInstance)
  r.reorderInLog(reorder.OldInstance, reorder.NewInstance)

  rreply = &mdlinproto.ReorderReply{reorder.OldInstance, 1}
  r.replyReorder(reorder.LeaderId, rreply)
}

func (r *Replica) handleReorderReply(rreply *mdlinproto.ReorderReply) {
  inst := r.instanceSpace[rreply.OldInstance]
  if (inst.status != REORDERING) {
    return
  }
  if rreply.OK == TRUE {
    inst.lb.reorderOKs++
    if inst.lb.reorderOKs+1 > r.N>>1 {
      log.Println("Reordered at quorum, COMMITTING")
      inst.status = REORDERED
      r.recordInstanceMetadata(inst)
	    r.sync()

	    r.updateCommittedUpTo()

	    r.bcastCommit(rreply.OldInstance, inst.ballot, inst.cmds, inst.pid, inst.seqno, REORDERED)
    }
  } else {
    panic("why was the reorder RPC response OK==false?")
  }
}

func (r *Replica) executeCommands() {
	i := int32(0)
	for !r.Shutdown {
		executed := false

		for i <= r.committedUpTo {
			if r.instanceSpace[i].cmds != nil {
				inst := r.instanceSpace[i]
        if (inst.status == REORDERED) {
          // We reordered these in the log, so when they're marked like this
          // we shouldn't execute them!
          log.Printf("****NOT executing log index %d cuz it's REORDERED", i)
        } else {
          log.Println("Number of commands in this entry is", len(inst.cmds))
				  for j := 0; j < len(inst.cmds); j++ {
					  // If an instands has multiple commands (a batch)
					  // they will get executed in consecutive order.
					  // This is good because we assume this to provide MD-lin
					  val := inst.cmds[j].Execute(r.State)
					  if inst.lb != nil && !state.AllBlindWrites(inst.cmds) {
						  propreply := &mdlinproto.ProposeReply{
							  TRUE,
							  inst.lb.clientProposals[j].CommandId,
							  val,
							  17,
                r.num_conflicts}
						  log.Printf("Responding to client with OK = true (1) in executeCommand, we executed command %d", inst.lb.clientProposals[j].CommandId)
						  log.Printf("It has OK = TRUE, CommandID = %d, val = %v, Timestamp = %v", inst.lb.clientProposals[j].CommandId, val, 17)
						  r.MDReplyPropose(propreply, inst.lb.clientProposals[j].Reply)
            }
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
