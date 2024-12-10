package gryff

import (
  "dlog"
  "fastrpc"
  "state"
  "time"
  "gryffcommon"
  "gryffproto"
)

type RMWState int
const (
	PREPARE RMWState = iota
  PROPOSE
  COMMIT
  FINISHED
)

type RMWInfo struct {
  coordinatorRequestId int64
  clientRequestId int64
  requestId int32
  clientId int32
  key state.Key
  oldValue state.Value
  newValue state.Value
  dep *gryffproto.Dep
  preparedBallot int32
  numPromisesReceived int
  numGoodPromisesReceived int
  numAcceptsReceived int
  numGoodAcceptsReceived int
  numCommitRepliesReceived int
  maxValTag *gryffproto.ValTag
  proposedOwn bool
  proposedProp gryffproto.Proposal
  acceptedProposal gryffproto.Proposal
  state RMWState
  maxUpdateId int64
  readValTags []gryffcommon.ReadValTag
  restarting bool
  retries int32
  prevAcceptedBallot int32
}

func newRMW(coordinatorRequestId int64, clientRequestId int64, requestId int32, clientId int32, key state.Key, oldValue state.Value,
    newValue state.Value, dep *gryffproto.Dep, retries int32,
    prevAcceptedBallot int32) *RMWInfo {
  rmw := &RMWInfo{
    coordinatorRequestId,
    clientRequestId,
    requestId,
    clientId,
    key,
    oldValue,
    newValue,
    dep,
    0,
    0,
    0,
    0,
    0,
    0,
    &gryffproto.ValTag{0, gryffproto.Tag{-1, 0, 0}},
    false,
    gryffproto.Proposal{0, 0, 0, 0, 0, gryffproto.Tag{0, 0, 0}, 0},
    gryffproto.Proposal{0, 0, 0, 0, 0, gryffproto.Tag{0, 0, 0}, 0},
    PREPARE,
    -1,                                 // maxUpdateId
    make([]gryffcommon.ReadValTag, 0),  // readValTags
    false,                              // restarting
    retries,
    prevAcceptedBallot,
  }
  return rmw
}

type SDPRMWHandler struct {
  r *Replica
  prepareChan  chan fastrpc.Serializable
  promiseChan  chan fastrpc.Serializable
  proposeChan  chan fastrpc.Serializable
  acceptChan   chan fastrpc.Serializable
  commitChan   chan fastrpc.Serializable
  commitReplyChan chan fastrpc.Serializable
  prepareRPC uint8
  promiseRPC uint8
  proposeRPC uint8
  acceptRPC  uint8
  commitRPC  uint8
  commitReplyRPC uint8
  ballotPromised map[state.Key]int32
  acceptedProposal map[state.Key]gryffproto.Proposal
  currRMW map[state.Key]*RMWInfo
  rmwQueue map[state.Key]chan *gryffproto.RMW
  finishedRMWChan chan state.Key
  highestSeenBallot int32
  preemptedSleep int
  retryRMWTimerChan chan bool
  retryRMWChan chan *RMWInfo
  rmwCounter int64
  committedRMWs map[int64]gryffproto.Proposal
}

func NewSDPRMWHandler(r *Replica) *SDPRMWHandler {
  s := &SDPRMWHandler{
    r,
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE),
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE),
    0,
    0,
    0,
    0,
    0,
    0,
    make(map[state.Key]int32),
    make(map[state.Key]gryffproto.Proposal),
    make(map[state.Key]*RMWInfo),
    make(map[state.Key]chan *gryffproto.RMW),
    make(chan state.Key, CHAN_BUFFER_SIZE),
    -1,
    5,                                                 // preemptedSleep
    make(chan bool, CHAN_BUFFER_SIZE),
    make(chan *RMWInfo, CHAN_BUFFER_SIZE),
    0,                                                  // rmwCounter
    make(map[int64]gryffproto.Proposal),                // committedRMWs
  }
  s.prepareRPC = r.RegisterRPC(new(gryffproto.Prepare), s.prepareChan)
  s.promiseRPC = r.RegisterRPC(new(gryffproto.Promise), s.promiseChan)
  s.proposeRPC = r.RegisterRPC(new(gryffproto.Propose), s.proposeChan)
  s.acceptRPC  = r.RegisterRPC(new(gryffproto.Accept),  s.acceptChan)
  s.commitRPC  = r.RegisterRPC(new(gryffproto.Commit),  s.commitChan)
  s.commitReplyRPC = r.RegisterRPC(new(gryffproto.CommitReply),
    s.commitReplyChan)

  return s
}

func (s *SDPRMWHandler) Loop(slowClockChan chan bool) {
  if s.r.proxy {
    select {
      case <-slowClockChan:
        if s.r.Beacon {
          for q := int32(0); q < int32(s.r.N); q++ {
            if q == s.r.Id {
              continue
            }
            s.r.SendBeacon(q)
          }
        }
        break
      case beacon := <-s.r.BeaconChan:
        dlog.Printf("Received Beacon from replica %d with timestamp %d\n",
            beacon.Rid, beacon.Timestamp)
        s.r.ReplyBeacon(beacon)
        break
      case <-s.r.DoneAdaptingChan:
        for i := 0; i < s.r.N - 1; i++ { 
          s.r.myPreferredPeerOrder[i + 1] = s.r.PreferredPeerOrder[i]
        }
        break
      case readS := <-s.r.readChan:
        read := readS.Obj.(*gryffproto.Read)
        // got Read message
        s.r.handleRead(read)
        break
      case writeS := <-s.r.writeChan:
        write := writeS.Obj.(*gryffproto.Write)
        // got Write message
        s.r.handleWrite(write)
        break
      case rmwS := <-s.r.rmwChan:
        rmw := rmwS.Obj.(*gryffproto.RMW)
        s.HandleRMW(rmw)
        break
      case write1ProxiedS := <-s.r.write1ChanProxied:
        write1Proxied := write1ProxiedS.(*gryffproto.Write1Proxied)
        //got a Write  message
        s.r.handleWrite1Proxied(write1Proxied)
        break
      case write2ProxiedS := <-s.r.write2ChanProxied:
        write2Proxied := write2ProxiedS.(*gryffproto.Write2Proxied)
        //got a Write  message
        s.r.handleWrite2Proxied(write2Proxied)
        break
      case read1ProxiedS := <-s.r.read1ChanProxied:
        read1Proxied := read1ProxiedS.(*gryffproto.Read1Proxied)
        //got a Read  message
        s.r.handleRead1Proxied(read1Proxied)
        break
      case read2ProxiedS := <-s.r.read2ChanProxied:
        read2Proxied := read2ProxiedS.(*gryffproto.Read2Proxied)
        //got a Read  message
        s.r.handleRead2Proxied(read2Proxied)
        break
      case write1ReplyS := <-s.r.write1ReplyChan:
        write1Reply := write1ReplyS.(*gryffproto.Write1Reply)
        //got a Write  message
        s.r.handleWrite1Reply(write1Reply)
        break
      case write2ReplyS := <-s.r.write2ReplyChan:
        write2Reply := write2ReplyS.(*gryffproto.Write2Reply)
        //got a Write  message
        s.r.handleWrite2Reply(write2Reply)
        break
      case read1ReplyS := <-s.r.read1ReplyChan:
        read1Reply := read1ReplyS.(*gryffproto.Read1Reply)
        //got a Read  message
        s.r.handleRead1Reply(read1Reply)
        break
      case read2ReplyS := <-s.r.read2ReplyChan:
        read2Reply := read2ReplyS.(*gryffproto.Read2Reply)
        //got a Read  message
        s.r.handleRead2Reply(read2Reply)
        break
      case prepareS := <-s.prepareChan:
        prepare := prepareS.(*gryffproto.Prepare)
        s.handlePrepare(prepare)
        break
      case promiseS := <-s.promiseChan:
        dlog.Printf("handling promise\n")
        promise := promiseS.(*gryffproto.Promise)
        s.handlePromise(promise)
        break
      case proposeS := <-s.proposeChan:
        propose := proposeS.(*gryffproto.Propose)
        s.handlePropose(propose)
        break
      case acceptS  :=  <-s.acceptChan:
        accept := acceptS.(*gryffproto.Accept)
        s.handleAccept(accept)
        break
      case commitS  :=  <-s.commitChan:
        commit := commitS.(*gryffproto.Commit)
        s.handleCommit(commit)
        break
      case commitReplyS := <-s.commitReplyChan:
        commitReply := commitReplyS.(*gryffproto.CommitReply)
        s.handleCommitReply(commitReply)
        break
      case key := <-s.finishedRMWChan:
        select {
          case queuedRMW := <-s.rmwQueue[key]:
            s.HandleRMW(queuedRMW)
            break
          default:
            break
        }
        break
      case <-s.retryRMWTimerChan:
        currRMW := <-s.retryRMWChan
        s.startRMW(currRMW.requestId, currRMW.clientId, currRMW.key,
            currRMW.oldValue, currRMW.newValue, currRMW.dep, currRMW.retries + 1, currRMW.prevAcceptedBallot)
        break

    }
  } else {
    select {
      case rmwS := <-s.r.rmwChan:
        rmw := rmwS.Obj.(*gryffproto.RMW)
        s.HandleRMW(rmw)
        break
      case read1S := <-s.r.read1Chan:
        read1 := read1S.Obj.(*gryffproto.Read1)
        //got a Read message
        s.r.handleRead1(read1, read1S.Reply)
        break
      case read2S := <-s.r.read2Chan:
        read2 := read2S.Obj.(*gryffproto.Read2)
        //got a Read message
        s.r.handleRead2(read2, read2S.Reply)
        break
      case write1S := <-s.r.write1Chan:
        write1 := write1S.Obj.(*gryffproto.Write1)
        //got a Write  message
        s.r.handleWrite1(write1, write1S.Reply)
        break
      case write2S := <-s.r.write2Chan:
        write2 := write2S.Obj.(*gryffproto.Write2)
        //got a Write  message
        s.r.handleWrite2(write2, write2S.Reply)
        break
      case prepareS := <-s.prepareChan:
        prepare := prepareS.(*gryffproto.Prepare)
        s.handlePrepare(prepare)
        break
      case promiseS := <-s.promiseChan:
        promise := promiseS.(*gryffproto.Promise)
        s.handlePromise(promise)
        break
      case proposeS := <-s.proposeChan:
        propose := proposeS.(*gryffproto.Propose)
        s.handlePropose(propose)
        break
      case acceptS  :=  <-s.acceptChan:
        accept := acceptS.(*gryffproto.Accept)
        s.handleAccept(accept)
        break
      case commitS  :=  <-s.commitChan:
        commit := commitS.(*gryffproto.Commit)
        s.handleCommit(commit)
        break
      case commitReplyS := <-s.commitReplyChan:
        commitReply := commitReplyS.(*gryffproto.CommitReply)
        s.handleCommitReply(commitReply)
        break
      case key := <-s.finishedRMWChan:
        select {
          case queuedRMW := <-s.rmwQueue[key]:
            s.HandleRMW(queuedRMW)
            break
          default:
            break
        }
        break
      case <-s.retryRMWTimerChan:
        currRMW := <-s.retryRMWChan
        s.startRMW(currRMW.requestId, currRMW.clientId, currRMW.key,
            currRMW.oldValue, currRMW.newValue, currRMW.dep, currRMW.retries + 1, currRMW.prevAcceptedBallot)
        break
    }
  }
}

func (s *SDPRMWHandler) HandleRMW(rmw *gryffproto.RMW) {
  currRMW, ok := s.currRMW[rmw.K]
  if !ok || currRMW == nil {
    s.r.Printf("Starting RMW(%d,%d).\n", rmw.RequestId, rmw.ClientId)
    s.startRMW(rmw.RequestId, rmw.ClientId, rmw.K, rmw.OldValue, rmw.NewValue,
        &rmw.D, 0, 0)
  } else {
    s.r.Printf("Queueing RMW(%d,%d).\n", rmw.RequestId, rmw.ClientId)
    // TODO: can we batch RMWs if we haven't yet proposed the
    //   current RMW?
    // save the request for after we finish the current RMW
    queue, ook := s.rmwQueue[rmw.K]
    if !ook {
      queue = make(chan *gryffproto.RMW, CHAN_BUFFER_SIZE)
      s.rmwQueue[rmw.K] = queue
    }
    queue <- rmw
    s.r.Stats.Max("rmw_queue_length", len(queue))
    s.r.Printf("RMW queue length: %d.\n", len(queue))
  }
}

func (s *SDPRMWHandler) startRMW(rid int32, cid int32, k state.Key,
    oldVal state.Value, newVal state.Value, dep *gryffproto.Dep,
    retries int32, prevAcceptedBallot int32) {
  coordinatorRequestId := int64(s.rmwCounter << 4) | int64(s.r.Id & 0xF)
  s.rmwCounter++
  clientRequestId := (int64(rid) << 32) | int64(cid)
  s.r.Printf("[RMW][rid=%d][cid=%d] starting (%d,%d,%d)\n",
    rid, cid, k, oldVal, newVal)
  currRMW := newRMW(coordinatorRequestId, clientRequestId, rid, cid, k,
      oldVal, newVal, dep, retries, prevAcceptedBallot)
  s.currRMW[k] = currRMW
  currRMW.preparedBallot = s.nextBallot() 
  prepare := &gryffproto.Prepare{currRMW.clientRequestId,
      currRMW.coordinatorRequestId, s.r.Id, currRMW.key, *currRMW.dep,
      currRMW.preparedBallot}
  s.r.Printf("Sending Prepare to all.\n")
  s.sendPrepareToAll(prepare)
}

func (s *SDPRMWHandler) nextBallot() int32 {
  s.r.Printf("Getting next ballot with hsb=%d.\n", s.highestSeenBallot)
  b := (s.highestSeenBallot + 1) << 4 | s.r.Id
  s.highestSeenBallot++
  return b
}

func (s *SDPRMWHandler) sendPrepareToAll(prepare *gryffproto.Prepare) {
  s.prepareChan <- prepare
  for i := 1; i < s.r.N; i++ {
    q := (s.r.Id + int32(i)) % int32(s.r.N)
		if !s.r.Alive[q] {
			continue
		}
    s.r.Printf("Sending Prepare(%d,%d) to %d.\n", prepare.ClientRequestId >> 32,
        prepare.ClientRequestId & 0xFFFFFFFF, q)
		s.r.SendMsg(q, s.prepareRPC, prepare)
	}
}

func (s *SDPRMWHandler) handlePrepare(prepare *gryffproto.Prepare) {
  s.r.Printf("Prepare(%d,%d) ballot=%+v.\n",
      prepare.ClientRequestId >> 32, prepare.ClientRequestId & 0xFFFFFFFF,
      prepare.Ballot)

  var promise *gryffproto.Promise
  prop, ok := s.committedRMWs[prepare.ClientRequestId]
  if ok {
    promise = &gryffproto.Promise{
      prepare.ClientRequestId,
      prepare.CoordinatorRequestId,
      s.r.Id,
      prepare.K,
      -1,
      prop,
      prop.OldValue,
      prop.T,
      TRUE,
    }
  } else {
    if prepare.D.Key >= 0 { // dep.Key < 0 ==> no dep
      s.r.HandleOverwrite(prepare.D.Key, prepare.D.Vt.V, &prepare.D.Vt.T)
    }

    ballotPromised, okB := s.ballotPromised[prepare.K]
    if !okB || ballotPromised < prepare.Ballot {
      s.r.Printf("Prepare(%d,%d) promising %+v.\n",
          prepare.ClientRequestId >> 32, prepare.ClientRequestId & 0xFFFFFFFF,
          prepare.Ballot)
      s.ballotPromised[prepare.K] = prepare.Ballot
    } else if okB {
      s.r.Printf("Prepare(%d,%d) already promised %+v.\n",
          prepare.ClientRequestId >> 32, prepare.ClientRequestId & 0xFFFFFFFF,
          ballotPromised)
    }

    s.r.Printf("Responding to Prepare(%d,%d) with accepted proposal %v.\n",
        prepare.ClientRequestId >> 32, prepare.ClientRequestId & 0xFFFFFFFF,
        s.acceptedProposal[prepare.K])

    meta := s.r.GetStoreMetadata(prepare.K)
    promise = &gryffproto.Promise{
      prepare.ClientRequestId,
      prepare.CoordinatorRequestId,
      s.r.Id,
      prepare.K,
      s.ballotPromised[prepare.K],
      s.acceptedProposal[prepare.K],
      s.r.GetCurrentValue(prepare.K),
      *meta.Tag,
      FALSE,
    }
  }
  s.replyPromise(prepare.CoordinatorId, promise)
}

func (s *SDPRMWHandler) replyPromise(coordinatorId int32,
    promise *gryffproto.Promise) {
  if coordinatorId == s.r.Id {
    s.promiseChan <- promise
  } else {
    s.r.SendMsg(coordinatorId, s.promiseRPC, promise)
  }
}

func (s *SDPRMWHandler) handlePromise(promise *gryffproto.Promise) {
  currRMW, ok := s.currRMW[promise.K]
  if !ok || currRMW == nil || currRMW.state != PREPARE || currRMW.coordinatorRequestId != promise.CoordinatorRequestId || currRMW.restarting {
    // TODO: or promise is from slow replica for already completed RMW
    return
  }

  if promise.RequestCommitted == TRUE {
    s.r.completeRMW(promise.AcceptedProposal.RequestId,
        promise.AcceptedProposal.ClientId, promise.AcceptedProposal.OldValue)
    s.currRMW[promise.K] = nil
    s.finishedRMWChan <- promise.K
    return
  }

  currRMW.numPromisesReceived++
  done := false
  if promise.BallotPromised == currRMW.preparedBallot {
    s.r.Printf("Promise(%d,%d) promise for %d from %d.\n",
        promise.ClientRequestId >> 32, promise.ClientRequestId & 0xFFFFFFFF,
        currRMW.preparedBallot, promise.ReplicaId)
    currRMW.numGoodPromisesReceived++
    
    if currRMW.acceptedProposal.Ballot < promise.AcceptedProposal.Ballot {
      s.r.Printf("Promise(%d,%d) contained accepted proposal %+v.\n",
          promise.ClientRequestId >> 32, promise.ClientRequestId & 0xFFFFFFFF,
          promise.AcceptedProposal)
      currRMW.acceptedProposal = promise.AcceptedProposal
    }


    readValTag := &gryffproto.ValTag{promise.V, promise.T}
    if promise.T.GreaterThan(currRMW.maxValTag.T) {
      currRMW.maxValTag = readValTag
    }
    if currRMW.numGoodPromisesReceived >= (s.r.N / 2) + 1 {
      s.r.Printf("Promise(%d,%d) received enough promises to propose %d.\n",
          promise.ClientRequestId >> 32, promise.ClientRequestId & 0xFFFFFFFF,
          currRMW.preparedBallot) 
      var propose *gryffproto.Propose

      done = true
      tag := gryffproto.Tag{currRMW.maxValTag.T.Ts, currRMW.maxValTag.T.Cid,
        currRMW.maxValTag.T.Rmwc + 1}
      s.r.Printf("Promise(%d,%d) deciding what to propose ballot=%d,retry=%d.\n",
          promise.ClientRequestId >> 32, promise.ClientRequestId & 0xFFFFFFFF,
          currRMW.acceptedProposal.Ballot, currRMW.retries)
      if currRMW.acceptedProposal.Ballot == 0 || (currRMW.prevAcceptedBallot == currRMW.acceptedProposal.Ballot && currRMW.retries > 0) {
        if currRMW.acceptedProposal.Ballot > 0 && (currRMW.prevAcceptedBallot == currRMW.acceptedProposal.Ballot && currRMW.retries > 0) {
          // already a higher accepted proposal for this tag
          propose = &gryffproto.Propose{
            currRMW.clientRequestId,
            currRMW.coordinatorRequestId,
            s.r.Id,
            currRMW.key,
            currRMW.preparedBallot,
            currRMW.acceptedProposal,
          }
          currRMW.proposedOwn = false
        } else if currRMW.acceptedProposal.Ballot == 0 {
          newValue, oldValue := s.determineNewValue(currRMW)
          propose = &gryffproto.Propose{
            currRMW.clientRequestId,
            currRMW.coordinatorRequestId,
            s.r.Id,
            currRMW.key,
            currRMW.preparedBallot,
            gryffproto.Proposal{
              currRMW.requestId,
              currRMW.clientId,
              currRMW.preparedBallot,
              currRMW.key,
              newValue,
              tag,
              oldValue,
            },
          }
          currRMW.proposedOwn = true
        }
        currRMW.proposedProp = propose.Prop
        s.sendProposeToAll(propose)
        currRMW.state = PROPOSE
      }
    }
    currRMW.prevAcceptedBallot = currRMW.acceptedProposal.Ballot
  } else {
    s.r.Stats.Increment("preempted_prepare")
    s.r.Printf("[Promise][rid=%d][cid=%d][k=%d] mine %d preempted by %d at %d.\n",
      currRMW.requestId, currRMW.clientId,
      currRMW.key, currRMW.preparedBallot, promise.BallotPromised,
      promise.ReplicaId)
    s.highestSeenBallot = Max(s.highestSeenBallot, promise.BallotPromised >> 4)
    done = true
  }
  if done && currRMW.state != PROPOSE {
    currRMW.restarting = true
    s.retryRMWChan <- currRMW
    go func() {
      s.randSleep(1)
      s.retryRMWTimerChan <- true
    }()
  }
}

// compare and swap for now
func (s *SDPRMWHandler) determineNewValue(rmw *RMWInfo) (state.Value, state.Value) {
  if rmw.maxValTag.V == rmw.oldValue {
    return rmw.newValue, rmw.maxValTag.V
  } else {
    return rmw.maxValTag.V, rmw.maxValTag.V
  }
}

func (s *SDPRMWHandler) sendProposeToAll(propose *gryffproto.Propose) {
  s.proposeChan <- propose
  for i := 1; i < s.r.N; i++ {
    q := (s.r.Id + int32(i)) % int32(s.r.N)
		if !s.r.Alive[q] {
			continue
		}
    s.r.Printf("Sending Propose(%d,%d) for prop (%d,%d) to %d.\n",
        propose.ClientRequestId >> 32,
        propose.ClientRequestId & 0xFFFFFFFF, propose.Prop.RequestId,
        propose.Prop.ClientId, q)
		s.r.SendMsg(q, s.proposeRPC, propose)
	}
}

func (s *SDPRMWHandler) handlePropose(propose *gryffproto.Propose) {
  s.r.Printf("Received Propose(%d,%d) with prop for (%d,%d).\n",
      propose.ClientRequestId >> 32, propose.ClientRequestId & 0xFFFFFFFF,
      propose.Prop.RequestId, propose.Prop.ClientId)
  ballotPromised, okB := s.ballotPromised[propose.K]
  if okB && ballotPromised <= propose.Ballot {
    s.r.Printf("[Propose][cid=%d][k=%d] accepting %v.\n",
      propose.CoordinatorId, propose.K, propose.Ballot)
    s.acceptedProposal[propose.K] = propose.Prop
    s.ballotPromised[propose.K] = propose.Ballot
  } else if okB {
    s.r.Printf("[Propose][cid=%d][k=%d] already promised %v.\n",
      propose.CoordinatorId, propose.K, ballotPromised)
  }

  accept := &gryffproto.Accept{propose.ClientRequestId,
      propose.CoordinatorRequestId, s.r.Id, propose.K, ballotPromised}
  s.r.Printf("Propose(%d,%d) replying accept=%+v to %d.\n",
      propose.ClientRequestId >> 32, propose.ClientRequestId & 0xFFFFFFFF,
      *accept, propose.CoordinatorId)
  s.replyAccept(propose.CoordinatorId, accept)
}

func (s *SDPRMWHandler) replyAccept(coordinatorId int32,
    accept *gryffproto.Accept) {
  if coordinatorId == s.r.Id {
    s.acceptChan <- accept 
  } else {
    s.r.SendMsg(coordinatorId, s.acceptRPC, accept)
  }
}

func (s *SDPRMWHandler) handleAccept(accept *gryffproto.Accept) {
  s.r.Printf("Received Accept(%d,%d) from %d.\n",
      accept.ClientRequestId >> 32, accept.ClientRequestId & 0xFFFFFFFF,
      accept.ReplicaId)

  currRMW, ok := s.currRMW[accept.K]
  if !ok || currRMW == nil || currRMW.state != PROPOSE || currRMW.coordinatorRequestId != accept.CoordinatorRequestId || currRMW.restarting { // TODO: or accept is from slow replica for already completed RMW
    if currRMW != nil {
      s.r.Printf("Ignored Accept(%d,%d) ok=%v, clientRequestId=%v, coordinatorRequestId=%v",
          accept.ClientRequestId >> 32, accept.ClientRequestId & 0xFFFFFFFF,
          ok, currRMW.clientRequestId, currRMW.coordinatorRequestId)
    } else {
      s.r.Printf("Ignored Accept(%d,%d) because currRMW=nil.\n",
          accept.ClientRequestId >> 32, accept.ClientRequestId & 0xFFFFFFFF)
    }
    return
  }

  s.r.Printf("Accept(%d,%d) from %d was accepted?=%v.\n",
          accept.ClientRequestId >> 32, accept.ClientRequestId & 0xFFFFFFFF,
          accept.ReplicaId, 
          accept.BallotPromised == currRMW.preparedBallot)

  currRMW.numAcceptsReceived++
  if accept.BallotPromised == currRMW.preparedBallot {
    s.r.Printf("[Accept][k=%d] accept for %d from %d.\n",
      currRMW.key, currRMW.preparedBallot, accept.ReplicaId)

    currRMW.numGoodAcceptsReceived++
    
    if currRMW.numGoodAcceptsReceived >= (s.r.N / 2) + 1 {
      s.r.Printf("[Accept][k=%d] received enough accepts to commit %d.\n",
        currRMW.key, currRMW.preparedBallot) 
      commit := &gryffproto.Commit{
        currRMW.clientRequestId,
        currRMW.coordinatorRequestId,
        s.r.Id,
        currRMW.proposedProp.K,
        gryffproto.ValTag{currRMW.proposedProp.NewValue, currRMW.proposedProp.T},
        currRMW.proposedProp,
      }
      s.sendCommitToAll(commit)
      currRMW.state = COMMIT
    }
  } else {
    s.r.Stats.Increment("preempted_propose")
    s.r.Printf("[Accept][rid=%d][cid=%d][k=%d] mine %d preempted by %d at %d.\n",
      currRMW.requestId, currRMW.clientId,
      currRMW.key, currRMW.preparedBallot, accept.BallotPromised,
      accept.ReplicaId)
    s.highestSeenBallot = Max(s.highestSeenBallot, accept.BallotPromised >> 4)

    currRMW.restarting = true
    s.retryRMWChan <- currRMW
    go func() {
      s.randSleep(2)
      s.retryRMWTimerChan <- true
    }()
  }
}

func (s *SDPRMWHandler) sendCommitToAll(commit *gryffproto.Commit) {
  s.commitChan <- commit
  for i := 1; i < s.r.N; i++ {
    q := (s.r.Id + int32(i)) % int32(s.r.N)
		if !s.r.Alive[q] {
			continue
		}
    s.r.Printf("Sending Commit(%d,%d) to %d.\n", commit.ClientRequestId >> 32,
        commit.ClientRequestId & 0xFFFFFFFF, q)
		s.r.SendMsg(q, s.commitRPC, commit)
	}
}

func (s *SDPRMWHandler) handleCommit(commit *gryffproto.Commit) {
  s.r.Printf("[Commit][cid=%d][k=%d] received=%+v.\n",
    commit.CoordinatorId, commit.K, *commit)
  s.committedRMWs[(int64(commit.Prop.RequestId) << 32) | int64(commit.Prop.ClientId)] = commit.Prop

  s.r.HandleOverwrite(commit.K, commit.Vt.V, &commit.Vt.T)
  acceptedProposal, ok := s.acceptedProposal[commit.K]
  s.r.Printf("Received Commit(%d,%d) with tag %v; current accepted proposal tag %v.\n",
      commit.ClientRequestId >> 32, commit.ClientRequestId & 0xFFFFFFFF, commit.Vt.T,
      acceptedProposal.T)
  if ok && !commit.Vt.T.LessThan(acceptedProposal.T) {
    s.r.Printf("Deleting accepted proposal for %d.\n", commit.K)
    delete(s.acceptedProposal, commit.K)
  }
  commitReply := &gryffproto.CommitReply{commit.ClientRequestId,
      commit.CoordinatorRequestId, s.r.Id, commit.K}
  s.replyCommitReply(commit.CoordinatorId, commitReply)
}

func (s *SDPRMWHandler) replyCommitReply(coordinatorId int32,
    commitReply *gryffproto.CommitReply) {
  if coordinatorId == s.r.Id {
    s.commitReplyChan <- commitReply
  } else {
    s.r.SendMsg(coordinatorId, s.commitReplyRPC, commitReply)
  }
}

func (s *SDPRMWHandler) handleCommitReply(commitReply *gryffproto.CommitReply) {
   s.r.Printf("Received CommitReply(%d,%d) from %d.\n",
      commitReply.ClientRequestId >> 32,
      commitReply.ClientRequestId & 0xFFFFFFFF,
      commitReply.ReplicaId)
  currRMW, ok := s.currRMW[commitReply.K]
  if !ok || currRMW == nil || currRMW.state != COMMIT || currRMW.coordinatorRequestId != commitReply.CoordinatorRequestId || currRMW.restarting { // TODO: or commit is from slow replica for already completed RMW
    s.r.Printf("Ignored CommitReply(%d,%d)", commitReply.ClientRequestId >> 32,
      commitReply.ClientRequestId & 0xFFFFFFFF)
    return
  }
  s.r.Printf("[Commit][k=%d] commit reply from %d.\n", commitReply.K,
    commitReply.ReplicaId)
  
  currRMW.numCommitRepliesReceived++
  if currRMW.numCommitRepliesReceived >= (s.r.N / 2) + 1 {
    s.r.completeRMW(currRMW.proposedProp.RequestId, currRMW.proposedProp.ClientId,
        currRMW.proposedProp.OldValue)
    if currRMW.proposedOwn {
      s.r.Printf("Finished RMW(%d,%d) for key %d.\n", currRMW.requestId,
          currRMW.clientId, commitReply.K)
      s.currRMW[commitReply.K] = nil
      // TODO: instead of doing queued RMWs, can we batch them all in this
      //   commit?
      s.finishedRMWChan <- currRMW.key
    } else {
      s.r.Printf("Restarting RMW(%d,%d) because proposed other (%d,%d).\n",
          currRMW.requestId, currRMW.clientId, currRMW.proposedProp.RequestId,
          currRMW.proposedProp.ClientId)
      // TODO: we committed some other RMW; need to try again for our own RMW
      s.startRMW(currRMW.requestId, currRMW.clientId, currRMW.key,
          currRMW.oldValue, currRMW.newValue, currRMW.dep, 0, 0)
    }
  }
}

func (s *SDPRMWHandler) randSleep(n int) {
  d :=  0//rand.Intn(20) + int(Max(0, int32(n * s.preemptedSleep - 10)))
  s.r.Stats.Add("rmw_sleep", d)
  time.Sleep(time.Duration(d) * time.Millisecond)
}
