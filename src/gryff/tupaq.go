package gryff

/*
#include <sys/times.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
*/
import "C"

import (
//  "log"
  "dlog"
  "genericsmr"
  "gryffproto"
  "gryffcommon"
  "bufio"
  "clientproto"
  "state"
  "fastrpc"
  "fmt"
  "time"
  "math/rand"
  "stats"
  "sync"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)
const MAX_BATCH = 5000

type RMWHandlerType int8
const (
  SDP RMWHandlerType = iota
  EPAXOS
)

func Max(a int32, b int32) int32 {
  if a > b {
    return a
  } else {
    return b
  }
}

type Replica struct {
  *genericsmr.Replica // extends a generic Paxos replica
  parti *gryffcommon.GryffParticipant
  coord *gryffcommon.GryffCoordinator
  read1Chan    chan *genericsmr.ClientRPC 
  read2Chan    chan *genericsmr.ClientRPC 
  write1Chan   chan *genericsmr.ClientRPC
  write2Chan   chan *genericsmr.ClientRPC
  rmwChan      chan *genericsmr.ClientRPC
  readChan chan *genericsmr.ClientRPC
  writeChan chan *genericsmr.ClientRPC
  read1ChanProxied chan fastrpc.Serializable
  read2ChanProxied chan fastrpc.Serializable
  write1ChanProxied chan fastrpc.Serializable
  write2ChanProxied chan fastrpc.Serializable
  write1ReplyChan chan fastrpc.Serializable
  write1TimeoutChan chan fastrpc.Serializable
  write2ConcurrentTimeoutChan chan fastrpc.Serializable
  write2ReplyChan chan fastrpc.Serializable
  read1ChanRepl chan fastrpc.Serializable
  read2ChanRepl chan fastrpc.Serializable
  read1ReplyChan chan fastrpc.Serializable
  read2ReplyChan chan fastrpc.Serializable
  forceWriteChan chan bool
  write1RPC uint8
  write2RPC uint8
  write1ReplyRPC uint8
  write2ReplyRPC uint8
  read1RPC uint8
  read2RPC uint8
  read1ReplyRPC uint8
  read2ReplyRPC uint8
  Shutdown     bool
  flush        bool
  maxSeenTag gryffproto.Tag
  read1 *gryffproto.Read1Proxied
  read2 *gryffproto.Read2Proxied
  write1 *gryffproto.Write1Proxied
  write2 *gryffproto.Write2Proxied
  proxy bool
  myPreferredPeerOrder []int32
  rmwHandler gryffcommon.IRMWHandler
  shortcircuitTime  int
  fastOverwriteTime int
  forceWritePeriod int
  mtx *sync.Mutex
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool,
    dreply bool, beacon bool, durable bool, statsFile string, regular bool,
    proxy bool, noConflicts bool, epaxosMode bool, rmwHandler RMWHandlerType,
    shortcircuitTime int, fastOverwriteTime int, forceWritePeriod int, broadcastOptimizationEnabled bool) *Replica {
  r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply,
      false, statsFile),
    nil,                                                // parti
    nil,                                                // coord
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE),
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE),
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE),
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE),
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE),
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE), // readChan
    make(chan *genericsmr.ClientRPC, CHAN_BUFFER_SIZE), // writeChan
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read1ChanProxied
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read2ChanProxied
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write1ChanProxied
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write2ChanProxied
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write1ReplyChan
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write1TimeoutChan
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write2ConcurrentTimeoutChan
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // write2ReplyChan
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read1ChanRepl
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read2ChanRepl
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read1ReplyChan
    make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE), // read2ReplyChan
    make(chan bool, 1), // forceWriteChan
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    false,
    true,
    gryffproto.Tag{0, 0, 0},                             // maxSeenTag
    new(gryffproto.Read1Proxied),
    new(gryffproto.Read2Proxied),
    new(gryffproto.Write1Proxied),
    new(gryffproto.Write2Proxied),
    proxy,
    nil,
    nil,
    shortcircuitTime,
    fastOverwriteTime,
    forceWritePeriod,
    new(sync.Mutex),
  }
  switch rmwHandler {
    case SDP:
      r.rmwHandler = NewSDPRMWHandler(r)
      break
    case EPAXOS:
      r.rmwHandler = NewEPaxosRMWHandler(r, noConflicts, broadcastOptimizationEnabled)
      break
  }

  rand.Seed(time.Now().UnixNano() << 4 | int64(id))
  r.Durable = durable
  r.Beacon = beacon

  r.myPreferredPeerOrder = make([]int32, r.N)
  r.myPreferredPeerOrder[0] = r.Id
  for i := 1; i < r.N; i++ {
		r.myPreferredPeerOrder[i] = int32((int(r.Id) + i) % r.N)
	}

  r.parti = gryffcommon.NewGryffParticipant(r.Id, r, noConflicts)
  if proxy {
    r.Printf("Registering listeners for proxied requests.\n")
    r.coord = gryffcommon.NewGryffCoordinator(r.Id, r, regular, r.N, proxy,
        thrifty, epaxosMode)
    r.RegisterClientRPC(new(gryffproto.Read), clientproto.Gryff_READ,
        r.readChan)
    r.RegisterClientRPC(new(gryffproto.Write), clientproto.Gryff_WRITE,
        r.writeChan)
    r.read1RPC = r.RegisterRPC(new(gryffproto.Read1Proxied),
        r.read1ChanProxied)
    r.read2RPC = r.RegisterRPC(new(gryffproto.Read2Proxied),
        r.read2ChanProxied)
    r.write1RPC = r.RegisterRPC(new(gryffproto.Write1Proxied),
        r.write1ChanProxied)
    r.write2RPC = r.RegisterRPC(new(gryffproto.Write2Proxied),
        r.write2ChanProxied)
    r.read1ReplyRPC = r.RegisterRPC(new(gryffproto.Read1Reply),
        r.read1ReplyChan)
    r.read2ReplyRPC = r.RegisterRPC(new(gryffproto.Read2Reply),
        r.read2ReplyChan)
    r.write1ReplyRPC = r.RegisterRPC(new(gryffproto.Write1Reply),
        r.write1ReplyChan)
    r.write2ReplyRPC = r.RegisterRPC(new(gryffproto.Write2Reply),
        r.write2ReplyChan)
  } else {
    r.RegisterClientRPC(new(gryffproto.Read1), clientproto.Gryff_READ_1,
        r.read1Chan)
    r.RegisterClientRPC(new(gryffproto.Read2), clientproto.Gryff_READ_2,
        r.read2Chan)
    r.RegisterClientRPC(new(gryffproto.Write1), clientproto.Gryff_WRITE_1,
        r.write1Chan)
    r.RegisterClientRPC(new(gryffproto.Write2), clientproto.Gryff_WRITE_2,
        r.write2Chan)
  }
  r.RegisterClientRPC(new(gryffproto.RMW), clientproto.Gryff_RMW, r.rmwChan)

  go r.run()

  return r
}

/**
 * gryffcommon.IGryffParticipant methods
 */
func (r *Replica) Printf(format string, v ...interface{}) {
  if dlog.DLOG {
    dlog.Printf("[%d] %s", r.Id, fmt.Sprintf(format, v...))
  }
}

func (r *Replica) GetCurrentValue(key state.Key) state.Value {
  if r.Exec {
    return r.State.Store[key]
  } else {
    return 0
  }
}

func (r *Replica) SetCurrentValue(key state.Key, val state.Value) {
  if r.Exec {
    r.State.Store[key] = val
  }
}

func (r *Replica) TagSeen(tag *gryffproto.Tag) {
  if tag.GreaterThan(r.maxSeenTag) {
    r.maxSeenTag = *tag
  }
}
/**
 * End gryffcommon.IGryffParticipant methds
 */

/**
 * gryffcommon.IGryffCoordinator methods
 */
func (r *Replica) SendRead1(rop *gryffcommon.ReadOp, replicas []int32) {
  r.read1.RequestId = rop.RequestId
  r.read1.ClientId = rop.ClientId
  r.read1.K = state.Key(rop.Key)
  r.read1.ReplicaId = r.Id

  meta := r.parti.GetStoreMetadata(r.read1.K)
  r.read1.Vt = gryffproto.ValTag{r.GetCurrentValue(r.read1.K), *meta.Tag}

  r.Printf("Sending Read1=%+v.\n", *r.read1)
  
  for i := 0; i < len(replicas); i++ {
    r.sendRead1To(replicas[i])
  }
}

func (r *Replica) SendRead2(rop *gryffcommon.ReadOp, replicas []int32) {
  r.read2.RequestId = rop.RequestId
  r.read2.ClientId = rop.ClientId
  r.read2.K = state.Key(rop.Key)
  r.read2.Vt = *rop.ReadValTag
  r.read2.ReplicaId = r.Id
  r.read2.UpdateId = rop.ReadUpdateId

  for i := 0; i < len(replicas); i++ {
    r.sendRead2To(replicas[i])
  }
}

func (r *Replica) SendWrite1(wop *gryffcommon.WriteOp, replicas []int32) {
  r.write1.RequestId = wop.RequestId
  r.write1.ClientId = wop.ClientId
  r.write1.N = wop.N
  r.write1.ForceWrite = wop.ForceWrite
  r.write1.K = state.Key(wop.Key)
  r.write1.V = state.Value(wop.WriteValue)
  r.write1.D = *wop.Dep
  r.write1.ReplicaId = r.Id
  r.write1.MaxAppliedTag = *wop.MaxAppliedTag

  for i := 0; i < len(replicas); i++ {
    r.sendWrite1To(replicas[i])
  }
}

func (r *Replica) SendWrite2(wop *gryffcommon.WriteOp, replicas []int32) {
  r.write2.RequestId = wop.RequestId
  r.write2.ClientId = wop.ClientId
  r.write2.N = wop.N
  r.write2.K = state.Key(wop.Key)
  r.write2.Vt = gryffproto.ValTag{state.Value(wop.WriteValue), *wop.WriteTag}
  r.write2.ReplicaId = r.Id

  for i := 0; i < len(replicas); i++ {
    r.sendWrite2To(replicas[i])
  }
}

func (r *Replica) CompleteRead(requestId int32, clientId int32,
    success bool, readValue int64) {
  var ok uint8
  if success {
    ok = 1
  } else {
    ok = 0
  }
  readReply := &gryffproto.ReadReply{requestId, clientId,
      state.Value(readValue), r.maxSeenTag, ok}
  r.Printf("Replying to client for Read[%d,%d].\n", requestId, clientId)
  client := r.GetClientWriter(clientId)

  client.WriteByte(clientproto.Gryff_READ_REPLY)
  readReply.Marshal(client)
  client.Flush()
}

func (r *Replica) CompleteWrite(requestId int32, clientId int32,
    success bool) {
  var ok uint8
  if success {
    ok = 1
  } else {
    ok = 0
  }
  r.Printf("Replying to client for Write[%d,%d].\n", requestId, clientId)
  writeReply := &gryffproto.WriteReply{requestId, clientId, r.maxSeenTag, ok}
  client := r.GetClientWriter(clientId)

  client.WriteByte(clientproto.Gryff_WRITE_REPLY)
  writeReply.Marshal(client)
  client.Flush()
}

func (r *Replica) GetStats() *stats.StatsMap {
  return r.Stats
}

func (r *Replica) GetReplicasByRank() []int32 {
  if r.Beacon {
  }
  return r.myPreferredPeerOrder
}

func (r *Replica) HandleOverwrite(key state.Key, newValue state.Value,
    newTag *gryffproto.Tag) {
  r.parti.HandleOverwrite(key, newValue, newTag)
}

func (r *Replica) GetStoreMetadata(key state.Key) *gryffcommon.StoreMetadata {
  return r.parti.GetStoreMetadata(key)
}
/**
 * End gryffcommon.IGryffCoordinator methods
 */

//sync with the stable store
func (r *Replica) sync() {
  if !r.Durable {
    return
  }

  r.StableStore.Sync()
}

func (r *Replica) replyRead1(w *bufio.Writer, reply *gryffproto.Read1Reply) {
  w.WriteByte(clientproto.Gryff_READ_1_REPLY)
  reply.Marshal(w)
  w.Flush()
}

func (r *Replica) replyRead2(w *bufio.Writer, reply *gryffproto.Read2Reply) {
  w.WriteByte(clientproto.Gryff_READ_2_REPLY)
  reply.Marshal(w)
  w.Flush()
}

func (r *Replica) replyWrite1(w *bufio.Writer, reply *gryffproto.Write1Reply) {
  w.WriteByte(clientproto.Gryff_WRITE_1_REPLY)
  reply.Marshal(w)
  w.Flush()
}

func (r *Replica) replyWrite2(w *bufio.Writer, reply *gryffproto.Write2Reply) {
  w.WriteByte(clientproto.Gryff_WRITE_2_REPLY)
  reply.Marshal(w)
  w.Flush()
}

func (r *Replica) replyWrite1Replica(coordinatorId int32,
    write1Reply *gryffproto.Write1Reply) {
  if coordinatorId == r.Id {
    r.write1ReplyChan <- write1Reply
  } else {
    r.SendMsg(coordinatorId, r.write1ReplyRPC, write1Reply)
  }
}

func (r *Replica) replyWrite2Replica(coordinatorId int32,
    write2Reply *gryffproto.Write2Reply) {
  if coordinatorId == r.Id {
    r.write2ReplyChan <- write2Reply
  } else {
    r.SendMsg(coordinatorId, r.write2ReplyRPC, write2Reply)
  }
}

func (r *Replica) replyRead1Replica(coordinatorId int32,
    read1Reply *gryffproto.Read1Reply) {
  if coordinatorId == r.Id {
    r.read1ReplyChan <- read1Reply
  } else {
    r.SendMsg(coordinatorId, r.read1ReplyRPC, read1Reply)
  }
}

func (r *Replica) replyRead2Replica(coordinatorId int32,
    read2Reply *gryffproto.Read2Reply) {
  if coordinatorId == r.Id {
    r.read2ReplyChan <- read2Reply
  } else {
    r.SendMsg(coordinatorId, r.read2ReplyRPC, read2Reply)
  }
}

func (r *Replica) sendRead1To(replica int32) {
  if replica == r.Id {
    read1Copy := *r.read1
    r.read1ChanProxied <- &read1Copy
  } else {
    r.SendMsg(replica, r.read1RPC, r.read1)
  }
}

func (r *Replica) sendRead2To(replica int32) {
  if replica == r.Id {
    read2Copy := *r.read2
    r.read2ChanProxied <- &read2Copy
  } else {
    r.SendMsg(replica, r.read2RPC, r.read2)
  }
}

func (r *Replica) sendWrite1To(replica int32) {
  if replica == r.Id {
    write1Copy := *r.write1
    r.write1ChanProxied <- &write1Copy
  } else {
    r.SendMsg(replica, r.write1RPC, r.write1)
  }
}

func (r *Replica) SendWrite1Timeout(write1Reply *gryffproto.Write1Reply) {
  time.Sleep(time.Duration(170 * 1e6)) // r.fastOverwriteTime * 1e6)) // 100 ms
  r.write1TimeoutChan <-write1Reply
}

func (r *Replica) SendWrite2ConcurrentTimeout(wop *gryffcommon.WriteOp) {
  r.Printf("ShortcircuitTime timeout time %d \n", r.shortcircuitTime)
  write2 := &gryffproto.Write2Proxied{wop.RequestId, wop.ClientId, wop.N,
    wop.Key, gryffproto.ValTag{state.Value(wop.WriteValue), *wop.WriteTag}, r.Id}
  // send this write to timeout channel because it is concurrent with at least one
  // ongoing write
  time.Sleep(time.Duration(0 * 1e6)) // r.shortcircuitTime * 1e6)) // 100 ms
  r.write2ConcurrentTimeoutChan <-write2
}

func (r *Replica) sendWrite2To(replica int32) {
  if replica == r.Id {
    write2Copy := *r.write2
    r.write2ChanProxied <- &write2Copy
  } else {
    r.SendMsg(replica, r.write2RPC, r.write2)
  }
}

func (r *Replica) SetForceWrite() {
  for !r.Shutdown {
    time.Sleep(time.Duration(100 * 1e6)) // r.forceWritePeriod * 1e6)) // 100 ms
    r.forceWriteChan <- true
  }
}

/* ============= */

/* Main event processing loop */

func (r *Replica) run() {
  r.ConnectToPeers()

  dlog.Println("Waiting for client connections")

  slowClockChan := make(chan bool, 1)
  if r.Beacon {
    r.Printf("Starting adaptive peer ordering...\n")
	  go r.SlowClock(slowClockChan)
    go r.StopAdapting()
	}

  if r.shortcircuitTime >= 0 {
    go r.SetForceWrite()
  }

  go r.WaitForClientConnections()

  for !r.Shutdown {
    r.rmwHandler.Loop(slowClockChan)
  }
}

func (r *Replica) handleRead1(read1 *gryffproto.Read1, client *bufio.Writer) {
  r1reply := r.parti.HandleRead1(read1.K, &read1.D, read1.RequestId,
    read1.ClientId, false, nil, read1.ClientId)
  r.replyRead1(client, r1reply)
}

func (r *Replica) handleRead1Proxied(read1Proxied *gryffproto.Read1Proxied) {
  r1reply := r.parti.HandleRead1(read1Proxied.K, &read1Proxied.D,
      read1Proxied.RequestId, read1Proxied.ClientId, true, &read1Proxied.Vt,
      read1Proxied.ReplicaId)
  r.replyRead1Replica(read1Proxied.ReplicaId, r1reply)
}

func (r *Replica) handleRead2(read2 *gryffproto.Read2, client *bufio.Writer) {
  r2reply := r.parti.HandleRead2(read2.K, read2.Vt, read2.RequestId,
      read2.ClientId, read2.UpdateId)
  r.replyRead2(client, r2reply)
}

func (r *Replica) handleRead2Proxied(read2Proxied *gryffproto.Read2Proxied) {
  r2reply := r.parti.HandleRead2(read2Proxied.K, read2Proxied.Vt,
      read2Proxied.RequestId, read2Proxied.ClientId, read2Proxied.UpdateId)
  r.replyRead2Replica(read2Proxied.ReplicaId, r2reply)
}

func (r *Replica) handleRead(read *gryffproto.Read) {
  r.Printf("Starting Read[%d,%d].\n", read.RequestId, read.ClientId)
  r.coord.StartRead(read.RequestId, read.ClientId, read.K, &read.D)
}

func (r *Replica) handleRead1Reply(read1Reply *gryffproto.Read1Reply) {
  r.Printf("Reply for Read1[%d,%d] from %d.\n", read1Reply.RequestId,
      read1Reply.ClientId, read1Reply.ReplicaId)
  r.coord.HandleRead1Reply(gryffcommon.ShiftInts(read1Reply.RequestId,
        read1Reply.ClientId), &read1Reply.Vt, read1Reply.ReplicaId)
}

func (c *Replica) handleRead2Reply(read2Reply *gryffproto.Read2Reply) {
  c.coord.HandleRead2Reply(gryffcommon.ShiftInts(read2Reply.RequestId,
        read2Reply.ClientId), read2Reply.T, read2Reply.ReplicaId)
}

func (r *Replica) handleForceWrite() {
  r.coord.HandleForceWrite()
}

func (r *Replica) handleWrite1(write1 *gryffproto.Write1,
    client *bufio.Writer) {
  w1reply := r.parti.HandleWrite1(write1.K, write1.V, write1.N, &write1.D,
      write1.RequestId, write1.ClientId, &write1.MaxAppliedTag,
      write1.ForceWrite, write1.ReplicaId)
  if (w1reply != nil) {
    r.replyWrite1(client, w1reply)
  }
}

func (r *Replica) HandleWrite1Overwrite(coordinatorId int32,
    write1Reply *gryffproto.Write1Reply) {
  r.replyWrite1Replica(coordinatorId, write1Reply)
}

func (r *Replica) handleWrite1Proxied(write1Proxied *gryffproto.Write1Proxied) {
  w1reply := r.parti.HandleWrite1(write1Proxied.K, write1Proxied.V,
      write1Proxied.N, &write1Proxied.D, write1Proxied.RequestId,
      write1Proxied.ClientId, &write1Proxied.MaxAppliedTag,
      write1Proxied.ForceWrite, write1Proxied.ReplicaId)
  if (w1reply != nil) {
    r.replyWrite1Replica(write1Proxied.ReplicaId, w1reply)
  }
}

func (r *Replica) handleWrite2(write2 *gryffproto.Write2,
    client *bufio.Writer) {
  w2reply := r.parti.HandleWrite2(write2.K, write2.Vt, write2.RequestId,
      write2.ClientId)
  r.replyWrite2(client, w2reply)
}

func (r *Replica) handleWrite2Proxied(write2Proxied *gryffproto.Write2Proxied) {
  w2reply := r.parti.HandleWrite2(write2Proxied.K, write2Proxied.Vt,
      write2Proxied.RequestId, write2Proxied.ClientId)
  r.replyWrite2Replica(write2Proxied.ReplicaId, w2reply)
}

func (r *Replica) handleWrite(write *gryffproto.Write) {
  r.coord.StartWrite(write.RequestId, write.ClientId, write.K, write.V, &write.D)
}

func (r *Replica) handleWrite1Reply(write1Reply *gryffproto.Write1Reply) {
  r.coord.HandleWrite1Reply(gryffcommon.ShiftInts(write1Reply.RequestId,
      write1Reply.ClientId), write1Reply.ClientId, write1Reply.ReplicaId,
      &write1Reply.Vt)
}

func (r *Replica) handleWrite1Timeout(write1Reply *gryffproto.Write1Reply) {
  if r.parti.OngoingWrites[write1Reply.ClientId] != nil &&
  r.parti.OngoingWrites[write1Reply.ClientId].RequestId == write1Reply.RequestId &&
  !r.parti.OngoingWrites[write1Reply.ClientId].SentWrite1Reply {
    r.Printf("Timed out waiting for Write2s; sending Write1Reply[%d,%d] now.\n",
      write1Reply.RequestId, write1Reply.ClientId)
    r.GetStats().Increment("fast_overwrites_timeout")
    if r.parti.OngoingWrites[write1Reply.ClientId].K == 0 {
      r.GetStats().Increment("fast_overwrites_timeout_0")
    }
    r.parti.OngoingWrites[write1Reply.ClientId].SentWrite1Reply = true
    r.replyWrite1Replica(r.parti.OngoingWrites[write1Reply.ClientId].ReplicaId, write1Reply)
  }
}

func (r *Replica) handleWrite2ConcurrentTimeout(write2Proxied *gryffproto.Write2Proxied) {
  r.coord.HandleWrite2ConcurrentTimeout(write2Proxied.RequestId,
    write2Proxied.ClientId, write2Proxied.N)
}

func (r *Replica) handleWrite2Reply(write2Reply *gryffproto.Write2Reply) {
  r.coord.HandleWrite2Reply(gryffcommon.ShiftInts(write2Reply.RequestId,
      write2Reply.ClientId), write2Reply.ReplicaId, &write2Reply.T,
      write2Reply.ClientId, write2Reply.Applied)
}

func (r *Replica) replyRMW(client *bufio.Writer, rmwReply *gryffproto.RMWReply) {
  client.WriteByte(clientproto.Gryff_RMW_REPLY)
  rmwReply.Marshal(client)
  client.Flush()
}

func (r *Replica) completeRMW(requestId int32, clientId int32, oldValue state.Value) {
  r.completeRMWWriter(requestId, r.GetClientWriter(clientId), oldValue)
}

func (r *Replica) completeRMWWriter(requestId int32, client *bufio.Writer,
    oldValue state.Value) {
  dlog.Printf("Replying to RMW(%d).\n", requestId)
  rmwReply := &gryffproto.RMWReply{
    requestId,
    r.Id,
    oldValue,
    1,
  }
  r.replyRMW(client, rmwReply)
}


