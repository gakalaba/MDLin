package gryffcommon

import (
  "gryffproto"
  "sort"
  "stats"
  "state"
)

func Max(a int32, b int32) int32 {
  if a > b {
    return a
  } else {
    return b
  }
}

type Op struct {
  RequestId int32
  ClientId int32
  Key state.Key
  Dep *gryffproto.Dep
}

type OngoingOp struct {
  RequestId int32
  ClientId int32
  OpType bool
  Key state.Key
  Value *gryffproto.ValTag
  SentWrite1 bool
  ShortReads  []int32
  ShortWrites []int32
  Replicas    []int32
}

type ReadValTag struct {
  Vt *gryffproto.ValTag
  UpdateId int64
}

type ReadOp struct {
  *Op
  N int32
  maxValTag *gryffproto.ValTag
  ReadValTag *gryffproto.ValTag
  numRead1Replies int
  numRead2Replies int
  readsWithMaxTag int
  sentRead2Round bool
  determinedReadVal bool
  read1Replies []ReadValTag
  maxUpdateId int64
  replicasWithMaxTag []int32
  ReadUpdateId int64
}

type WriteOp struct {
  *Op
  N int32
  WriteValue state.Value
  MaxAppliedTag *gryffproto.Tag
  maxValTag *gryffproto.ValTag
  ForceWrite int32
  numWrite1Applies int
  numWrite1Replies int
  numWrite2Applies int
  numWrite2Replies int
  sentWrite2Round bool
  maxGreenTag *gryffproto.Tag
  repliesWithMaxGreenTag int
  WriteTag *gryffproto.Tag
  replicasWithTag map[gryffproto.Tag][]int32
  startedShortcircuitTimeout bool
}

func NewOp(requestId int32, clientId int32, key state.Key, dep *gryffproto.Dep) *Op {
  o := &Op{
    requestId,        // requestId
    clientId,         // clientId
    key,              // key
    dep,              // dep
  }
  return o
}

func newOngoingOp(requestId int32, clientId int32, opType bool, key state.Key,
  sentWrite1 bool, replicas []int32) *OngoingOp {
  oop := &OngoingOp{
    requestId,             // requestId
    clientId,              // clientId
    opType,                // opType, where true is read and write is false
    key,                   // key
    nil,                   // value
    sentWrite1,            // sentWrite1
    make([]int32, 0),      // shortReads
    make([]int32, 0),      // shortWrites
    replicas,              // replicas
  }
  return oop
}

func newRead(requestId int32, clientId int32, key state.Key,
  dep *gryffproto.Dep, n int32) *ReadOp {
  r := &ReadOp{
    NewOp(requestId, clientId, key, dep),
    n,                              // n
    nil,                            // maxValTag
    nil,                            // readValTag
    0,                              // numRead1Replies
    0,                              // numRead2Replies
    0,                              // readsWithMaxTag
    false,                          // sentRead2Round
    false,                          // determinedReadVal
    make([]ReadValTag, 0),          // read1Replies
    -1,                             // maxUpdateId
    nil,                            // replicasWithMaxTag
    0,                              // readUpdateId
  }
  return r
}

func newWrite(requestId int32, clientId int32, key state.Key, val state.Value,
    dep *gryffproto.Dep, n int32, maxTag *gryffproto.Tag) *WriteOp {
  w := &WriteOp{
    NewOp(requestId, clientId, key, dep),
    n,                                // n
    val,                              // writeValue
    maxTag,                           // maxAppliedTag
    &gryffproto.ValTag{               // maxValTag
      V: -1,
      T: gryffproto.Tag{0, 0, 0},
    },
    0,                                // forceWrite     
    0,                                // numWrite1Applies
    0,                                // numWrite1Replies
    0,                                // numWrite2Applies
    0,                                // numWrite2Replies
    false,                            // sentWrite2Round
    nil,                              // maxGreenTag
    0,                                // repliesWithMaxGreenTag
    nil,                              // writeTag
    make(map[gryffproto.Tag][]int32), // replicasWithTag
    false,                            // startedShortcircuitTimeout
  }
  return w
}

type IGryffCoordinator interface {
  Printf(fmt string, v... interface{}) 
  SendRead1(rop *ReadOp, replicas []int32)
  SendRead2(rop *ReadOp, replicas []int32)
  SendWrite1(wop *WriteOp, replicas []int32)
  SendWrite2(wop *WriteOp, replicas []int32)
  CompleteRead(requestId int32, clientId int32, success bool, readValue int64)
  CompleteWrite(requestId int32, clientId int32, success bool)
  GetStats() *stats.StatsMap
  GetReplicasByRank() []int32
  HandleOverwrite(key state.Key, newValue state.Value, newTag *gryffproto.Tag)
  SendWrite2ConcurrentTimeout(w *WriteOp)
  SetForceWrite() 
}

type GryffCoordinator struct {
  id int32
  icoord IGryffCoordinator
  regular bool
  numReplicas int
  maxSeenTag gryffproto.Tag
  maxAppliedTag gryffproto.Tag
  maxN int32
  rops map[int64]*ReadOp
  wops map[int64]*WriteOp
  allReplicas []int32
  proxy bool
  thrifty bool
  epaxosMode bool
  OngoingReqs map[int32]*OngoingOp
  ForcedKeys map[state.Key]int32
  MaxTags map[state.Key]*gryffproto.Tag
  ForcedKeysChanged []state.Key
}

func NewGryffCoordinator(id int32, icoord IGryffCoordinator, regular bool,
    numReplicas int, proxy bool, thrifty bool,
    epaxosMode bool) *GryffCoordinator {
  tc := &GryffCoordinator{
    id,                                                // id
    icoord,                                            // icoord
    regular,                                           // regular
    numReplicas,                                       // numReplicas
    gryffproto.Tag{0, 0, 0},                           // maxSeenTag
    gryffproto.Tag{0, 0, 0},                           // maxAppliedTag
    0,                                                 // maxN
    make(map[int64]*ReadOp),                           // rops
    make(map[int64]*WriteOp),                          // wops
    make([]int32, numReplicas),                        // allReplicas
    proxy,                                             // proxy
    thrifty,                                           // thrifty
    epaxosMode,                                        // epaxosMode
    make(map[int32]*OngoingOp),                        // ongoingReqs
    make(map[state.Key]int32),                         // forcedKeys
    make(map[state.Key]*gryffproto.Tag),               // maxTags
    make([]state.Key, 0),                              // forcedKeysChanged
  }
  for i := 0; i < numReplicas; i++ {
    tc.allReplicas[i] = int32(i)
  }
  
  return tc
}

/**
 * Begin common procedures
 */
func ShiftInts(a int32, b int32) int64 {
  return (int64(a) << 32) | int64(b)
}

func (c *GryffCoordinator) ShortCircuit(n int32, value *gryffproto.ValTag, fastOverwrite bool) {
  // sort sequence numbers of operations in decreasing order
  // key := c.OngoingReqs[n].Key
  // keys := make([]int32, 0)
  // for num, k := range c.OngoingReqs {
  //     if k.Key == key && k.OpType {
  //       keys = append(keys, num)
  //     }
  // }
  keys := c.OngoingReqs[n].ShortReads
  sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
  c.icoord.Printf("Shortcircuit (is fastoverwrite %v) called for op %d [%d,%d] with reads %v and writes %v.\n",
    fastOverwrite, n, c.OngoingReqs[n].RequestId, c.OngoingReqs[n].ClientId, keys,
    c.OngoingReqs[n].ShortWrites)

  // iterate and shortcircuit any ongoing reads with sequence numbers < n
  for _, k := range keys {
      if k >= n && fastOverwrite {
        break;
      }
      op, ok := c.OngoingReqs[k]
      if !ok {
        c.icoord.Printf("Ignored shortcircuit Read because request already completed.\n")
        continue
      }
      id := ShiftInts(op.RequestId, op.ClientId)
      read, ok := c.rops[id]
      if !ok {
        c.icoord.Printf("Ignored shortcircuit Read because request already completed.\n")
        continue
      }
      c.icoord.Printf("Shortcircuit for read[%d,%d].\n", read.RequestId, read.ClientId)
      c.icoord.GetStats().Increment("shortcircuit_reads")
      if read.Key == 0 {
        c.icoord.GetStats().Increment("shortcircuit_reads_0")
      }
      delete(c.OngoingReqs, k)
      delete(c.rops, id)
      c.icoord.CompleteRead(op.RequestId, op.ClientId, true, int64(value.V))
  }
  // cannot shortcircuit writes is this is a fast overwrite
  if fastOverwrite {
    return
  }

  // shortcircuit any concurrent writes saved in ShortWrites for write n
  writeNums := c.OngoingReqs[n].ShortWrites
  for _, num := range writeNums {
      op, ok := c.OngoingReqs[num]
      if !ok {
        c.icoord.Printf("Ignored shortcircuit Write because request already completed.\n")
        continue
      }
      id := ShiftInts(op.RequestId, op.ClientId)
      write, ok := c.wops[id]
      if !ok {
        c.icoord.Printf("Ignored shortcircuit Write because request already completed.\n")
        continue
      }
      // cannot shortcircuit write if Write2 for this write has already been sent
      if write.sentWrite2Round {
        c.icoord.Printf("Ignored shortcircuit Write[%d,%d] because Write2 already sent.\n",
          write.RequestId, write.ClientId)
        continue
      }
      c.icoord.Printf("Shortcircuit for Write[%d,%d].\n", write.RequestId, write.ClientId)
      c.icoord.GetStats().Increment("shortcircuit_writes")
      if write.Key == 0 {
          c.icoord.GetStats().Increment("shortcircuit_writes_0")
      }
      // for _, numWrite := range op.ConcurrentWrites {
      //   wop, ok := c.OngoingReqs[numWrite]
      //   if !ok {
      //     c.icoord.Printf("Ignored shortcircuit Write because request already completed.\n")
      //     continue
      //   }
      //   wid := ShiftInts(wop.RequestId, wop.ClientId)
      //   writeNext, ok := c.wops[wid]
      //   if !ok {
      //     c.icoord.Printf("Ignored shortcircuit Write because request already completed.\n")
      //     continue
      //   }
      //   if writeNext.startedShortcircuitTimeout {
      //     c.icoord.GetStats().Increment("early_shortcircuit_timeout")
      //     c.HandleWrite2ConcurrentTimeout(wop.RequestId, wop.ClientId, numWrite)
      //   }
      // }
      delete(c.OngoingReqs, num)
      delete(c.wops, id)
      c.icoord.CompleteWrite(op.RequestId, op.ClientId, true)
  }
}
/**
 * End common procedures
 */

/**
 * Begin Read related procedures
 */
func (c *GryffCoordinator) StartRead(requestId int32, clientId int32,
    key state.Key, dep *gryffproto.Dep) {
  c.maxN++
  n := c.maxN
  op := newOngoingOp(requestId, clientId, true, key, false, make([]int32, 0))
  c.OngoingReqs[n] = op
  r := newRead(requestId, clientId, key, dep, n)
  c.rops[ShiftInts(requestId, clientId)] =  r

  for _, wop := range c.wops {
    // c.icoord.Printf("Compare key %v with wop.Key %v.\n", key, wop.Key)
    if wop.Key == key && !wop.sentWrite2Round {
      c.icoord.Printf("Write[%d,%d] is concurrent with Read[%d,%d]", wop.RequestId, wop.ClientId,
        requestId, clientId)
      c.OngoingReqs[wop.N].ShortReads = append(c.OngoingReqs[wop.N].ShortReads, n)
    }
  }

  if c.proxy {
    r.numRead1Replies = 1
  }

  replicas := make([]int32, 0)
  for i := int32(0); i < int32(c.numReplicas); i++ {
    if !c.proxy || i != c.id {
      replicas = append(replicas, i)
    }
  }
  if c.thrifty {
    // TODO: why was this commented?
    replicas = c.getThriftyReplicas(replicas, r.numRead1Replies)
  } 

  c.icoord.SendRead1(r, replicas)
}

func (c *GryffCoordinator) HandleRead1Reply(id int64, vt *gryffproto.ValTag,
    replicaId int32) {
  read, ok := c.rops[id]
  if !ok {
    c.icoord.Printf("Ignored Read1 reply because request already completed.\n")
    return
  }

  if !read.determinedReadVal {
    c.icoord.GetStats().IncrementStrInt("read_1_quorum", int(replicaId))
  }

  read.numRead1Replies++
  c.icoord.Printf("Received %d read replies so far.\n", read.numRead1Replies)
  if read.maxValTag == nil || vt.T.GreaterThan(read.maxValTag.T) {
    if read.maxValTag != nil {
      c.icoord.Printf("Updating max tag %v with %v.\n", read.maxValTag.T, vt.T)
    }
    read.maxValTag = vt
    read.replicasWithMaxTag = make([]int32, 0)
    if c.proxy {
      c.icoord.HandleOverwrite(state.Key(read.Key),
          state.Value(read.maxValTag.V), &read.maxValTag.T)
      read.readsWithMaxTag = 1
      read.replicasWithMaxTag = append(read.replicasWithMaxTag, c.id)
    } else {
      read.readsWithMaxTag = 0
    }
  }
  if vt.T == read.maxValTag.T {
    read.readsWithMaxTag++
    read.replicasWithMaxTag = append(read.replicasWithMaxTag, replicaId)
    c.icoord.Printf("Received another reply %d with tag = current max tag = %v.\n",
      read.readsWithMaxTag, read.maxValTag.T)
  }

  if read.numRead1Replies >= (c.numReplicas / 2) + 1 {
    if !read.determinedReadVal {
      read.ReadValTag = read.maxValTag
      read.determinedReadVal = true
    }
    if c.regular || read.readsWithMaxTag >= (c.numReplicas / 2) + 1 {
      c.icoord.Printf("Fast read because regular=%v, readsWithMaxTag=%d, n=%d.\n",
          c.regular, read.readsWithMaxTag, c.numReplicas)
      c.icoord.GetStats().Increment("fast_reads")
      if read.Key == 0 {
        c.icoord.GetStats().Increment("fast_reads_0")
      }
      delete(c.OngoingReqs, read.N)
      delete(c.rops, id)
      c.icoord.CompleteRead(read.RequestId, read.ClientId, true,
          int64(read.ReadValTag.V))
    } else if !read.sentRead2Round {
      // Only send Read2 to replicas with tag < maxTag
      c.icoord.Printf("Slow read because regular=%v, readsWithMaxTag=%d, n=%d.\n",
          c.regular, read.readsWithMaxTag, c.numReplicas)

      replicas := make([]int32, 0)
      for i := int32(0); i < int32(c.numReplicas); i++ {
        send := true
        for j := 0; j < len(read.replicasWithMaxTag); j++ {
          if i == read.replicasWithMaxTag[j] {
            send = false
            break
          }
        }
        if send {
          replicas = append(replicas, int32(i))
        }
      }
      // TODO: this feels kind of hacky. we are implying that if we don't send
      // you a Read2, we already "received" a Read2Reply from you, because the
      // purpose of the Read2Reply is to let us know your tag is >= maxTag
      read.numRead2Replies = c.numReplicas - len(replicas)

      if c.thrifty {
        // TODO: why was this commented?
        replicas = c.getThriftyReplicas(replicas, read.numRead2Replies)
      }

      c.icoord.GetStats().IncrementStrInt("read_2_sent", len(replicas))
      c.icoord.SendRead2(read, replicas)
      read.sentRead2Round = true
    }
  }
}

func (c *GryffCoordinator) HandleRead2Reply(id int64, t gryffproto.Tag,
    replicaId int32) {
  read, ok := c.rops[id]
  if !ok {
    c.icoord.Printf("Ignored Read2 reply because request already completed.\n")
    return
  }

  c.icoord.GetStats().IncrementStrInt("read_2_quorum", int(replicaId))

  read.numRead2Replies++

  // TODO: technically we can complete as soon as we know the tag is on
  // a quorum (read.readsWithMaxTag + read.numRead2Replies >= N / 2 + 1)
  if read.numRead2Replies >= (c.numReplicas / 2) + 1 {
    c.icoord.GetStats().Increment("slow_reads")
    if read.Key == 0 {
      c.icoord.GetStats().Increment("slow_reads_0")
    }
    delete(c.OngoingReqs, read.N)
    delete(c.rops, id)
    c.icoord.CompleteRead(read.RequestId, read.ClientId, true, int64(read.ReadValTag.V))
  }
}
/**
 * End Read related procedures
 */

/**
 * Begin Write related procedures
 */
func (c *GryffCoordinator) StartWrite(requestId int32, clientId int32,
    key state.Key, val state.Value, dep *gryffproto.Dep) {
  c.maxN++
  n := c.maxN

  forceWrite, ok := c.ForcedKeys[key]
  if !ok {
    forceWrite = 1
  } 

  // TODO: change to checkout if shortcircuiting/fast overwrite is enabled
  //if c.R.shortcircuitTime == -1 {
    forceWrite = 1
  //}

  // if write does not need to be force written, add new write to list
  // of ShortWrites for concurrent writes
  concurrentWrites := make([]int32, 0)
  if forceWrite == 0 {
    c.icoord.Printf("Length of c.wops %d and value %v.\n", len(c.wops), c.wops)
    for _, wop := range c.wops {
      // c.icoord.Printf("Compare key %v with wop.Key %v.\n", key, wop.Key)
      if wop.Key == key && !wop.startedShortcircuitTimeout && !wop.sentWrite2Round {
        c.icoord.Printf("Write[%d,%d] is concurrent with Write[%d,%d]", wop.RequestId, wop.ClientId,
          requestId, clientId)
        concurrentWrites = append(concurrentWrites, wop.N)
      }
      // } else if wop.Key == key && wop.sentWrite2Round {
      //   forceWrite = 1
      //   concurrentWrites = make([]int32, 0)
      //   break;
      // }
    }
    // if there are no ongoing concurrent writes, force new write
    // otherwise, add new write to list of ShortWrites for concurrent writes
    if len(concurrentWrites) == 0 {
      forceWrite = 1
    } else {
      for _, writeN := range concurrentWrites {
        c.OngoingReqs[writeN].ShortWrites = append(c.OngoingReqs[writeN].ShortWrites, n)
      }
    }
  }

  tag, ok := c.MaxTags[key]
  if !ok {
    tag = &c.maxAppliedTag
    c.MaxTags[key] = tag
  }
  c.icoord.Printf("Max tag %v for Write1[%d,%d] for key %v", tag,
        requestId, clientId, key)
  w := newWrite(requestId, clientId, key, val, dep, n, tag)
  w.ForceWrite = forceWrite
  c.wops[ShiftInts(requestId, clientId)] =  w

  var replicas []int32
  if c.thrifty {
    // TODO: why was this commented?
    replicas = c.getThriftyReplicas(c.allReplicas, 0)
  } else {
    replicas = c.allReplicas
  }

  // if no writes to same key currently Ongoing, send write1 immediately
  // else wait until after timeout to send
  // c.icoord.Printf("Count of shortWrites for concurrent write %d.\n", count)
  // if count == 0 {
    op := newOngoingOp(requestId, clientId, false, key, true, make([]int32, 0))
    c.OngoingReqs[n] = op
    c.icoord.SendWrite1(w, replicas)
  // } else {
  //   op := newOngoingOp(requestId, clientId, false, key, false, replicas)
  //   c.OngoingReqs[n] = op
  //   c.icoord.Printf("Timeout for concurrent write[%d,%d] with OngoingReqs n %d value %v.\n",
  //     requestId, clientId, n, c.OngoingReqs[n])
  //   go c.icoord.SendWrite1ConcurrentTimeout(w, n)
  // }
  c.icoord.Printf("ForceWrite %v for Write1[%d,%d] for key %v.\n", forceWrite,
    requestId, clientId, key)
  
  // TODO: check if shortcircuiting/fast overwrite is enabled
  /*if c.R.shortcircuitTime >= 0 && forceWrite > 0 {
    c.ForcedKeys[key] = 0
    c.ForcedKeysChanged = append(c.ForcedKeysChanged, key)
  }*/
}

func (c *GryffCoordinator) HandleForceWrite() {
  for _, key := range c.ForcedKeysChanged {
    c.ForcedKeys[key] = 1
  }
  c.ForcedKeysChanged = make([]state.Key, 0)
}

func (c *GryffCoordinator) HandleWrite1Reply(id int64, clientId int32,
    replicaId int32, vt *gryffproto.ValTag) {
  c.icoord.Printf("Received Write1[%d,%d] reply from replica %d.\n",
      id >> 32, clientId, replicaId)

  write, ok := c.wops[id]
  if !ok {
    c.icoord.Printf("Ignored Write1 reply because request already completed.\n")
    return
  }

  if !write.sentWrite2Round {
    c.icoord.GetStats().IncrementStrInt("write_1_quorum", int(replicaId))
  }

  write.numWrite1Replies++
  if vt.T.GreaterThan(write.maxValTag.T) {
    write.maxValTag = vt
    if write.maxValTag.V >= 0 {
      write.numWrite1Applies = 1
    } else {
      write.numWrite1Applies = 0
    }
  } else if vt.T.Equals(write.maxValTag.T) {
    if vt.V == write.maxValTag.V && write.maxValTag.V >= 0 { // && write.numWrite1Applies > 0 
      write.numWrite1Applies++
    }
  }

  c.icoord.Printf("Write1Reply[%d,%d] with tag %v value %d.\n",
        id >> 32, clientId, vt.T, int64(vt.V))
  c.icoord.Printf("numWrite1Applies %d with value %v for Write1[%d,%d].\n",
    write.numWrite1Applies, vt.V, write.RequestId, write.ClientId)
  c.icoord.Printf("Total Write1[%d,%d] replies received: %d.\n", id >> 32,
      clientId, write.numWrite1Replies)
  if write.numWrite1Replies >= (c.numReplicas / 2) + 1 {
    // if a quorum of Write1Apply messages received, then apply value
    if write.numWrite1Applies >= (c.numReplicas / 2) + 1 {
      c.icoord.GetStats().Increment("fast_writes")
      if write.Key == 0 {
          c.icoord.GetStats().Increment("fast_writes_0")
      }
      c.icoord.Printf("Fast OverWrite1[%d,%d] reply from replica %d.\n",
          id >> 32, clientId, replicaId)
      c.ShortCircuit(write.N, write.maxValTag, true)
      delete(c.wops, id)
      delete(c.OngoingReqs, write.N)
      c.icoord.CompleteWrite(write.RequestId, write.ClientId, true)
    } else if !write.sentWrite2Round && !write.startedShortcircuitTimeout {
      write.WriteTag = &gryffproto.Tag{write.maxValTag.T.Ts + 1, clientId, 0}

      var replicas []int32
      if c.thrifty {
        replicas = c.getThriftyReplicas(c.allReplicas, 0)
      } else {
        replicas = c.allReplicas
      }
      // check if there are ongoing concurrent writes
      // count := 0;
      // for _, wop := range c.wops {
      //   if wop.Key == write.Key { // && wop.sentWrite2Round{
      //     count++
      //   }
      // }
      if write.numWrite1Applies > 0 {
        c.icoord.GetStats().Increment("failed_fast_overwrite_quorum_condition")
      }      
      c.icoord.Printf(
          "Sending Write2[%d,%d] forceWrite %v with OngoingReqs n %d value %v tag %v.\n",
          write.RequestId, write.ClientId, write.ForceWrite, write.N, c.OngoingReqs[write.N], write.WriteTag)
      // if write.ForceWrite > 0 {
        c.icoord.GetStats().IncrementStrInt("write_2_sent", len(replicas))
        c.icoord.SendWrite2(write, replicas)
        write.sentWrite2Round = true
      // } else {
      //   c.OngoingReqs[write.N].Replicas = replicas
      //   c.icoord.Printf(
      //     "Shortcircuit timeout for concurrent Write2[%d,%d] with OngoingReqs n %d value %v tag %v.\n",
      //     write.RequestId, write.ClientId, write.N, c.OngoingReqs[write.N], write.maxValTag.T)
      //   write.startedShortcircuitTimeout = true
      //   go c.icoord.SendWrite2ConcurrentTimeout(write)
      // }
    }
  }
}

func (c *GryffCoordinator) HandleWrite2ConcurrentTimeout(requestId int32,
  clientId int32, n int32) {
  c.icoord.Printf("Received timeout Write2[%d,%d] reply with OngoingReqs n %d value %v.\n",
    requestId, clientId, n, c.OngoingReqs[n])
  // if c.wops[ShiftInts(requestId, clientId)].Key == 0 {
  //     c.icoord.GetStats().Increment("shortcircuit_writes_timeout_0")
  // }
  write, ok := c.wops[ShiftInts(requestId, clientId)]
  if !ok {
    c.icoord.Printf("Ignored Write2 request to send because request already completed.\n")
    return
  }
  if write.sentWrite2Round {
    c.icoord.Printf("Ignored Write2 request to send because request already completed.\n")
    return
  }
  c.icoord.GetStats().Increment("shortcircuit_writes_timeout")
  if write.Key == 0 {
    c.icoord.GetStats().Increment("shortcircuit_writes_timeout_0")
  }
  c.icoord.GetStats().IncrementStrInt("write_2_sent", len(c.OngoingReqs[n].Replicas))
  c.icoord.Printf("Sending Write2[%d,%d] after timeout to %v for %v.\n", requestId,
    clientId, c.OngoingReqs[n].Replicas, c.OngoingReqs[n])
  c.icoord.SendWrite2(write, c.OngoingReqs[n].Replicas)
  write.sentWrite2Round = true
}

func (c *GryffCoordinator) getThriftyReplicas(replicas []int32,
    n int) []int32 {
  thriftyReplicas := make([]int32, 0)
  rbr := c.icoord.GetReplicasByRank()
  // selection sort
  for i := 0; i < len(rbr); i++ {
    for j := 0; j < len(replicas); j++ {
      if int32(rbr[i]) == replicas[j] {
        thriftyReplicas = append(thriftyReplicas, replicas[j])
        break
      }
    }
    if n + len(thriftyReplicas) >= c.numReplicas / 2 + 1 {
      break
    }
  }
  return thriftyReplicas
}

func (c *GryffCoordinator) HandleWrite2Reply(id int64, replicaId int32,
    t *gryffproto.Tag, clientId int32, applied int32) {
  c.icoord.Printf("Received Write2[%d,%d] reply from replica %d with applied %v.\n",
    id >> 32, clientId, replicaId, applied)

  write, ok := c.wops[id]
  if !ok {
    c.icoord.Printf("Ignored Write2 reply because request already completed.\n")
    return
  }

  c.icoord.GetStats().IncrementStrInt("write_2_quorum", int(replicaId))

  write.numWrite2Replies++
  if applied > 0 {
    write.numWrite2Applies++
  }

  c.icoord.Printf("Total Write2 replies received: %d and applied received: %d for tag %v.\n",
    write.numWrite2Replies, write.numWrite2Applies, t)
  if write.numWrite2Replies >= (c.numReplicas / 2) + 1 {
    if c.epaxosMode {
      replicas := make([]int32, 0)
      for i := int32(0); i < int32(c.numReplicas); i++ {
        if i != c.id {
          replicas = append(replicas, i)
        }
      }
      c.icoord.SendWrite2(write, replicas)
    }
    c.icoord.GetStats().Increment("slow_writes")
    if write.Key == 0 {
        c.icoord.GetStats().Increment("slow_writes_0")
    }
    // update maxAppliedTag
    if c.maxAppliedTag.LessThan(*write.WriteTag) {
        c.maxAppliedTag = *write.WriteTag
    }
    if c.MaxTags[write.Key].LessThan(*write.WriteTag) {
      c.MaxTags[write.Key] = write.WriteTag
      c.icoord.Printf("MaxTag for key %v for Write2[%d,%d] is tag %v.\n",
          write.Key, id >> 32, clientId, c.MaxTags[write.Key])
    }
    // only shortcircuit other operations if this write applied to a quorum
    if write.numWrite2Applies >= (c.numReplicas / 2) + 1 {
      c.icoord.GetStats().Increment("fulfilled_shortcircuit_quorum_condition")
      c.ShortCircuit(write.N, &gryffproto.ValTag{ V: write.WriteValue, T: *write.WriteTag}, false)
    } else {
      c.icoord.GetStats().Increment("failed_shortcircuit_quorum_condition")
    }

    delete(c.wops, id)
    delete(c.OngoingReqs, write.N)
    c.icoord.CompleteWrite(write.RequestId, write.ClientId, true)
  }
}
