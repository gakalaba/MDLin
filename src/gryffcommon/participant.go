package gryffcommon

import (
  "gryffproto"
  "state"
  "stats"
)

type StoreMetadata struct {
  Tag *gryffproto.Tag
}

type OngoingWriteOp struct {
  *gryffproto.Write1Proxied
  SentWrite1Reply bool
  FastOverwite bool
}

type IGryffParticipant interface {
  Printf(fmt string, v... interface{})
  GetCurrentValue(key state.Key) state.Value
  SetCurrentValue(key state.Key, val state.Value)
  GetStats() *stats.StatsMap
  HandleWrite1Overwrite(coordId int32, w1reply *gryffproto.Write1Reply)
  SendWrite1Timeout(w1reply *gryffproto.Write1Reply)
}

type GryffParticipant struct {
  id int32
  iparti IGryffParticipant
  storeTag map[state.Key]*StoreMetadata
  noConflicts bool
  OngoingWrites map[int32]*OngoingWriteOp
}

func newWrite1Op(requestId int32, clientId int32, n int32, forceWrite int32,
  key state.Key, val state.Value, dep gryffproto.Dep, id int32,
  maxAppliedTag gryffproto.Tag) *gryffproto.Write1Proxied {
  o := &gryffproto.Write1Proxied{
    requestId,        // requestId
    clientId,         // clientId
    n,                // n
    forceWrite,       // forceWrite
    key,              // key
    val,              // val
    dep,              // dep
    id,               // replicaId
    maxAppliedTag,    // maxAppliedTag
  }
  return o
}

func newWriteOp(requestId int32, clientId int32, n int32, forceWrite int32,
  key state.Key, val state.Value, dep gryffproto.Dep, id int32,
  maxAppliedTag gryffproto.Tag) *OngoingWriteOp {
  wop := &OngoingWriteOp{
    newWrite1Op(requestId, clientId, n, forceWrite, key, val, dep, id, maxAppliedTag),
    false,                          // sentWrite1Reply
    false,                          // fastOverwrite
  }
  return wop
}

func NewGryffParticipant(id int32, iparti IGryffParticipant,
    noConflicts bool) *GryffParticipant {
  tp := &GryffParticipant{
    id,                                           // id
    iparti,                                       // iparti
    make(map[state.Key]*StoreMetadata),           // storeTag
    noConflicts,                                  // noConflict
    make(map[int32]*OngoingWriteOp),              // ongoingWrites
  }

  return tp
}

func (r *GryffParticipant) GetStoreMetadata(key state.Key) *StoreMetadata {
  meta, ok := r.storeTag[key]
  if ok {
    return meta
  } else {
    return &StoreMetadata{&gryffproto.Tag{0, 0, 0}}
  }
}

func (r *GryffParticipant) HandleRead1(key state.Key, dep *gryffproto.Dep,
    requestId int32, clientId int32, proxied bool,
    vt *gryffproto.ValTag, coordinatorId int32) *gryffproto.Read1Reply {
  r.iparti.Printf("Received Read1[%d,%d] from %d.\n", requestId, clientId,
      coordinatorId)
  if dep.Key >= 0 { // dep.Key < 0 ==> no dep
    r.HandleOverwrite(dep.Key, dep.Vt.V, &dep.Vt.T)
  }
  if proxied {
    r.HandleOverwrite(key, vt.V, &vt.T)
  }
  var tag gryffproto.Tag
  if r.noConflicts {
    tag = gryffproto.Tag{0, 0, 0}
  } else {
    meta := r.GetStoreMetadata(key)
    tag = *meta.Tag
  }
  r1reply := &gryffproto.Read1Reply{requestId, clientId, r.id,
      gryffproto.ValTag{r.iparti.GetCurrentValue(key), tag}}
  r.iparti.Printf("Replying to Read1 with (v=%d,tag=%v)\n",
      r.iparti.GetCurrentValue(key), tag)
  return r1reply
}

func (r *GryffParticipant) HandleRead2(key state.Key, vt gryffproto.ValTag,
    requestId int32, clientId int32, updateId int64) *gryffproto.Read2Reply {
  r.HandleOverwrite(key, vt.V, &vt.T)
  r2reply := &gryffproto.Read2Reply{requestId, clientId, r.id,
      *r.GetStoreMetadata(key).Tag}
  return r2reply
}

func (r *GryffParticipant) HandleWrite1(key state.Key, val state.Value,
    n int32, dep *gryffproto.Dep, requestId int32, clientId int32,
    maxAppliedTag *gryffproto.Tag, forceWrite int32,
    coordId int32) *gryffproto.Write1Reply {
  r.iparti.Printf("Received Write1[%d,%d] reply from replica %d.\n",
      requestId, clientId, coordId)
  if r.OngoingWrites[clientId] != nil && r.OngoingWrites[clientId].RequestId > requestId {
    return nil
  }
  if dep.Key >= 0 { // dep.Key < 0 ==> no dep
    r.HandleOverwrite(dep.Key, dep.Vt.V, &dep.Vt.T)
  }
  // check for concurrent writes with valid timestamps
  // noTimeout := true
  // concurrentWrites := make([]*OngoingWriteOp, 0)
  // for _, write := range r.OngoingWrites {
  //   r.iparti.Printf("concurrent Write1[%d,%d] with SentWrite1Reply %v.\n",
  //     write.RequestId, write.ClientId, write.SentWrite1Reply)
  //   if write.K == key && !write.SentWrite1Reply {
  //     concurrentWrites = append(concurrentWrites, write)
  //   } else if write.K == key && write.SentWrite1Reply && !write.FastOverwite {
  //     noTimeout = false
  //   }
  // }
  r.iparti.Printf("OngoingWrites %v Write1[%d,%d].\n", r.OngoingWrites, requestId, clientId)
  // r.iparti.Printf("Length of concurrentWrites %d Write1[%d,%d].\n", len(concurrentWrites), requestId, clientId)
  // add new write to ongoingWrites
  if r.OngoingWrites[clientId] != nil && r.OngoingWrites[clientId].RequestId < requestId {
    wop := newWriteOp(requestId, clientId, n, forceWrite, key, val, *dep, coordId, *maxAppliedTag)
    r.OngoingWrites[clientId] = wop
  } else if r.OngoingWrites[clientId] == nil {
    wop := newWriteOp(requestId, clientId, n, forceWrite, key, val, *dep, coordId, *maxAppliedTag)
    r.OngoingWrites[clientId] = wop
  }
  // if no other writes for this key currently ongoing, then send Write1Reply
  // else wait until timeout
  meta := r.GetStoreMetadata(key)
  valTag := &gryffproto.ValTag{ V: -1, T: *meta.Tag}
  if forceWrite > 0  { // || noTimeout len(concurrentWrites) == 0 {
    r.iparti.GetStats().Max("tag_map_size", len(r.storeTag))
    w1reply := &gryffproto.Write1Reply{requestId, clientId, r.id, *valTag}
    // mark Write1Reply for this write as sent
    r.OngoingWrites[clientId].SentWrite1Reply = true
    r.iparti.Printf("Replying to Write1[%d,%d](%d).\n",
        requestId, clientId, key)
    return w1reply
  } else {
    r.OngoingWrites[clientId].SentWrite1Reply = false
    w1r := &gryffproto.Write1Reply{requestId, clientId, r.id, *valTag}
    r.iparti.Printf("Begin timeout for write1[%d,%d] with SentWrite1Reply %v.\n",
      requestId, clientId, r.OngoingWrites[clientId].SentWrite1Reply)
    go r.iparti.SendWrite1Timeout(w1r)
    return nil
  }
}

func (r *GryffParticipant) HandleWrite2(key state.Key, vt gryffproto.ValTag,
    requestId int32, clientId int32) *gryffproto.Write2Reply {
  r.iparti.Printf("Received Write2[%d,%d] from coordinator with tag %v.\n",
      requestId, clientId, vt.T)
  meta := r.GetStoreMetadata(key)
  // check if write has a tag greater than what has previously been applied
  applied := 0
  if meta.Tag.LessThan(vt.T) {
    applied = 1
  }
  r.HandleOverwrite(key, vt.V, &vt.T)

  delete(r.OngoingWrites, clientId)
  ongoingWrite := false
  concurrentWrites := make([]*OngoingWriteOp, 0)
  // send Write1Replies with values if they are concurrent with this write
  for _, write := range r.OngoingWrites {
    if write.K == key {
      r.iparti.Printf("Tag %v Write2[%d,%d] concurrent with tag %v Write1[%d,%d] sent %v.\n", vt.T, requestId,
          clientId, write.MaxAppliedTag, write.RequestId, write.ClientId, write.SentWrite1Reply)
    }
    if write.K == key && !write.SentWrite1Reply && write.MaxAppliedTag.LessThan(vt.T) { // 
      // mark Write1Reply for this concurrent write as sent
      r.OngoingWrites[write.ClientId].SentWrite1Reply = true
      r.OngoingWrites[write.ClientId].FastOverwite = true
      r.iparti.GetStats().Max("tag_map_size", len(r.storeTag))
      w1reply := &gryffproto.Write1Reply{write.RequestId, write.ClientId, r.id, vt}
      r.iparti.Printf("Replying to fast overwrite Write1[%d,%d](%d).\n",
        write.RequestId, write.ClientId, key)
      r.iparti.Printf("Fast overwrite sent for Write1[%d,%d] with value %d tag %v to replica %d.\n",
        write.RequestId, write.ClientId, int64(vt.V), vt.T, write.ReplicaId)
      r.iparti.HandleWrite1Overwrite(write.ReplicaId, w1reply)
    } else if write.K == key && !write.SentWrite1Reply {
      concurrentWrites = append(concurrentWrites, write)
    // check if there is at least one write still ongoing
    } else if write.K == key && write.SentWrite1Reply {
      ongoingWrite = true
    }
  }

  // if no more ongoing concurrent writes, end timeout for pending writes
  if !ongoingWrite {
    vt := &gryffproto.ValTag{ V: -1, T: *meta.Tag}
    for _, write := range concurrentWrites {                          // TODO: only end timeout for write with max ts or for all writes?
      r.OngoingWrites[write.ClientId].SentWrite1Reply = true
      r.iparti.GetStats().Max("tag_map_size", len(r.storeTag))
      w1reply := &gryffproto.Write1Reply{write.RequestId, write.ClientId,
        r.id, *vt}
      r.iparti.Printf("Replying to Write1[%d,%d](%d) and ending timeout.\n", requestId,
        clientId, key)
      r.iparti.Printf("Stopped timeout for Write1[%d,%d].\n", write.RequestId, write.ClientId)
      r.iparti.HandleWrite1Overwrite(write.ReplicaId, w1reply)
    }
  }

  w2reply := &gryffproto.Write2Reply{requestId, clientId, r.id,
      *r.GetStoreMetadata(key).Tag, int32(applied)}
  r.iparti.Printf("Replying to Write2[%d,%d](%d) with applied %v.\n",
    requestId, clientId, key, applied)
  return w2reply
}

func (r *GryffParticipant) HandleOverwrite(key state.Key, newValue state.Value,
    newTag *gryffproto.Tag) {
  if r.noConflicts {
    r.iparti.SetCurrentValue(key, newValue)
  } else {
    meta := r.GetStoreMetadata(key)
    if meta.Tag.LessThan(*newTag) {
      r.iparti.Printf("Overwriting (v=%d,tag=%v) with (v=%d,tag=%v)\n",
        r.iparti.GetCurrentValue(key), meta.Tag, newValue, newTag)
      r.iparti.SetCurrentValue(key, newValue)
      meta.Tag = newTag
      r.storeTag[key] = meta
    } else {
      r.iparti.Printf("Not overwriting (v=%d,tag=%v) with (v=%d,tag=%v)\n",
        r.iparti.GetCurrentValue(key), meta.Tag, newValue, newTag)
    }
  }
  r.iparti.GetStats().Max("tag_map_size", len(r.storeTag))
}
