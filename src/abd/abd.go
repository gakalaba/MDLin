package abd

import (
  "dlog"
  "genericsmr"
  "abdproto"
  "bufio"
  "clientproto"
  "state"
)

const CHAN_BUFFER_SIZE = 200000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 5000

type StateTs struct {
  *state.State
  Ts map[state.Key]abdproto.Timestamp
}

type Replica struct {
  *genericsmr.Replica // extends a generic Paxos replica
  StoreTs     map[state.Key]abdproto.Timestamp
  readChan    chan *genericsmr.ClientRPC 
  writeChan   chan *genericsmr.ClientRPC
  Shutdown    bool
  flush       bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, statsFile string) *Replica {
  r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply, false, statsFile),
    make(map[state.Key]abdproto.Timestamp), 
    make(chan *genericsmr.ClientRPC, genericsmr.CHAN_BUFFER_SIZE),
    make(chan *genericsmr.ClientRPC, genericsmr.CHAN_BUFFER_SIZE),
    false,
    true,
  }
  r.Durable = durable
  r.RegisterClientRPC(new(abdproto.Read), clientproto.ABD_READ, r.readChan)
  r.RegisterClientRPC(new(abdproto.Write), clientproto.ABD_WRITE, r.writeChan)

  go r.run()

  return r
}

//sync with the stable store
func (r *Replica) sync() {
  if !r.Durable {
    return
  }

  r.StableStore.Sync()
}

/* RPC to be called by master */

func (r *Replica) replyRead(w *bufio.Writer, reply *abdproto.ReadReply) {
  w.WriteByte(clientproto.ABD_READ_REPLY)
  reply.Marshal(w)
  w.Flush()
}

func (r *Replica) replyWrite(w *bufio.Writer, reply *abdproto.WriteReply) {
  w.WriteByte(clientproto.ABD_WRITE_REPLY)
  reply.Marshal(w)
  w.Flush()
}

/* ============= */

/* Main event processing loop */

func (r *Replica) run() {
  r.ConnectToPeers()

  dlog.Println("Waiting for client connections")

  go r.WaitForClientConnections()

  for !r.Shutdown {
    select {
      case readS := <-r.readChan:
        read := readS.Obj.(*abdproto.Read)
        //got a Read message
        r.handleRead(read, readS.Reply)
        break

      case writeS := <-r.writeChan:
        write := writeS.Obj.(*abdproto.Write)
        //got a Write  message
        r.handleWrite(write, writeS.Reply)
        break
    }
  }
}

func (r *Replica) handleRead(read *abdproto.Read, client *bufio.Writer) {
  var rreply *abdproto.ReadReply
  rreply = &abdproto.ReadReply{read.RequestId, r.Id, r.State.Store[read.K],
    r.StoreTs[read.K], 1}
  dlog.Printf("Replying to Read with (v=%d,ts=%v)\n", r.State.Store[read.K],
    r.StoreTs[read.K])
  r.replyRead(client, rreply)
}

func (r *Replica) handleWrite(write *abdproto.Write, client *bufio.Writer) {
  currTs := r.StoreTs[write.K]
  if currTs.LessThan(write.Ts) {
    dlog.Printf("Overwriting (v=%d,ts=%v) with (v=%d,ts=%v)\n", r.State.Store[write.K],
      r.StoreTs[write.K], write.V, write.Ts)
    r.State.Store[write.K] = write.V
    r.StoreTs[write.K] = write.Ts
  } else {
    dlog.Printf("Not overwriting (v=%d,ts=%v) with (v=%d,ts=%v)\n",
      r.State.Store[write.K], r.StoreTs[write.K], write.V, write.Ts)
  }
  wreply := &abdproto.WriteReply{write.RequestId, r.Id, 1}
  r.replyWrite(client, wreply)
}
