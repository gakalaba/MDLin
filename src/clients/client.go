package clients

import (
	"bufio"
	"clientproto"
	"coordinatorproto"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"io"
	"log"
	"masterproto"
	"net"
	"net/rpc"
	"state"
	"stats"
	"time"
	"dlog"
)

const CHAN_BUFFER_SIZE = 200000

const TIMEOUT_SECS = 10
const MAX_RETRIES = 5

type ReplicaReply struct {
	reply   interface{}
	replica int
	err     error
}

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Client interface {
	Read(key int64) (bool, int64)
	Write(key int64, value int64) bool
	CompareAndSwap(key int64, oldValue int64,
		newValue int64) (bool, int64)
	AppRequest(opTypes []state.Operation, keys []int64) (bool, int64)
	Finish()
	ConnectToCoordinator()
	// ConnectToReplicas()
	// DetermineLeader()
	// DetermineReplicaPings()
	// DelayRPC(replica int, opCode uint8)
}

type AbstractClient struct {
	id                 int32
	coordinatorAddr    string
	coordinatorPort    int
	forceLeader        int
	stats              *stats.StatsMap
	statsFile          string
	rpcTable           map[uint8]*RPCPair
	glArgs             *masterproto.GetLeaderArgs
	glReply            *masterproto.GetLeaderReply
	replyChan          chan *ReplicaReply
	leaderAddrs        []string
	numLeaders         int
	leaders            []net.Conn
	readers            []*bufio.Reader
	writers            []*bufio.Writer
	shutdown           bool
	leader             int
	pingReplyChan      chan fastrpc.Serializable
	leaderAlive        []bool
	replicaPing        []uint64
	replicasByPingRank []int32
	retries            []int
	delayRPC           []map[uint8]bool
	delayedRPC         []map[uint8]chan fastrpc.Serializable
}

func NewAbstractClient(id int32, coordinatorAddr string, coordinatorPort int, forceLeader int, statsFile string) *AbstractClient {
	c := &AbstractClient{
		id,                            // id
		coordinatorAddr,               // coordinatorAddr
		coordinatorPort,               // coordinatorPort
		forceLeader,                   // forceLeader
		stats.NewStatsMap(),           // stats
		statsFile,                     // statsFile
		make(map[uint8]*RPCPair),      // rpcTable
		&masterproto.GetLeaderArgs{},  // glArgs
		&masterproto.GetLeaderReply{}, // glReply
		make(chan *ReplicaReply),      // replyChan
		make([]string, 0),             // replicaAddrs
		-1,                            // numReplicas
		make([]net.Conn, 0),           // replicas
		make([]*bufio.Reader, 0),      // readers
		make([]*bufio.Writer, 0),      // writers
		false,                         // shutdown
		-1,                            // leader
		make(chan fastrpc.Serializable, // pingReplyChan
			CHAN_BUFFER_SIZE),
		make([]bool, 0),           // replicasAlive
		make([]uint64, 0),         // replicaPing
		make([]int32, 0),          // replicasByPingRank
		make([]int, 0),            // retries
		make([]map[uint8]bool, 0), // delayRPC
		make([]map[uint8]chan fastrpc.Serializable, 0), // delayedRPC
	}
	log.Printf("Num leaders = %v", c.numLeaders)
	c.RegisterRPC(new(clientproto.PingReply), clientproto.GEN_PING_REPLY, c.pingReplyChan)

	c.ConnectToCoordinator()
	c.ConnectToShards()
	// c.DetermineLeader()
	// c.DetermineReplicaPings()

	return c
}

func (c *AbstractClient) Finish() {
	if !c.shutdown {
		c.shutdown = true
		if len(c.statsFile) > 0 {
			c.stats.Export(c.statsFile)
		}
		for _, replica := range c.leaders {
			replica.Close()
		}
	}
}

func (c *AbstractClient) ConnectToCoordinator() {
	log.Printf("Dialing coordinator at addr %s:%d\n", c.coordinatorAddr, c.coordinatorPort)
	//////////////////////////////////////
	// Get info from coordinator
	//////////////////////////////////////
	coordinator, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", c.coordinatorAddr, c.coordinatorPort))
	if err != nil {
		log.Fatalf("Error connecting to coordinator: %v\n", err)
	}

	// Get the shard leaders
	llReply := new(coordinatorproto.GetShardLeaderListReply)
	err = coordinator.Call("Coordinator.GetShardLeaderList", new(coordinatorproto.GetShardLeaderListArgs), llReply)
	if err != nil {
		log.Fatalf("Error making the GetShardLeaderList RPC: %v\n", err)
	}
	log.Printf("Got shard leader list from coordinator: [")
	for i := 0; i < len(llReply.LeaderList); i++ {
		log.Printf("%s", llReply.LeaderList[i])
		if i != len(llReply.LeaderList)-1 {
			log.Printf(", ")
		}
	}
	log.Printf("]\n")

	// TODO: Update coordinator to return leader and replicas for each shard.
	//       Then update data structures here to keep track of state for all of them.
	c.leaderAddrs = llReply.LeaderList
	c.numLeaders = len(c.leaderAddrs)
	log.Printf("after getting list from coordinator, this client thinks there's %v leaders", c.numLeaders)
	c.leaderAlive = make([]bool, c.numLeaders)
	// c.replicaPing = make([]uint64, c.numLeaders)
	// c.replicasByPingRank = make([]int32, c.numLeaders)
	c.retries = make([]int, c.numLeaders)
	// c.delayRPC = make([]map[uint8]bool, c.numLeaders)
	// c.delayedRPC = make([]map[uint8]chan fastrpc.Serializable, c.numLeaders)
	// for i := 0; i < c.numLeaders; i++ {
	// 	c.delayRPC[i] = make(map[uint8]bool)
	// 	c.delayedRPC[i] = make(map[uint8]chan fastrpc.Serializable)
	// }
}

func (c *AbstractClient) ConnectToShards() {
	log.Printf("Connecting to %v shards ANJAAAAA...\n", c.numLeaders)
	c.leaders = make([]net.Conn, c.numLeaders)
	c.readers = make([]*bufio.Reader, c.numLeaders)
	c.writers = make([]*bufio.Writer, c.numLeaders)
	for i := 0; i < c.numLeaders; i++ {
		if !c.connectToLeader(i) {
			log.Fatalf("Must connect to all leaders on startup.\n")
		}
	}
	log.Printf("Successfully connected to all %d leaders.\n", c.numLeaders)
}

func (c *AbstractClient) connectToLeader(i int) bool {
	var err error
	if c.leaders[i] != nil {
		c.leaders[i].Close()
	}
	c.retries[i]++
	log.Printf("Dialing leader %d with addr %s\n", i, c.leaderAddrs[i])
	c.leaders[i], err = net.Dial("tcp", c.leaderAddrs[i])
	if err != nil {
		log.Printf("Error connecting to leader %d: %v\n", i, err)
		return false
	}
	log.Printf("Connected to leader %d with connection %s\n", i, c.leaders[i].LocalAddr().String())
	c.readers[i] = bufio.NewReader(c.leaders[i])
	c.writers[i] = bufio.NewWriter(c.leaders[i])

	var idBytes [4]byte
	idBytesS := idBytes[:4]
	binary.LittleEndian.PutUint32(idBytesS, uint32(c.id))
	c.writers[i].Write(idBytesS)
	c.writers[i].Flush()

	c.leaderAlive[i] = true
	go c.leaderListener(i)
	return true
}

func (c *AbstractClient) GetShardFromKey(k state.Key) int {
	nShards := len(c.leaders)
	return int(k) % nShards
}

// func (c *AbstractClient) DetermineReplicaPings() {
// 	log.Printf("Determining replica pings...\n")
// 	for i := 0; i < c.numLeaders; i++ {
// 	}

// 	done := make(chan bool, c.numLeaders)
// 	for i := 0; i < c.numLeaders; i++ {
// 		go c.pingReplica(i, done)
// 	}

// 	for i := 0; i < c.numLeaders; i++ {
// 		if ok := <-done; !ok {
// 			log.Fatalf("Must successfully ping all replicas on startup.\n")
// 		}
// 	}

// 	// mini selection sort
// 	for i := 0; i < c.numLeaders; i++ {
// 		c.replicasByPingRank[i] = int32(i)
// 		for j := i + 1; j < c.numLeaders; j++ {
// 			if c.replicaPing[j] < c.replicaPing[c.replicasByPingRank[i]] {
// 				c.replicasByPingRank[i] = int32(j)
// 			}
// 		}
// 	}
// 	log.Printf("Successfully pinged all replicas!\n")
// }

// func (c *AbstractClient) pingReplica(i int, done chan bool) {
// 	var err error
// 	log.Printf("Sending ping to replica %d\n", i)
// 	err = c.writers[i].WriteByte(clientproto.GEN_PING)
// 	if err != nil {
// 		log.Printf("Error writing GEN_PING opcode to replica %d: %v\n", i, err)
// 		done <- false
// 		return
// 	}
// 	ping := &clientproto.Ping{c.id, uint64(time.Now().UnixNano())}
// 	ping.Marshal(c.writers[i])
// 	err = c.writers[i].Flush()
// 	if err != nil {
// 		log.Printf("Error flushing connection to replica %d: %v\n", i, err)
// 		done <- false
// 		return
// 	}
// 	select {
// 	case pingReplyS := <-c.pingReplyChan:
// 		pingReply := pingReplyS.(*clientproto.PingReply)
// 		c.replicaPing[pingReply.ReplicaId] = uint64(time.Now().UnixNano()) -
// 			pingReply.Ts
// 		log.Printf("Received ping from replica %d in time %d\n",
// 			pingReply.ReplicaId, c.replicaPing[pingReply.ReplicaId])
// 		done <- true
// 		log.Printf("Done pinging replica %d.\n", i)
// 		break
// 	case <-time.After(TIMEOUT_SECS * time.Second):
// 		log.Printf("Timeout out pinging replica %d.\n", i)
// 		for c.retries[i] < MAX_RETRIES {
// 			if c.connectToLeader(i) {
// 				c.pingReplica(i, done)
// 				return
// 			}
// 		}
// 		done <- false
// 		break
// 	}
// }

func (c *AbstractClient) RegisterRPC(msgObj fastrpc.Serializable,
	opCode uint8, notify chan fastrpc.Serializable) {
	c.rpcTable[opCode] = &RPCPair{msgObj, notify}
}

func (c *AbstractClient) DelayRPC(replica int, opCode uint8) {
	_, ok := c.delayedRPC[replica][opCode]
	if !ok {
		c.delayedRPC[replica][opCode] = make(chan fastrpc.Serializable, CHAN_BUFFER_SIZE)
	}
	c.delayRPC[replica][opCode] = true
}

func (c *AbstractClient) ShouldDelayNextRPC(replica int, opCode uint8) bool {
	delay, ok := c.delayRPC[replica][opCode]
	c.delayRPC[replica][opCode] = false
	return ok && delay
}

func (c *AbstractClient) leaderListener(leader int) {
	var msgType byte
	var err error
	var errS string
	for !c.shutdown && err == nil {
		if msgType, err = c.readers[leader].ReadByte(); err != nil {
			dlog.Printf("&&&&&&&&&&&&&&&&&Got this error from leader %d at time %v: %v\n", leader, time.Now().UnixMilli(), err)
			errS = "reading opcode"
			break
		}

		if rpair, present := c.rpcTable[msgType]; present {
			obj := rpair.Obj.New()
			if err = obj.Unmarshal(c.readers[leader]); err != nil {
				errS = "unmarshling message"
				break
			}
			rpair.Chan <- obj
		} else {
			log.Printf("Error: received unknown message type: %d\n", msgType)
		}
	}
	if err != nil && err != io.EOF {
		log.Printf("Error %s from replica %d: %v\n", errS, leader, err)
		c.leaderAlive[leader] = false
	}
}
