package main

import (
	"coordinatorproto"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var numNodes *int = flag.Int("N", 3, "Number of replicas. Defaults to 3.")
var nodeIPs *string = flag.String("ips", "", "Space separated list of IP addresses (ordered). The leader will be 0")
var coordAddr *string = flag.String("caddr", "", "Coordinator address. Defaults to localhost")
var coordPort *int = flag.Int("cport", 7097, "Coordinator port. Defaults to 7097.")
var nShards *int = flag.Int("nshrds", 1, "Number of shards. Defaults to 1.")
var myShardId *int = flag.Int("shardId", 0, "My shard id. Defaults to 0.")

type Master struct {
	N              int
	myaddr         string
	nodeList       []string
	addrList       []string
	portList       []int
	rpcPortList    []int
	lock           *sync.Mutex
	nodes          []*rpc.Client
	leader         []bool
	alive          []bool
	expectAddrList []string
	connected      []bool
	nConnected     int
	numShards      int
	shards         []string
	shardId		int32
}

func main() {
	flag.Parse()

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	ips := []string{}
	if *nodeIPs != "" {
		ips = strings.Split(*nodeIPs, ",")
		log.Println("Ordered replica ips:", ips, len(ips))
	} else {
		for i := 0; i < *numNodes; i++ {
			ips = append(ips, "")
		}
	}
	log.Println(ips, len(ips))
	master := &Master{
		*numNodes,
		*myAddr,
		make([]string, *numNodes),
		make([]string, *numNodes),
		make([]int, *numNodes),
		make([]int, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes),
		ips,
		make([]bool, *numNodes),
		0,
		-1,
		nil,
		int32(*myShardId),
	}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	// TODO: can use return value to obtain new leaders after failures?
	log.Println("Master is registering itself with the coordinator")
	registerWithCoordinator(fmt.Sprintf("%s:%d", *coordAddr, *coordPort))

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if master.nConnected == master.N {
			master.lock.Unlock()
			log.Println("All connected!", master.nodeList) //TODO delete
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i])
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d: %v\n", i, err)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

	// send the leader to the Coordinator
	sendLeaderToCoord(fmt.Sprintf("%s:%d", *coordAddr, *coordPort), fmt.Sprintf("%s:%d", master.addrList[0], master.rpcPortList[0]))

	for true {
		time.Sleep(3000 * 1000 * 1000)
		new_leader := false
		for i, node := range master.nodes {
			err := node.Call("Replica.Ping", new(genericsmrproto.PingArgs), new(genericsmrproto.PingReply))
			if err != nil {
				//log.Printf("Replica %d has failed to reply\n", i)
				master.alive[i] = false
				if master.leader[i] {
					// neet to choose a new leader
					new_leader = true
					master.leader[i] = false
				}
			} else {
				master.alive[i] = true
			}
		}
		if !new_leader {
			continue
		}
		for i, new_master := range master.nodes {
			if master.alive[i] {
				err := new_master.Call("Replica.BeTheLeader", new(genericsmrproto.BeTheLeaderArgs), new(genericsmrproto.BeTheLeaderReply))
				if err == nil {
					master.leader[i] = true
					log.Printf("Replica %d is the new leader.", i)
					break
				}
			}
		}
	}
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)
	log.Println("Hi, master getting", addrPort, args.RpcPort)
	i := master.N + 1

	for index, ap := range master.nodeList {
		if ap == addrPort {
			i = index
			break
		}
	}

	if i == master.N+1 {
		for index, a := range master.expectAddrList {
			if args.Addr == a {
				i = index
				if !master.connected[i] {
					break
				}
			}
		}
	}

	if i == master.N+1 {
		log.Println("Received register from bad IP:", addrPort)
		return nil
	}

	if !master.connected[i] {
		master.nodeList[i] = addrPort
		master.addrList[i] = args.Addr
		master.portList[i] = args.Port
		master.rpcPortList[i] = args.RpcPort
		master.connected[i] = true
		master.nConnected++
	}

	if master.nConnected == master.N {
		reply.Ready = true
		reply.ReplicaId = i
		reply.NodeList = make([]string, master.N)
		for k, p := range master.rpcPortList {
			reply.NodeList[k] = fmt.Sprintf("%s:%d", master.addrList[k], p)
		}

	} else {
		reply.Ready = false
	}

	return nil
}

func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000)
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i, master.nodeList[i]}
			break
		}
	}
	return nil
}

func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if master.nConnected == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}

func registerWithCoordinator(coordAddr string) []string {
	args := &coordinatorproto.RegisterArgs{*myAddr, *portnum}
	var reply coordinatorproto.RegisterReply

	for done := false; !done; {
		log.Println("Trying coordinator: ", coordAddr)
		ccli, err := rpc.DialHTTP("tcp", coordAddr)
		if err == nil {
			err = ccli.Call("Coordinator.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	return reply.MasterList
}

func sendLeaderToCoord(coordAddr string, leader string) {
	log.Printf("Registering Leader %s with Coordinator", leader)
	args := &coordinatorproto.RegisterLeaderArgs{leader, fmt.Sprintf("%s:%d", *myAddr, *portnum)}
	var reply coordinatorproto.RegisterLeaderReply

	for true {
		ccli, err := rpc.DialHTTP("tcp", coordAddr)
		if err == nil {
			err = ccli.Call("Coordinator.RegisterLeader", args, &reply)
			if err == nil {
				break
			}
		}
		time.Sleep(1e9)
	}
}

// Coordinator --> Master: giving list of shards when available
func (master *Master) RegisterShards(args *masterproto.RegisterShardsArgs, reply *masterproto.RegisterShardsReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	master.shards = args.ShardList
	master.numShards = len(master.shards)
	for i, e := range master.shards {
		l := strings.Split(e, ":")
		p, err := strconv.Atoi(l[1])
		if err != nil {
			panic("Malformed port")
		}
		master.shards[i] = fmt.Sprintf("%s:%d", l[0], p)
		log.Printf("-->Shard %d has leader at %s\n", i, master.shards[i])
	}
	return nil
}

// Servers --> Master: asking for shards
func (master *Master) GetShardList(args *masterproto.GetShardListArgs, reply *masterproto.GetShardListReply) error {
	for true {
		master.lock.Lock()
		if master.shards != nil {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}

	reply.ShardList = master.shards
	reply.ShardId = master.shardId
	return nil
}

// Servers --> Master: notifying they've connnected to all shards
func (master *Master) ShardReady(args *masterproto.ShardReadyArgs, reply *masterproto.ShardReadyReply) error {
	log.Printf("Master got notified by leader it's ready!")
	margs := &coordinatorproto.ThisShardConnectedArgs{}
        var mreply coordinatorproto.ThisShardConnectedReply

        ccli, _ := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *coordAddr, *coordPort))
        ccli.Call("Coordinator.ThisShardConnected", margs, &mreply)
	return nil
}
