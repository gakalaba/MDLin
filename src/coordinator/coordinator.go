package main

import (
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
  "coordinatorproto"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

var portnum *int = flag.Int("port", 7097, "Port # to listen on. Defaults to 7097")
var nShards *int = flag.Int("N", 2, "Number of shards. Defaults to 2.")
var masterIPs *string = flag.String("ips", "", "Space separated list of master IP addresses (ordered).")

type Coordinator struct {
	numShards              int
	masterList       []string
	addrList       []string
	portList       []int
	lock           *sync.Mutex
	masters          []*rpc.Client
  shardLeaders []*rpc.Client
	leader         []bool
	alive          []bool
	expectAddrList []string
	connected      []bool
	nConnected     int
}

func main() {
	flag.Parse()

	log.Printf("Coordinator starting on port %d\n", *portnum)
	log.Printf("...waiting for %d shards\n", *numShards)

	ips := []string{}
	if *masterIPs != "" {
		ips = strings.Split(*masterIPs, ",")
		log.Println("Ordered master ips:", ips, len(ips))
	} else {
    for i := 0; i < *numShards; i++ {
	    ips = append(ips, "")
	  }
  }
  log.Println(ips, len(ips))
	master := &Master{
		*numNodes,
		make([]string, *numNodes),
		make([]string, *numNodes),
		make([]int, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes),
		ips,
		make([]bool, *numNodes),
		0,
	}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if master.nConnected == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d: %v\n", i, err)
		}
		master.leader[i] = false
	}
	master.leader[0] = true

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

	i := master.N + 1

	log.Println("Received Register", addrPort, master.nodeList)

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

	log.Println("Ended up with index", i)

	if !master.connected[i] {
		master.nodeList[i] = addrPort
		master.addrList[i] = args.Addr
		master.portList[i] = args.Port
		master.connected[i] = true
		master.nConnected++
	}

	if master.nConnected == master.N {
		log.Println("All connected!")
		reply.Ready = true
		reply.ReplicaId = i
		reply.NodeList = master.nodeList
	} else {
		reply.Ready = false
	}

	return nil
}

func (coordinator *Coordinator) GetShardLeaderList(args *coordinatorproto.GetShardLeaderListArgs, reply *coordinatorproto.GetShardLeaderListReply) error {
  coordinator.lock.Lock()
  defer coordinator.lock.Unlock()

  if coordinator.nConnected == coordinator.numShards {
    for e, i := range coordinator.masterList {
      reply := new(masterproto.GetLeaderReply)
      if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
        log.Fatalf("Error making the GetLeader RPC\n")
      }
      coordinator.shardList[i] = reply.LeaderAddr
    }
    reply.Ready = true
  return master.nodeList
}
