package main

import (
	"flag"
	"fmt"
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
  shardLeaders []string
	alive          []bool
	expectAddrList []string
	connected      []bool
	nConnected     int
  leadersConnected int
}

func main() {
	flag.Parse()

	log.Printf("Coordinator starting on port %d\n", *portnum)
	log.Printf("...waiting for %d shards\n", *nShards)

	ips := []string{}
	if *masterIPs != "" {
		ips = strings.Split(*masterIPs, ",")
		log.Println("Ordered master ips:", ips, len(ips))
	} else {
    for i := 0; i < *nShards; i++ {
	    ips = append(ips, "")
	  }
  }
  log.Println(ips, len(ips))
	coordinator := &Coordinator{
		*nShards,
		make([]string, *nShards),
		make([]string, *nShards),
		make([]int, *nShards),
		new(sync.Mutex),
		make([]*rpc.Client, *nShards),
		make([]string, *nShards),
		make([]bool, *nShards),
		ips,
		make([]bool, *nShards),
		0,
    0,
	}

	rpc.Register(coordinator)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Coordinator listen error:", err)
	}

	go coordinator.run()

	http.Serve(l, nil)
}

func (coordinator *Coordinator) run() {
	for true {
		coordinator.lock.Lock()
		if coordinator.nConnected == coordinator.numShards {
			coordinator.lock.Unlock()
			break
		}
		coordinator.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(2000000000)

	// connect to master servers
	for i := 0; i < coordinator.numShards; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", coordinator.addrList[i], coordinator.portList[i]+1000)
		coordinator.masters[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to shard %d: %v\n", i, err)
      coordinator.alive[i] = false
		} else {
		  coordinator.alive[i] = false
    }
	}

  // Give the masters the ips of all other shard masters/leaders
  for true {
		coordinator.lock.Lock()
		if coordinator.leadersConnected == coordinator.numShards {
			coordinator.lock.Unlock()
			break
		}
		coordinator.lock.Unlock()
		time.Sleep(100000000)
	}
  coordinator.sendShardsToMasters()

	for true {
		time.Sleep(3000 * 1000 * 1000)
    //TODO can add something for handling leader failure
  }
}

func (coordinator *Coordinator) Register(args *coordinatorproto.RegisterArgs, reply *coordinatorproto.RegisterReply) error {
	coordinator.lock.Lock()
	defer coordinator.lock.Unlock()

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	i := coordinator.numShards + 1

	log.Println("Received Register", addrPort, coordinator.masterList)

	for index, ap := range coordinator.masterList {
		if ap == addrPort {
			i = index
			break
		}
	}

	if i == coordinator.numShards+1 {
		for index, a := range coordinator.expectAddrList {
			if args.Addr == a {
				i = index
				if !coordinator.connected[i] {
					break
				}
			}
		}
	}

	if i == coordinator.numShards+1 {
		log.Println("Received register from bad Master IP:", addrPort)
		return nil
	}

	log.Println("Ended up with index", i)

	if !coordinator.connected[i] {
		coordinator.masterList[i] = addrPort
		coordinator.addrList[i] = args.Addr
		coordinator.portList[i] = args.Port
		coordinator.connected[i] = true
		coordinator.nConnected++
	}

	if coordinator.nConnected == coordinator.numShards {
		log.Println("All connected!")
		reply.Ready = true
		reply.MasterList = coordinator.masterList
	} else {
		reply.Ready = false
	}

	return nil
}

func (coordinator *Coordinator) RegisterLeader(args *coordinatorproto.RegisterLeaderArgs, reply *coordinatorproto.RegisterLeaderReply) error {
  coordinator.lock.Lock()
  defer coordinator.lock.Unlock()
  for i:=0;i<coordinator.numShards;i++ {
    if coordinator.masterList[i] == args.MasterAddr {
      if coordinator.shardLeaders[i] == "" {
        coordinator.leadersConnected++
      }
      coordinator.shardLeaders[i] = args.LeaderAddr
    }
  }
  return nil
}

func (coordinator *Coordinator) sendShardsToMasters() error {
  coordinator.lock.Lock()
  defer coordinator.lock.Unlock()

  var reply masterproto.RegisterShardsReply
  for _, mcli := range coordinator.masters {
    args := &masterproto.RegisterShardsArgs{coordinator.shardLeaders}
    if err := mcli.Call("Master.RegisterShards", args, &reply); err != nil {
      log.Fatalf("Error making the RegisterShards RPC\n")
    }
  }
  return nil
}

func (coordinator *Coordinator) GetShardLeaderList(args *coordinatorproto.GetShardLeaderListArgs, reply *coordinatorproto.GetShardLeaderListReply) error {
  for true {
    coordinator.lock.Lock()
    if coordinator.leadersConnected == coordinator.numShards {
      coordinator.lock.Unlock()
      break
    }
    coordinator.lock.Unlock()
    time.Sleep(100000000)
  }

  reply.LeaderList = coordinator.shardLeaders
  return nil
}
