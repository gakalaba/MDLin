package main

import (
	"epaxos"
	"flag"
	"fmt"
	"log"
	"masterproto"
	"mdlin"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"paxos"
	"runtime"
	"runtime/pprof"
	"time"
  "state"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var doEpaxos *bool = flag.Bool("e", false, "Use EPaxos as the replication protocol. Defaults to false.")
var doMDLin *bool = flag.Bool("mdl", false, "Use Multi-dispatch Linearizability instead of Single-dispatch Linearizability. Defaults to true.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var batch = flag.Bool("batch", false, "Enables batching of inter-server messages")
var infiniteFix = flag.Bool("inffix", false, "Enables a bound on execution latency for EPaxos")
var clockSyncType = flag.Int("clocksync", 0, "0 to not sync clocks, 1 to delay the opening of messages until the quorum, 2 to delay so that all process at same time, 3 to delay to CA, VA, and OR.")
var clockSyncEpsilon = flag.Float64("clockepsilon", 4, "The number of milliseconds to add as buffer for OpenAfter times.")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))

  rpc.HandleHTTP()
  //listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

  go run(replicaId, nodeList)
	http.Serve(l, nil)
}

func run(replicaId int, nodeList []string) {
	if *doEpaxos {
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *beacon,
			*durable, *batch, *infiniteFix, epaxos.ClockSyncType(*clockSyncType),
			int64(*clockSyncEpsilon*1e6) /* ms to ns */)
		rpc.Register(rep)
	} else if *doMDLin {
		log.Println("Starting MD Linearizability replica...")
    // Get the shards for multi-sharded MD-Lin
    shards, keyspace := getShardsFromMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
    shardId := -1
    if (shards != nil) {
      for i,e := range shards {
        if e == fmt.Sprintf("%s:%d", *myAddr, *portnum+100) {
          shardId = i
        }
        log.Printf("-->Shard %d leader at %s", i, e)
      }
    }
		rep := mdlin.NewReplica(replicaId, nodeList, shards, shardId, keyspace, *thrifty, *durable, *batch)
		rpc.Register(rep)
	} else {
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *durable, *batch)
		rpc.Register(rep)
	}
}

func registerWithMaster(masterAddr string) (int, []string) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
	var reply masterproto.RegisterReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
		}
		time.Sleep(1e9)
	}

	return reply.ReplicaId, reply.NodeList
}

func getShardsFromMaster(masterAddr string) ([]string, map[int][2]state.Key) {
	var args masterproto.GetShardListArgs
	var reply masterproto.GetShardListReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.GetShardList", &args, &reply)
			if err == nil {
				done = true
				break
			}
		}
	}
	return reply.ShardList, reply.Keyspace
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
