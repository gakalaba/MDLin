package main

import (
	"clients"
	"dlog"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"state"
	"time"
)

var clientId *int = flag.Int(
	"clientId",
	0,
	"Client identifier for use in replication protocols.")

var conflicts *int = flag.Int(
	"conflicts",
	-1,
	"Percentage of conflicts. If < 0, a zipfian distribution will be used for "+
		"choosing keys.")

var conflictsDenom *int = flag.Int(
	"conflictsDenom",
	100,
	"Denominator of conflict fraction when conflicts >= 0.")

var cpuProfile *string = flag.String(
	"cpuProfile",
	"",
	"Name of file for CPU profile. If empty, no profile is created.")

var debug *bool = flag.Bool(
	"debug",
	true,
	"Enable debug output.")

var defaultReplicaOrder *bool = flag.Bool(
	"defaultReplicaOrder",
	false,
	"Use default replica order for Gryff coordination.")

var epaxosMode *bool = flag.Bool(
	"epaxosMode",
	false,
	"Run Gryff with same message pattern as EPaxos.")

var expLength *int = flag.Int(
	"expLength",
	30,
	"Length of the timed experiment (in seconds).")

var fastPaxos *bool = flag.Bool(
	"fastPaxos",
	false,
	"Send message directly to all replicas a la Fast Paxos.")

var forceLeader *int = flag.Int(
	"forceLeader",
	-1,
	"Replica ID to which leader-based operations will be sent. If < 0, an "+
		"appropriate leader is chosen by default.")

var coordinatorAddr *string = flag.String(
	"caddr",
	"",
	"Coordinator address.")

var coordinatorPort *int = flag.Int(
	"cport",
	7097,
	"Coordinator port.")

var maxProcessors *int = flag.Int(
	"maxProcessors",
	2,
	"GOMAXPROCS. Defaults to 2")

var numKeys *uint64 = flag.Uint64(
	"numKeys",
	10000,
	"Number of keys in simulated store.")

var proxy *bool = flag.Bool(
	"proxy",
	false,
	"Proxy writes at local replica.")

var rampDown *int = flag.Int(
	"rampDown",
	5,
	"Length of the cool-down period after statistics are measured (in seconds).")

var rampUp *int = flag.Int(
	"rampUp",
	5,
	"Length of the warm-up period before statistics are measured (in seconds).")

var randSleep *int = flag.Int(
	"randSleep",
	0,
	"Max number of milliseconds to sleep after operation completed.")

var randomLeader *bool = flag.Bool(
	"randomLeader",
	false,
	"Egalitarian (no leader).")

var reads *int = flag.Int(
	"reads",
	0,
	"Percentage of reads.")

var regular *bool = flag.Bool(
	"regular",
	false,
	"Perform operations with regular consistency. (only for applicable protocols)")

var replProtocol *string = flag.String(
	"replProtocol",
	"",
	"Replication protocol used by clients and servers.")

var rmws *int = flag.Int(
	"rmws",
	0,
	"Percentage of rmws.")

var sequential *bool = flag.Bool(
	"sequential",
	true,
	"Perform operations with sequential consistency. "+
		"(only for applicable protocols")

var statsFile *string = flag.String(
	"statsFile",
	"",
	"Export location for collected statistics. If empty, no file file is written.")

var fanout *int = flag.Int(
	"fanout",
	1,
	"Fanout. Defaults to 1.")

var thrifty *bool = flag.Bool(
	"thrifty",
	false,
	"Only initially send messages to nearest quorum of replicas.")

var writes *int = flag.Int(
	"writes",
	1000,
	"Percentage of updates (writes).")

var zipfS = flag.Float64(
	"zipfS",
	2,
	"Zipfian s parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")

var zipfV = flag.Float64(
	"zipfV",
	1,
	"Zipfian v parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")

func createClient() clients.Client {
	switch *replProtocol {
	case "abd":
		return clients.NewAbdClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular)
	case "gryff":
		return clients.NewGryffClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular, *sequential, *proxy, *thrifty, *defaultReplicaOrder,
			*epaxosMode)
	case "epaxos":
		return clients.NewProposeClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true)
	case "mdl":
		return clients.NewMDLClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true)
	default:
		return clients.NewProposeClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)
	}
}

func Max(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func main() {
	flag.Parse()

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	dlog.DLOG = *debug

	if *conflicts >= 0 {
		dlog.Println("Using uniform distribution")
	} else {
		dlog.Println("Using zipfian distribution")
	}

	if *writes+*reads+*rmws != 1000 {
		log.Fatalf("Writes (%d), reads (%d), and rmws (%d) must add up to 1000.\n", *writes, *reads, *rmws)
	}

	runtime.GOMAXPROCS(*maxProcessors)

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Error creating CPU profile file %s: %v\n", *cpuProfile, err)
		}
		pprof.StartCPUProfile(f)
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
		defer pprof.StopCPUProfile()
	}

	client := createClient()

	r := rand.New(rand.NewSource(int64(*clientId)))
	zipf := rand.NewZipf(r, *zipfS, *zipfV, uint64(*numKeys))

	var count int32
	count = 0

	go func(client clients.Client) {
		time.Sleep(time.Duration(*expLength+1) * time.Second)
		client.Finish()
	}(client)

	start := time.Now()
	now := start
	currRuntime := now.Sub(start)
	for int(currRuntime.Seconds()) < *expLength {
		if *randSleep > 0 {
			time.Sleep(time.Duration(r.Intn(*randSleep * 1e6))) // randSleep ms
		}

		var opTypes []state.Operation
		var k int64
		var keys []int64
		for i := 0; i < *fanout; i++ {
			opTypeRoll := r.Intn(1000)
			var opType state.Operation
			if opTypeRoll < *reads {
				opType = state.GET
			} else if opTypeRoll < *reads+*writes {
				opType = state.PUT
			} else {
				opType = state.CAS
			}
			opTypes = append(opTypes, opType)

			if *conflicts >= 0 {
				if r.Intn(*conflictsDenom) < *conflicts {
					k = 0
				} else {
					k = (int64(count) << 32) | int64(*clientId)
				}
			} else {
				k = int64(zipf.Uint64())
			}
			keys = append(keys, k)
		}

		var success bool

		before := time.Now()
		success, _ = client.AppRequest(opTypes, keys)
		after := time.Now()

		opString := "app"
		if !success {
			log.Printf("Failed %s(%d).\n", opString, count)
		}
		count++
		dlog.Printf("AppRequests attempted: %d\n", count)

		currInt := int(currRuntime.Seconds())
		if *rampUp < currInt && currInt < *expLength-*rampDown {
			lat := int64(after.Sub(before).Nanoseconds())
			fmt.Printf("%s,%d,%d,%d\n", opString, lat, k, count)

		}
		now = time.Now()
		currRuntime = now.Sub(start)
	}
	log.Printf("Total AppRequests attempted: %d, total system level requests: %d\n", count, count*int32(*fanout))
	log.Printf("Experiment over after %f seconds\n", currRuntime.Seconds())
	client.Finish()
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
	}
	log.Printf("Caught signal and stopped CPU profile before exit.\n")
	os.Exit(0)
}
