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
  "zipfgenerator"
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

var singleShardAware *bool = flag.Bool(
	"SSA",
	false,
	"Single shard awareness optimization. Defaults to false.")

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


type RetwisOp uint8
const (
        NONE RetwisOp = iota
	POST
)



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
			*statsFile, false, true, *singleShardAware)
	case "ss-mdl":
		return clients.NewSSMDLClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)
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
  zipf, err := zipfgenerator.NewZipfGenerator(r, 0, *numKeys, *zipfS, false)
  if err != nil {
    panic("problem making the zipfian generator :0")
  }
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

		//dlog.Printf("Client %v about to issue AppRequest at time %v\n", *clientId, time.Now().UnixMilli())
		global_timeline := int64(zipf.Uint64())
		next_post_id := int64(zipf.Uint64())
		post_id := int64(0)
		before := time.Now()
		PostTransformed(post_id, global_timeline, next_post_id, client, zipf)
		after := time.Now()
		post_id++
                //dlog.Printf("!!!!Paxos APP level write took %d microseconds\n", int64(after.Sub(before).Microseconds()))

		opString := "app"
		count++
		dlog.Printf("AppRequests attempted: %d\n", count)
		//dlog.Printf("AppRequests attempted: %d at time %d\n", count, time.Now().UnixMilli())

		currInt := int(currRuntime.Seconds())
		if *rampUp <= currInt && currInt < *expLength-*rampDown {
			lat := int64(after.Sub(before).Nanoseconds())
			fmt.Printf("%s,%d,%d,%d\n", opString, lat, 0, count)

		}
		now = time.Now()
		currRuntime = now.Sub(start)
	}
	log.Printf("Total AppRequests attempted: %d, total system level requests: %d\n", count, count*int32(*fanout))
	log.Printf("Experiment over after %f seconds\n", currRuntime.Seconds())
	client.Finish()
}



//***********************************************************//
//********************** Retwis POST ************************//
//***********************************************************//

/*
$postid = $r->incr("next_post_id");
$status = str_replace("\n"," ",gt("status"));
$r->hmset("post:$postid","user_id",$User['id'],"time",time(),"body",$status);
$followers = $r->zrange("followers:".$User['id'],0,-1);
$followers[] = $User['id']; // Add the post to our own posts too

foreach($followers as $fid) {
    $r->lpush("posts:$fid",$postid);
}
# Push the post on the timeline, and trim the timeline to the
# newest 1000 elements.
$r->lpush("timeline",$postid);
$r->ltrim("timeline",0,1000);
*/

// next_post_id is always the same key when POST is called from main
// post_id is unique every time the client calls POST from main
// user_id is zipfian
// followers are uniformly random
// timeline is always the same key when POST is called from main

func PostSequential(post_id int64, timeline int64, next_post_id int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	var opTypes []state.Operation
	var keys []int64
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// $postid = $r->incr("next_post_id");
	client.AppRequest([]state.Operation{state.CAS}, []int64{next_post_id})

	// $r->hmset("post:$postid","user_id",$User['id'],"time",time(),"body",$status);
	client.AppRequest([]state.Operation{state.PUT}, []int64{post_id})

	// $followers = $r->zrange("followers:".$User['id'],0,-1);
	client.AppRequest([]state.Operation{state.GET}, []int64{int64(zipf.Uint64())})
	followers := int64(zipf.Uint64()) % 100

	/* 
	foreach($followers as $fid) {
		$r->lpush("posts:$fid",$postid);
	}*/
	keys = nil
	opTypes = nil
	for i := int64(0); i < followers; i++ {
		keys = append(keys, int64(r.Uint64()))
		opTypes = append(opTypes, state.PUT)
	}
	client.AppRequest(opTypes, keys)

	// $r->lpush("timeline",$postid);
	client.AppRequest([]state.Operation{state.PUT}, []int64{timeline})
	//$r->ltrim("timeline",0,1000);
	client.AppRequest([]state.Operation{state.CAS}, []int64{timeline})
}

func PostTransformed(post_id int64, timeline int64, next_post_id int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	var opTypes []state.Operation
	var keys []int64
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// $postid = $r->incr("next_post_id");
	client.AppRequest([]state.Operation{state.CAS}, []int64{next_post_id})

	// $r->hmset("post:$postid","user_id",$User['id'],"time",time(),"body",$status);
	// $followers = $r->zrange("followers:".$User['id'],0,-1);
	client.AppRequest([]state.Operation{state.PUT, state.GET}, []int64{post_id, int64(zipf.Uint64())})
	followers := int64(zipf.Uint64()) % 100

	/* 
	foreach($followers as $fid) {
		$r->lpush("posts:$fid",$postid);
	}*/
	keys = nil
	opTypes = nil
	for i := int64(0); i < followers; i++ {
		keys = append(keys, int64(r.Uint64()))
		opTypes = append(opTypes, state.PUT)
	}
	client.AppRequest(opTypes, keys)

	// $r->lpush("timeline",$postid);
	//$r->ltrim("timeline",0,1000);
	client.AppRequest([]state.Operation{state.PUT, state.CAS}, []int64{timeline, timeline})
}

func PostNaive(post_id int64, timeline int64, next_post_id int64, client clients.Client, zipf *zipfgenerator.ZipfGenerator) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	followers := int64(zipf.Uint64()) % 100
	total := 5+followers
	done := make([]int, total)
	// $postid = $r->incr("next_post_id");
	go func(i int) {
		client.AppRequest([]state.Operation{state.CAS}, []int64{next_post_id})
		done[i] = 1
	}(0)

	// $r->hmset("post:$postid","user_id",$User['id'],"time",time(),"body",$status);
	go func(i int) {
		client.AppRequest([]state.Operation{state.PUT}, []int64{post_id})
		done[i] = 1
	}(1)

	// $followers = $r->zrange("followers:".$User['id'],0,-1);
	go func(i int) {
		client.AppRequest([]state.Operation{state.GET}, []int64{int64(zipf.Uint64())})
		done[i] = 1
	}(2)

	/* 
	foreach($followers as $fid) {
		$r->lpush("posts:$fid",$postid);
	}*/
	for i := int64(0); i < followers; i++ {
		func(j int64) {
			go client.AppRequest([]state.Operation{state.PUT}, []int64{int64(r.Uint64())})
			done[j] = 1
		}(3 + i)
	}

	// $r->lpush("timeline",$postid);
	go func(i int64) {
		client.AppRequest([]state.Operation{state.PUT}, []int64{timeline})
		done[i] = 1
	}(3+followers)
	//$r->ltrim("timeline",0,1000);
	go func(i int64) {
		client.AppRequest([]state.Operation{state.CAS}, []int64{timeline})
		done[i] = 1
	}(4+followers)

	complete := false
	for !complete {
		complete = true
		for i :=int64(0); i < total; i++ {
			if done[i] == 0 {
				complete = false
				break
			}
		}
	}
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuProfile != "" {
		pprof.StopCPUProfile()
	}
	log.Printf("Caught signal and stopped CPU profile before exit.\n")
	os.Exit(0)
}
