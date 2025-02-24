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
	"runtime/debug"
	"runtime/pprof"
	"state"
	"time"
	"strconv"
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

var dbg *bool = flag.Bool(
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
	1,
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

func createClient() clients.Client {
	switch *replProtocol {
	/*case "abd":
		return clients.NewAbdClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular)
	case "gryff":
		return clients.NewGryffClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, *regular, *sequential, *proxy, *thrifty, *defaultReplicaOrder,
			*epaxosMode)
	case "epaxos":
		return clients.NewProposeClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true)*/
	case "mdl":
		return clients.NewAsynchClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, true, *singleShardAware)
	/*case "ss-mdl":
		return clients.NewSSMDLClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
			*statsFile, false, false)*/
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

	dlog.DLOG = *dbg

	if *conflicts >= 0 {
		dlog.Println("Using uniform distribution")
	} else {
		dlog.Println("Using zipfian distribution")
	}

	if *writes+*reads+*rmws != 1000 {
		log.Fatalf("Writes (%d), reads (%d), and rmws (%d) must add up to 1000.\n", *writes, *reads, *rmws)
	}

	runtime.GOMAXPROCS(2)
	debug.SetGCPercent(-1)
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
//   zipf, err := zipfgenerator.NewZipfGenerator(r, 0, *numKeys, *zipfS, false)
//   if err != nil {
//     panic("problem making the zipfian generator :0")
//   }
	var count int32
	count = 0

	go func(client clients.Client) {
		time.Sleep(time.Duration(*expLength+1) * time.Second)
		client.Finish()
	}(client)

	start := time.Now()
	now := start
	currRuntime := now.Sub(start)

	// pending_awaits := make([]state.Value, *fanout)
	// pending_success := make([]bool, *fanout)
	for int(currRuntime.Seconds()) < *expLength {
		//if *randSleep > 0 {
		time.Sleep(time.Duration(r.Intn(1e6))) // randSleep ms
		//}

		before := time.Now()
		for i := 0; i < *fanout; i++ {
			// opTypeRoll := r.Intn(1000)
			// var opType state.Operation
			// var k int64
			// if opTypeRoll < *reads {
			// 	opType = state.GET
			// } else if opTypeRoll < *reads+*writes {
			// 	opType = state.PUT
			// } else {
			// 	opType = state.CAS
			// }

			// if *conflicts >= 0 {
			// 	if r.Intn(*conflictsDenom) < *conflicts {
			// 		k = 0
			// 	} else {
			// 		k = (int64(count) << 32) | int64(*clientId)
			// 	}
			// } else {
			// 	k = int64(zipf.Uint64())
			// 	//k = int64(r.Intn(int(*numKeys)))
			// }
			if *replProtocol == "mdl" {
				createPrivateRoomMdl(client, 12345, 1234)
				// addMessageMDL(client, int64(1), string("1"), "Hello", int64(0))
				// _, future := client.AppRequest([]state.Operation{opType}, []int64{k},  nil, nil)
				// pending_awaits[i] = future
			} else {
				// var valueObj, oldValueObj state.Value
				// MPsuccess, _ := client.AppRequest([]state.Operation{opType}, []int64{k}, []state.Value{valueObj}, []state.Value{oldValueObj})
				// pending_success[i] = MPsuccess
				createPrivateRoomSync(client, 12345, 1234)
				//addMessageSync(client, int64(1), string("1"), "Hello", int64(0))
			}
		}

		// success := true

		// if *replProtocol == "mdl" {
		// 	for _, future := range pending_awaits {
		// 		fut, _ := strconv.Atoi(future.String)
		// 		_, val := client.AppResponse(int32(fut))
		// 		if val != 1 {
		// 			success = false
		// 			break
		// 		}
		// 	}
		// } else {
		// 	for _, MPsucc := range pending_success {
		// 		if MPsucc != true {
		// 			success = false
		// 			break
		// 		}
		// 	}
		// }

		after := time.Now()
                //dlog.Printf("!!!!Paxos APP level write took %d microseconds\n", int64(after.Sub(before).Microseconds()))

		opString := "app"
		// if !success {
		// 	log.Printf("Failed %s(%d).\n", opString, count)
		// 	panic("why is an AppRequest getting failed response")
		// }
		count++
		dlog.Printf("AppRequests attempted: %d\n", count)
		//dlog.Printf("AppRequests attempted: %d at time %d\n", count, time.Now().UnixMilli())

		currInt := int(currRuntime.Seconds())
		if *rampUp <= currInt && currInt < *expLength-*rampDown {
			lat := int64(after.Sub(before).Nanoseconds())
			fmt.Printf("%s,%d,%d,%d\n", opString, lat, 420, count)

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

func getPrivateRoomID(user1 int64, user2 int64) (string, error) {
	minUserID, maxUserID := user1, user2
	if user1 > user2 {
		minUserID, maxUserID = user2, user1
	}
	return fmt.Sprintf("%s:%s", minUserID, maxUserID), nil
}

func createPrivateRoomMdl(client clients.Client, user1 int64, user2 int64) (map[string]interface{}, bool) {
	pending_awaits := make([]int32, 2)
	var oldValue state.Value
	roomID, _ := getPrivateRoomID(user1, user2)
	newValue := state.Value{String: roomID}
	
	_ , future := client.AppRequest([]state.Operation{state.SADD}, []int64{user1}, []state.Value{newValue}, []state.Value{oldValue})
	futureValue := future.String
	futureInt, _ := strconv.Atoi(futureValue)
	pending_awaits[0] = int32(futureInt)

	_ , future = client.AppRequest([]state.Operation{state.SADD}, []int64{user2}, []state.Value{newValue}, []state.Value{oldValue})
	futureValue = future.String
	futureInt, _ = strconv.Atoi(futureValue)
	pending_awaits[1] = int32(futureInt)

	user2String := strconv.FormatInt(user1, 10)

	pending_awaits_hmget1, user1Data := hmget(client, user1, user2String)

	pending_awaits = append(pending_awaits_hmget1, pending_awaits...)
	
	pending_awaits_hmget2, user2Data := hmget(client, user1, user2String)
	
	pending_awaits = append(pending_awaits_hmget2, pending_awaits...)

	for _, future := range pending_awaits {
		client.AppResponse(future)
	}	

	return map[string]interface{}{
		"id":    roomID,
		"names": []string{user1Data, user2Data},
	}, false
}

func createPrivateRoomSync(client clients.Client, user1, user2 int64) (map[string]interface{}, bool) {
	roomID, _ := getPrivateRoomID(user1, user2)

	newValue := state.Value{String: roomID}
	var oldValue state.Value

	client.AppRequest([]state.Operation{state.SADD}, []int64{user1}, []state.Value{newValue}, []state.Value{oldValue})
	client.AppRequest([]state.Operation{state.SADD}, []int64{user2}, []state.Value{newValue}, []state.Value{oldValue})

	user2Str := strconv.FormatInt(user2, 10)

	user1Data := hmgetSync(client, user1, user2Str)
	user2Data := hmgetSync(client, user1, user2Str)

	return map[string]interface{}{
		"id":    roomID,
		"names": []string{user1Data, user2Data},
	}, false
}

func hmget(client clients.Client, key int64, key2 string) ([]int32, string) {
	value2 := []state.Value{
		{String: key2},
	}

	var oldValue state.Value
	pending_awaits := []int32{}

	// Send request
	_, future := client.AppRequest([]state.Operation{state.HMGET}, []int64{key}, value2, []state.Value{oldValue})
	futureValue := future.String
	// Convert string to integer
	futureInt, err := strconv.Atoi(futureValue)
	if err != nil {
		// Handle the error (return empty slice and error message)
		return nil, "Error converting value"
	}

	// Process response
	result, _ := client.AppResponse(int32(futureInt))
	resultStr := result.String
	return pending_awaits, resultStr
}

func hmgetSync(client clients.Client, key int64, key2 string) string {
	value2 := []state.Value{
		{String: key2},
	}
	var oldValue state.Value

	_, res := client.AppRequest([]state.Operation{state.HMGET}, []int64{key}, value2, []state.Value{oldValue})
	resStr := res.String
	return resStr
}


func addMessageMDL(client clients.Client, roomID int64, fromID string, content string, timestamp int64) {
    value := state.Value{String: string("hello")}
	pending_awaits := make([]int32, 4)
	// Perform 4 AppRequests
	for i := 0; i < 4; i++ {
		_, future := client.AppRequest([]state.Operation{state.ZADD}, []int64{roomID}, []state.Value{value}, nil)
		futureValue := future.String
		futureInt, err := strconv.Atoi(futureValue)
		if err != nil {
			log.Fatalf("Failed to convert future to int: %v", err)
		}
		pending_awaits = append(pending_awaits, int32(futureInt))
	}
	// Wait for all operations to complete
	for _, futureAwait := range pending_awaits {
		_, val := client.AppResponse(futureAwait)
		if val != 1 {
			log.Fatalf("App Response failed")
		}
	}
}


func addMessageSync(client clients.Client, roomID int64, fromID string, content string, timestamp int64) {
    // Create a state.Value containing the JSON string
    value := state.Value{String: string("Hello")}
	var oldValueObj state.Value
	for i := 0; i < 4; i++ {
		MPsuccess, _ := client.AppRequest([]state.Operation{state.ZADD}, []int64{roomID}, []state.Value{value}, []state.Value{oldValueObj})
		if MPsuccess != true {
			log.Fatal("MPsucc is false, terminating the program")
		}
	}
}

