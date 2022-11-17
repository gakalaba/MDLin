package main

import (
	"bufio"
	"coordinatorproto"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"mdlinproto"
	"net"
	"net/rpc"
	"runtime"
	"state"
	"time"
)

// client -q 5 -r 5 -T 2

var coordAddr *string = flag.String("caddr", "", "Coordinator address. Defaults to localhost")
var coordPort *int = flag.Int("cport", 7097, "Coordinator port. Defaults to 7097.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var mdlin *bool = flag.Bool("mdl", false, "Multi-dispatch Linearizability: allow clients to issue multiple outstanding requests. Defaults to true")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var T = flag.Int("T", 1, "Number of threads (simulated clients).")
var check = flag.Bool("check", true, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", -1, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")

var N int

var successful []int
var failed []int

var rarray []int

func newResponseArray(f int) []int {
	rsp := make([]int, f)
	for i := 0; i < f; i++ {
		rsp[i] = -1
	}
	return rsp
}

func giveCoordKeys(ks map[int][2]int) {
  rkArgs := new(coordinatorproto.RegisterKeyspaceArgs)
  rkArgs.Keyspace = ks
  err = coordinator.Call("Coordinator.RegisterKeyspace", rkArgs, new(coordinatorproto.RegisterKeyspaceReply))
  if err != nil {
    log.Fatalf("Error making the GetShardLeaderList RPC: %v\n", err)
  }
  time.Sleep(5 * time.Second)
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	//////////////////////////////////////
	// Get info from coordinator
	//////////////////////////////////////
	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	log.Printf("Dialing coordinator at addr %s:%d\n", *coordAddr,
		*coordPort)
	coordinator, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *coordAddr, *coordPort))
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

	////////////////////////////////////////
	// Create buffers for all servers that client reads from
	/////////////////////////////////////////
	N = len(llReply.LeaderList)
	shard_leaders := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	for i := 0; i < N; i++ {
		var err error
		shard_leaders[i], err = net.Dial("tcp", llReply.LeaderList[i]) //place the tcp connection object inside shard_leaders[i]
		if err != nil {
			log.Printf("Error connecting to leader %d: %v\n", i, err)
		}
		readers[i] = bufio.NewReader(shard_leaders[i])
		writers[i] = bufio.NewWriter(shard_leaders[i])
	}

	successful = make([]int, N)
	failed = make([]int, N)
	done := make(chan bool, N)
	////////////////////////////////////////////////
	// Sending the requests and waiting for replies
	////////////////////////////////////////////////

  rsp := newResponseArray(4)
  test1(readers, writers, done, rsp)
  //rsp := newResponseArray(6)
	//test2(readers, writers, leader, done, rsp)

  ////////////////////////////////////////////////
  // Close Connections
  ////////////////////////////////////////////////
  for _, client := range shard_leaders {
		if client != nil {
			client.Close()
		}
	}
	coordinator.Close()
}

/*
func test2(readers []*bufio.Reader, writers []*bufio.Writer, leader int, done chan bool, rsp []int) {
  before_total := time.Now()
	if *mdlin {
		for shard := 0; shard < N; shard++ {
			go waitRepliesMDL(readers, shard, &rsp, done)
		}
		var arg mdlinproto.Propose
		before := time.Now()

		// Send C1 reqs 2 and 1
		leader := 0
		arg = mdlinproto.Propose{0, state.Command{state.PUT, 0, 0}, 0, 0, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 0}, 0, 1, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// Send C2 reqs 2 and 1
		leader = 1
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 0, 0}, 0, 0, 1}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		arg = mdlinproto.Propose{3, state.Command{state.PUT, 0, 0}, 0, 1, 1}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// Send C1 req 0
		leader = 0
		arg = mdlinproto.Propose{4, state.Command{state.PUT, 0, 0}, 0, 2, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// Send C2 req 0
		leader = 1
		//time.Sleep(2*time.Second)
		arg = mdlinproto.Propose{5, state.Command{state.PUT, 0, 0}, 0, 2, 1}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
		after := time.Now()

		log.Printf("Round took %v\n", after.Sub(before))

		/////////////////////////////////////
		// Do some checks on the results
		///////////////////////////////////
		if *check {
			for j := 0; j < 6; j++ {
				if rsp[j] == -1 {
					log.Println("Didn't receive", j)
				}
			}
		}

			//if err {
			//	reply := new(masterproto.GetLeaderReply)
			//	master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
			//	leader = reply.LeaderId // Cannot contact leader, figure out new leader
			//	log.Printf("New leader is replica %d\n", leader)
			//}

		after_total := time.Now()
		log.Printf("Test took %v\n", after_total.Sub(before_total))
	} else {
		log.Println("Paxos....")
		for shard := 0; shard < N; shard++ {
			go waitReplies(readers, shard, &rsp, done)
		}
		var arg genericsmrproto.Propose
		before := time.Now()

		// Send C1 reqs 2 and 1
		leader := 0
		arg = genericsmrproto.Propose{0, state.Command{state.PUT, 0, 0}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		arg = genericsmrproto.Propose{1, state.Command{state.PUT, 0, 0}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		<-done //SINGLE DISPATCH
		// Send C2 reqs 2 and 1
		leader = 1
		arg = genericsmrproto.Propose{2, state.Command{state.PUT, 0, 0}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		arg = genericsmrproto.Propose{3, state.Command{state.PUT, 0, 0}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		<-done //SINGLE DISPATCH
		// Send C1 req 0
		leader = 0
		arg = genericsmrproto.Propose{4, state.Command{state.PUT, 0, 0}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// Send C2 req 0
		leader = 1
		//time.Sleep(2*time.Second)
		arg = genericsmrproto.Propose{5, state.Command{state.PUT, 0, 0}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done

		after := time.Now()

		log.Printf("Round took %v\n", after.Sub(before))

		/////////////////////////////////////
		// Do some checks on the results
		///////////////////////////////////
		if *check {
			for j := 0; j < 6; j++ {
				if rsp[j] == -1 {
					log.Println("Didn't receive", j)
				}
			}
		}

			//if err {
			//	reply := new(masterproto.GetLeaderReply)
			//	master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
			//	leader = reply.LeaderId // Cannot contact leader, figure out new leader
			//	log.Printf("New leader is replica %d\n", leader)
			//}

		after_total := time.Now()
		log.Printf("Test took %v\n", after_total.Sub(before_total))

	}

	succs := 0
	for _, succ := range successful {
		succs += succ
	}

	fails := 0
	for _, fai := range failed {
		fails += fai
	}

	log.Printf("Successful: %d, Failed: %d\n", succs, fails)
}
*/
func test1(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp []int) {
  // Give the coordinator the keyspace!
  ks := make(map[int][2]int, 2)
  ks[0] = make(int, 2)
  ks[1] = make(int, 2)
  ks[0][0] = 0
  ks[0][1] = 0
  ks[1][0] = 1
  ks[1][1] = 1
  giveCoordKeys(ks)

  if *mdlin {
		for shard := 0; shard < len(readers); shard++ {
			go waitRepliesMDL(readers, shard, &rsp, done)
		}
		var arg mdlinproto.Propose
	  before_total := time.Now()
    // Single Client, issueing 4 commands:
    // W(X=1)
    //  W(Y=1)
    //   R(X)
    //    R(Y)

    // W(X=1)
		leader := 0
    deps := make([]mdlinproto.Tag, 0)
		arg = mdlinproto.Propose{0, state.Command{state.PUT, 0, 1}, 0, 0, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=1)
		leader = 1
    deps = append(deps, mdlinproto.Tag{0,0,100})
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 1, 1}, 0, 0, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(X)
		leader = 0
    deps = append(deps, mdlinproto.Tag{1,0,100})
		arg = mdlinproto.Propose{2, state.Command{state.GET, 0, 0}, 0, 1, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(Y)
		leader = 1
    deps = append(deps, mdlinproto.Tag{0,1,100})
		arg = mdlinproto.Propose{3, state.Command{state.GET, 1, 0}, 0, 1, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
		after_total := time.Now()

		/////////////////////////////////////
		// Do some checks on the results
		///////////////////////////////////
		if *check {
			for j := 0; j < len(rsp); j++ {
				if rsp[j] == -1 {
					log.Println("Didn't receive", j)
				}
			}
		}

			//if err {
			//	reply := new(masterproto.GetLeaderReply)
			//	master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
			//	leader = reply.LeaderId // Cannot contact leader, figure out new leader
			//	log.Printf("New leader is replica %d\n", leader)
			//}

		log.Printf("Test took %v\n", after_total.Sub(before_total))
	} else {
		log.Println("Paxos....")
		for shard := 0; shard < len(readers); shard++ {
			go waitReplies(readers, shard, &rsp, done)
		}
		var arg genericsmrproto.Propose
		before_total := time.Now()

    // Single Client, issueing 4 commands:
    // W(X=1)
    //  W(Y=1)
    //   R(X)
    //    R(Y)

    // W(X=1)
		leader := 0
		arg = genericsmrproto.Propose{0, state.Command{state.PUT, 0, 1}, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    <-done
		// W(Y=1)
		leader = 1
		arg = genericsmrproto.Propose{1, state.Command{state.PUT, 1, 1}, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    <-done
		// R(X)
		leader = 0
		arg = genericsmrproto.Propose{2, state.Command{state.GET, 0, 0}, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    <-done
		// R(Y)
		leader = 1
		arg = genericsmrproto.Propose{3, state.Command{state.GET, 1, 0}, 0}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    <-done
		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////

		after_total := time.Now()

		/////////////////////////////////////
		// Do some checks on the results
		///////////////////////////////////
		if *check {
			for j := 0; j < 6; j++ {
				if rsp[j] == -1 {
					log.Println("Didn't receive", j)
				}
			}
		}

			//if err {
			//	reply := new(masterproto.GetLeaderReply)
			//	master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
			//	leader = reply.LeaderId // Cannot contact leader, figure out new leader
			//	log.Printf("New leader is replica %d\n", leader)
			//}

		log.Printf("Test took %v\n", after_total.Sub(before_total))

	}

	succs := 0
	for _, succ := range successful {
		succs += succ
	}

	fails := 0
	for _, fai := range failed {
		fails += fai
	}

	log.Printf("Successful: %d, Failed: %d\n", succs, fails)
}

func waitRepliesMDL(readers []*bufio.Reader, shard int, rsp *[]int, done chan bool) {
	e := false

	reply := new(mdlinproto.ProposeReply)

	var err error
	var msgType byte
	n := len(*rsp)
	for i := 0; i < n; i++ {
		if msgType, err = readers[shard].ReadByte(); err != nil ||
			msgType != mdlinproto.PROPOSE_REPLY {
			log.Printf("Error when reading (op:%d): %v on shard %d", msgType, err, shard)
			e = true
			continue
		}
		if err = reply.Unmarshal(readers[shard]); err != nil {
			log.Printf("Error when reading on shard %d: %v", shard, err)
			e = true
			continue
		}

		log.Printf("Shard %d: Reply.OK = %d, CommandId = %d, PID = %d, Timestamp = %d", shard, reply.OK, reply.CommandId, reply.Value, reply.Timestamp)
		log.Printf("rsp len %d and commandID was %d", len(*rsp), reply.CommandId)
		if reply.OK == 0 {
			log.Printf("Client request failed on shard %d", shard)
			failed[shard]++
			continue
		}
		if (*rsp)[reply.CommandId] != -1 {
			log.Printf("Duplicate reply on shard %d: %d", shard, reply.CommandId)
			failed[shard]++
			continue
		}
		(*rsp)[reply.CommandId] = int(reply.CommandId)
		log.Printf("Success!")
		successful[shard]++
	}
	done <- e
}
func waitReplies(readers []*bufio.Reader, shard int, rsp *[]int, done chan bool) {
	e := false

	reply := new(genericsmrproto.ProposeReply)

	var err error
	n := len(*rsp)
	for i := 0; i < n; i++ {
		if err = reply.Unmarshal(readers[shard]); err != nil {
			log.Printf("Error when reading on shard %d:%v", shard, err)
			e = true
			continue
		}

		log.Printf("Shard %d: Reply.OK = %d, CommandId = %d, PID = %d, Timestamp = %d", shard, reply.OK, reply.CommandId, reply.Value, reply.Timestamp)
		log.Printf("rsp len %d and commandID was %d", len(*rsp), reply.CommandId)
		if reply.OK == 0 {
			log.Printf("Client request failed on shard %d", shard)
			failed[shard]++
			continue
		} else {
			if (*rsp)[reply.CommandId] != -1 {
				log.Printf("Duplicate reply on shard %d: %d", shard, reply.CommandId)
				failed[shard]++
				continue
			}
			(*rsp)[reply.CommandId] = int(reply.Value)
			log.Printf("Success!")
			successful[shard]++
		}
		done <- e
	}
}
