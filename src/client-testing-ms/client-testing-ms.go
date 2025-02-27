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

  runTests(readers, writers, done, coordinator)

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

func runTests(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, coord *rpc.Client) {
  var rsp []int
	before_total := time.Now()
  //CHANGE ME
  //test1(readers, writers, done, &rsp)
  //test2(readers, writers, done, &rsp)
  //testviii(readers, writers, done, &rsp)
  //testvi(readers, writers, done, &rsp)
  //testv(readers, writers, done, &rsp)
  //testi(readers, writers, done, &rsp)
  //testii(readers, writers, done, &rsp)
	//testiv(readers, writers, done, &rsp)
  //testiii(readers, writers, done, &rsp)
  testvii(readers, writers, done, &rsp)
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


func test1(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test1...")
  (*rsp) = newResponseArray(4) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // Single Client, issueing 4 commands:
    // W(X=1)
    //  W(Y=1)
    //   R(X)
    //    R(Y)

    // W(X=1)
		leader := 0
    deps := make([]mdlinproto.Tag, 0)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=1)
		leader = 1
    deps = append(deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(X)
		leader = 0
    deps = append(deps, mdlinproto.Tag{1,-1, 100, 2})
		arg = mdlinproto.Propose{3, state.Command{state.GET, 0, 0}, 0, 1, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(Y)
		leader = 1
    deps = append(deps, mdlinproto.Tag{0,-1, 100, 3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 1, 0}, 0, 1, 100, deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func test2(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test2...")
  (*rsp) = newResponseArray(4)
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 2 Clients, each issueing 2 commands:
    // C1     W(X=1)
    //          W(Y=1)
    // C2     R(Y)=1
    //          R(X)=0

    // R(X)
		leader := 0
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 0, 0}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(X=1)
		leader = 0
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 2}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=1)
		leader = 1
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 2}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(Y)
		leader = 1
		arg = mdlinproto.Propose{3, state.Command{state.GET, 1, 0}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done

	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testi(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test i...")
  (*rsp) = newResponseArray(6) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 3 Clients, each issuing 2 commands. 3rd arrives late:
    // C1:   W(X=1)
    //        W(Y=1)
    // C2:   R(Y)=1
    //        R(X)=0
		// C3:          W(X=2)
		//                W(Y=2)

    // R(X)
		leader := 0
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 0, 0}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=1)
		leader = 1
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(Y)
		arg = mdlinproto.Propose{3, state.Command{state.GET, 1, 0}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

    // Inject a delay...
		//time.Sleep(time.Millisecond*200)

		// W(X=2)
		leader = 0
		arg = mdlinproto.Propose{5, state.Command{state.PUT, 0, 2}, 0, 0, 300, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=2)
		leader = 1
		c3deps := make([]mdlinproto.Tag, 0)
    c3deps = append(c3deps, mdlinproto.Tag{0,-1, 300, 5})
		arg = mdlinproto.Propose{6, state.Command{state.PUT, 1, 2}, 0, 0, 300, c3deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testii(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test ii...")
  (*rsp) = newResponseArray(6) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 3 Clients, 2 conflicts detected (1 on each shard)
    // C1: 1  W(X=1)
    //     2   W(Y=1)
    // C2: 3  R1(Y)=1
    //     4   R2(X)=0
		// C3: 5  R3(X)=1
		//     6   R4(Y)=0

    // R2(X)
		leader := 0
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 0, 0}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R3(X)
		arg = mdlinproto.Propose{5, state.Command{state.GET, 0, 0}, 0, 0, 300, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// R4(Y)
		c3deps := make([]mdlinproto.Tag, 0)
    c3deps = append(c3deps, mdlinproto.Tag{0,-1,300,5})
		arg = mdlinproto.Propose{6, state.Command{state.GET, 1, 0}, 0, 0, 300, c3deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=1)
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R1(Y)
		arg = mdlinproto.Propose{3, state.Command{state.GET, 1, 0}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testiii(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test iii...")
  (*rsp) = newResponseArray(8) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 4 clients, each issues 2 ops (on each key). Basic conf test twice, interleaved:
    // C1: 1 W1(X=1)
		//		 2 	 W2(Y=1)
		// C2: 3 R1(Y)=1
		//		 4	 R2(X)=0
		// C3: 5 W3(X=2)
		//		 6	 W4(Y=2)
		// C4: 7 R3(Y)=2
		//		 8	 R4(X)=0

    // R4(X)
		leader := 0
    c4deps := make([]mdlinproto.Tag, 0)
    c4deps = append(c4deps, mdlinproto.Tag{1,-1,400,7})
		arg = mdlinproto.Propose{8, state.Command{state.GET, 0, -1}, 0, 0, 400, c4deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    // R2(X)
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 0, -1}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W3(X=2)
		arg = mdlinproto.Propose{5, state.Command{state.PUT, 0, 2}, 0, 0, 300, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W1(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W2(Y=1)
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R1(Y)
		arg = mdlinproto.Propose{3, state.Command{state.GET, 1, -1}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W4(Y=2)
    c3deps := make([]mdlinproto.Tag, 0)
    c3deps = append(c3deps, mdlinproto.Tag{0,-1, 300, 5})
		arg = mdlinproto.Propose{6, state.Command{state.PUT, 1, 2}, 0, 0, 300, c3deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R3(Y)
		arg = mdlinproto.Propose{7, state.Command{state.GET, 1, -1}, 0, 0, 400, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()


		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testiv(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test iv...")
  (*rsp) = newResponseArray(8) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 4 clients, each issues 2 ops (on each key). Basic conf test twice, BUT the one client is totally fine
    // C1: 1 W1(X=1)
		//		 2 	 W2(Y=1)
		// C2: 3 R1(Y)=1
		//		 4	 R2(X)=0
		// C3: 5 W3(X=2)
		//		 6	 W4(Y=2)
		// C4: 7 R3(Y)=2
		//		 8	 R4(X)=2

		leader := 0
		// R2(X)
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 0, -1}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W3(X=2)
		arg = mdlinproto.Propose{5, state.Command{state.PUT, 0, 2}, 0, 0, 300, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R4(X)
    c4deps := make([]mdlinproto.Tag, 0)
    c4deps = append(c4deps, mdlinproto.Tag{1,-1,400,7})
		arg = mdlinproto.Propose{8, state.Command{state.GET, 0, -1}, 0, 0, 400, c4deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W1(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W2(Y=1)
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R1(Y)
		arg = mdlinproto.Propose{3, state.Command{state.GET, 1, -1}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W4(Y=2)
    c3deps := make([]mdlinproto.Tag, 0)
    c3deps = append(c3deps, mdlinproto.Tag{0,-1, 300, 5})
		arg = mdlinproto.Propose{6, state.Command{state.PUT, 1, 2}, 0, 0, 300, c3deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R3(Y)
		arg = mdlinproto.Propose{7, state.Command{state.GET, 1, -1}, 0, 0, 400, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testv(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test v...")
  (*rsp) = newResponseArray(4) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 2 Clients, Basic conf test but on 4 completely different keys, should have NO conflicts:
    // C1:   W(X=1)
    //        W(Y=1)
    // C2:   W(B=1)
    //        W(A=1)

    // W(A=1)
		leader := 0
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{3,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.PUT, 2, 1}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W(Y=1)
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(B=1)
		arg = mdlinproto.Propose{3, state.Command{state.PUT, 3, 1}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testvi(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test vi...")
  (*rsp) = newResponseArray(4)
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 2 Clients, each issueing 2 commands, read swapped with read:
    // C1  1   R1(X)=0
    //     2     W(Y=1)
    // C2  3   W(Y=2)
    //     4     R2(X)=0

		leader := 0
    // R2(X)
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.GET, 0, 0}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R1(X)
		arg = mdlinproto.Propose{1, state.Command{state.GET, 0, 0}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W(Y=1)
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=2)
		arg = mdlinproto.Propose{3, state.Command{state.PUT, 1, 2}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done

	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testvii(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test vii...")
  (*rsp) = newResponseArray(8) // get rid of this for performance later
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 3 Clients, each issuing 2 commands. 3rd arrives late:
    // C1: 1  W(X=1)
    //     2   W(Y=1)
		//		 3		W(A=1)
		//		 4			W(B=1)
    // C2: 5  R(Y)=1
    //     6   R(X)=0
		// C3: 7  R(B)=1
		//     8   R(A)=0

		leader := 0
		// R(X)
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,5})
		arg = mdlinproto.Propose{6, state.Command{state.GET, 0, -1}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(A)
    c3deps := make([]mdlinproto.Tag, 0)
    c3deps = append(c3deps, mdlinproto.Tag{3,-1,300,7})
		arg = mdlinproto.Propose{8, state.Command{state.GET, 2, -1}, 0, 0, 300, c3deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W(Y=1)
		c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 0
		// W(A=1)
    c1deps = append(c1deps, mdlinproto.Tag{1,-1, 100, 2})
		arg = mdlinproto.Propose{3, state.Command{state.PUT, 2, 1}, 0, 1, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W(B=1)
    c1deps = append(c1deps, mdlinproto.Tag{2,-1, 100, 3})
		arg = mdlinproto.Propose{4, state.Command{state.PUT, 3, 1}, 0, 1, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(Y)
		arg = mdlinproto.Propose{5, state.Command{state.GET, 1, -1}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// R(B)
		arg = mdlinproto.Propose{7, state.Command{state.GET, 3, -1}, 0, 0, 300, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()


		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done
	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
}

func testviii(readers []*bufio.Reader, writers []*bufio.Writer, done chan bool, rsp *[]int) {
  log.Println("Starting test viii...")
  (*rsp) = newResponseArray(4)
  if *mdlin {
    log.Println("MDLin...")
    for shard := 0; shard < len(readers); shard++ {
      go waitRepliesMDL(readers, shard, rsp, done)
    }
		var arg mdlinproto.Propose
    // 2 Clients, basic write only conf det:
    // C1  1   W1(X=1)
    //     2     W2(Y=1)
    // C2  3   W3(Y=2)
    //     4     W4(X=2)

		leader := 0
    // W4(X=2)
    c2deps := make([]mdlinproto.Tag, 0)
    c2deps = append(c2deps, mdlinproto.Tag{1,-1,200,3})
		arg = mdlinproto.Propose{4, state.Command{state.PUT, 0, 2}, 0, 0, 200, c2deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W1(X=1)
		arg = mdlinproto.Propose{1, state.Command{state.PUT, 0, 1}, 0, 0, 100, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		leader = 1
		// W(Y=1)
    c1deps := make([]mdlinproto.Tag, 0)
    c1deps = append(c1deps, mdlinproto.Tag{0,-1, 100, 1})
		arg = mdlinproto.Propose{2, state.Command{state.PUT, 1, 1}, 0, 0, 100, c1deps}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
		// W(Y=2)
		arg = mdlinproto.Propose{3, state.Command{state.PUT, 1, 2}, 0, 0, 200, nil}
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()

		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		<-done

	} else {
		log.Println("Paxos....")
    for shard := 0; shard < len(readers); shard++ {
      go waitReplies(readers, shard, rsp, done)
    }
		var arg genericsmrproto.Propose
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
	}
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

		log.Printf("Shard %d: Reply.OK = %d, CommandId = %d, VALUE = %d, Timestamp = %d", shard, reply.OK, reply.CommandId, reply.Value, reply.Timestamp)
		log.Printf("rsp len %d and commandID was %d", len(*rsp), reply.CommandId)
		if reply.OK == 0 {
			log.Printf("Client request failed on shard %d", shard)
			failed[shard]++
			continue
		}
		if (*rsp)[reply.CommandId-1] != -1 {
			log.Printf("Duplicate reply on shard %d: %d", shard, reply.CommandId)
			failed[shard]++
			continue
		}
		(*rsp)[reply.CommandId-1] = int(reply.CommandId)
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
