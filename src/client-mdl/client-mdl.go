package main

import (
	"bufio"
	"coordinatorproto"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"mdlinproto"
	"net"
	"net/rpc"
	"runtime"
	"state"
	"time"
)

// client -q 5 -r 5 -T 2

var coordAddr *string = flag.String("caddr", "", "Coordinator address. Defaults to localhost")
var coordPort *int = flat.Int("cport", 7097, "Coordinator port. Defaults to 7097.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var mdlin *bool = flag.Bool("mdl", true, "Multi-dispatch Linearizability: allow clients to issue multiple outstanding requests. Defaults to true")
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
var rsp []bool

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	//////////////////////////////////////
	// Get info from coordinator
	//////////////////////////////////////
	randObj := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNb / *rounds + *eps))

	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	log.Printf("Dialing coordinator at addr %s:%d\n", *coordAddr,
		*coordPort)
	coordinator, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *coordAddr, *coordPort))
	if err != nil {
		log.Fatalf("Error connecting to coordinator: %v\n", err)
	}

	llReply := new(coordinatorproto.GetShardLeaderListArgs)
	err = coordinator.Call("Coordinator.GetShardLeaderList", new(coordinatorproto.GGetShardLeaderListArgs), llReply)
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

	////////////////////////////////////
	// Prepare the commands being sent
	////////////////////////////////////
	N = len(llReply.LeaderList)
	leaders := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	rarray = make([]int, *reqsNb / *rounds + *eps)
	karray := make([]int64, *reqsNb / *rounds + *eps)
	put := make([]bool, *reqsNb / *rounds + *eps)
	perReplicaCount := make([]int, N)
	test := make([]int, *reqsNb / *rounds + *eps)
	for i := 0; i < len(rarray); i++ {
		r := rand.Intn(N)
		rarray[i] = r
		if i < *reqsNb / *rounds {
			perReplicaCount[r]++
		}

		if *conflicts >= 0 {
			r = rand.Intn(100)
			if r < *conflicts {
				karray[i] = 42
			} else {
				karray[i] = int64(43 + i)
			}
			r = rand.Intn(100)
			if r < *writes {
				put[i] = true
			} else {
				put[i] = false
			}
		} else {
			karray[i] = int64(zipf.Uint64())
			test[karray[i]]++
		}
	}
	if *conflicts >= 0 {
		log.Println("Uniform distribution")
	} else {
		log.Println("Zipfian distribution:")
		//log.Println(test[0:100])
	}

	////////////////////////////////////////
	// Create buffers for all servers that client reads from
	/////////////////////////////////////////
	for i := 0; i < N; i++ {
		var err error
		leaders[i], err = net.Dial("tcp", llReply.LeaderList[i]) //place the tcp connection object inside leaders[i]
		if err != nil {
			log.Printf("Error connecting to leader %d: %v\n", i, err)
		}
		readers[i] = bufio.NewReader(leaders[i])
		writers[i] = bufio.NewWriter(leaders[i])
	}

	successful = make([]int, N)
	failed = make([]int, N)

	var id int32 = 0
	done := make(chan bool, N)

	before_total := time.Now()

	////////////////////////////////////////////////
	// Sending the requests and waiting for replies
	////////////////////////////////////////////////
	for j := 0; j < *rounds; j++ {
		n := (*reqsNb) / *rounds

		if *check {
			rsp = make([]bool, n*(*T))
			for j := 0; j < n; j++ {
				rsp[j] = false
			}
		}

		go waitRepliesMDL(readers, leader, n*(*T), done) // number of reqs this round * threads

		before := time.Now()

		for tid := 0; tid < (*T); tid++ {
			go func(client_pid int64, id_base int32, arg mdlinproto.Propose) {
				for i := 0; i < n+*eps; i++ {
					arg.CommandId = id_base + int32(i)
					if put[i] {
						arg.Command.Op = state.PUT
					} else {
						arg.Command.Op = state.GET
					}
					arg.Command.K = state.Key(karray[i])
					arg.Command.V = state.Value(i)
					arg.PID = client_pid // ADD the client's id so we can sequence these
					arg.Timestamp = time.Now().UnixNano()
					arg.SeqNo = int64(i + j*(n+*eps)) //id increases across all the rounds
					if !*fast {
						if *noLeader {
							leader = rarray[i]
						}
						writers[leader].WriteByte(mdlinproto.PROPOSE) //gryff has clientproto.GEN_PROPOSE here
						arg.Marshal(writers[leader])
						writers[leader].Flush() //Should we flush right away?
					} else {
						//send to everyone
						for rep := 0; rep < N; rep++ {
							writers[rep].WriteByte(mdlinproto.PROPOSE) //gryff has clientproto.GEN_PROPOSE
							arg.Marshal(writers[rep])
							writers[rep].Flush()
						}
					}
					log.Printf("Thread %d sent command %d with seqNo %d", client_pid, id_base+int32(i), arg.SeqNo)
					log.Println(arg)
				}
			}(int64(tid), id, mdlinproto.Propose{id, state.Command{state.PUT, 0, 0}, 0, 0, 0})
			id += int32(n + *eps)
		}
		////////////////////////////////
		// Sync with WaitReplies()
		///////////////////////////
		err := false
		if *noLeader {
			for i := 0; i < N; i++ {
				e := <-done
				err = e || err
			}
		} else {
			err = <-done
		}

		after := time.Now()

		log.Printf("Round took %v\n", after.Sub(before))

		/////////////////////////////////////
		// Do some checks on the results
		///////////////////////////////////
		if *check {
			for j := 0; j < n*(*T); j++ {
				if !rsp[j] {
					log.Println("Didn't receive", j)
				}
			}
		}

		if err {
			if *noLeader {
				N = N - 1
			} else {
				reply := new(masterproto.GetLeaderReply)
				master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
				leader = reply.LeaderId // Cannot contact leader, figure out new leader
				log.Printf("New leader is replica %d\n", leader)
			}
		}
	}

	after_total := time.Now()
	log.Printf("Test took %v\n", after_total.Sub(before_total))

	s := 0
	for _, succ := range successful {
		s += succ
	}

	f := 0
	for _, fai := range failed {
		f += fai
	}

	log.Printf("Successful: %d, Failed: %d\n", s, f)

	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	master.Close()
}

func waitRepliesMDL(readers []*bufio.Reader, leader int, n int, done chan bool) {
	e := false

	reply := new(mdlinproto.ProposeReply)

	var err error
	var msgType byte
	for i := 0; i < n; i++ {
		if msgType, err = readers[leader].ReadByte(); err != nil ||
			msgType != mdlinproto.PROPOSE_REPLY {
			log.Printf("Error when reading (op:%d): %v", msgType, err)
			e = true
			continue
		}
		if err = reply.Unmarshal(readers[leader]); err != nil {
			log.Println("Error when reading:", err)
			e = true
			continue
		}

		log.Printf("Reply.OK = %d, CommandId = %d, PID = %d, Timestamp = %d, SeqNo = %d", reply.OK, reply.CommandId, reply.Value, reply.Timestamp, reply.ExpectedSeqNo)
		log.Printf("rsp len %d and commandID was %d and the index was %d", len(rsp), reply.CommandId, reply.CommandId/(int32(*rounds)))
		if reply.OK == 0 {
			log.Println("Client request failed")
			failed[leader]++
			continue
		}
		if *check {
			if rsp[reply.CommandId/(int32(*rounds))] {
				log.Println("Duplicate reply", reply.CommandId)
			}
			rsp[reply.CommandId/(int32(*rounds))] = true
		}
		if reply.OK != 0 {
			log.Printf("Success! expected seqno = %d", reply.ExpectedSeqNo)
			successful[leader]++
		}
	}
	done <- e
}

