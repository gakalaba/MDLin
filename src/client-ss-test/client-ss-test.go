package main

import (
	"bufio"
	"flag"
	"fmt"
	"genericsmrproto"
	"log"
	"masterproto"
	"mdlinproto"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"state"
	"time"
)

// client -q 5 -r 5 -T 2

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
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
var trials *int = flag.Int("tris", 10, "Number of trials. Defaults to 10")

var N int

var successful []int
var failed []int

var rarray []int

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	//////////////////////////////////////
	// Get info from master
	//////////////////////////////////////
	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	log.Printf("Dialing master at addr %s:%d\n", *masterAddr,
		*masterPort)
	master, merr := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
	if merr != nil {
		log.Fatalf("Error connecting to master: %v\n", merr)
	}

	rlReply := new(masterproto.GetReplicaListReply)
	merr = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if merr != nil {
		log.Fatalf("Error making the GetReplicaList RPC: %v\n", merr)
	}
	log.Printf("Got replica list from master: [")
	for i := 0; i < len(rlReply.ReplicaList); i++ {
		log.Printf("%s", rlReply.ReplicaList[i])
		if i != len(rlReply.ReplicaList)-1 {
			log.Printf(", ")
		}
	}
	log.Printf("]\n")

	////////////////////////////////////////
	// Create buffers for all servers that client reads from
	/////////////////////////////////////////
	N = len(rlReply.ReplicaList)
	servers := make([]net.Conn, N)
	readers := make([]*bufio.Reader, N)
	writers := make([]*bufio.Writer, N)

	for i := 0; i < N; i++ {
		var err error
		servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
		if err != nil {
			log.Printf("Error connecting to replica %d: %v\n", i, err)
		}
		readers[i] = bufio.NewReader(servers[i])
		writers[i] = bufio.NewWriter(servers[i])
	}

	successful = make([]int, N)
	failed = make([]int, N)
	leader := 0

	if *noLeader == false {
		reply := new(masterproto.GetLeaderReply)
		if merr = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); merr != nil {
			log.Fatalf("Error making the GetLeader RPC\n")
		}
		leader = reply.LeaderId
	}

	done := make(chan bool, 1)
	////////////////////////////////////////////////
	// Sending the requests and waiting for replies
	////////////////////////////////////////////////
	file, ferr := os.OpenFile("/users/akalaba/ss-test.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 777)
	if ferr != nil {
		log.Println("file error oh no", ferr)
		return
	}
	defer file.Close()
	if (*mdlin) {
		file.WriteString("MDLin Test\n")
	} else {
		file.WriteString("SDLin Test\n")
	}
	fanout := [5]int{1, 10, 100, 1000, 10000}
	base := 0
	for _, f := range fanout {
		log.Printf("doing fanout of %d", f)
		time_results := make([]time.Duration, *trials)
		for t := 0; t < *trials; t++ {
			log.Printf(".....trial %d", t)
			rsp := newResponseArray(f)
			if *mdlin {
				go waitRepliesMDL(readers, leader, &rsp, done) // number of reqs this round * threads
				var arg mdlinproto.Propose
				before := time.Now()
				for i := 0; i < f; i++ {
					// CommandId = 0, Command = put(key 0 = val i), timestamp = 0, seqno = i, PID = 0
					arg = mdlinproto.Propose{int32(i), state.Command{state.PUT, 0, state.Value(i)}, 0, int64(base + i), 0}
					writers[leader].WriteByte(mdlinproto.PROPOSE)
					arg.Marshal(writers[leader])
					writers[leader].Flush()
				}

				////////////////////////////////
				// Sync with WaitReplies()
				///////////////////////////
				err := <-done
				after := time.Now()

				//log.Printf("Round took %v\n", after.Sub(before))
				time_results[t] = after.Sub(before)

				/////////////////////////////////////
				// Do some checks on the results
				///////////////////////////////////
				for j := 0; j < f; j++ {
					if rsp[j] == -1 {
						log.Println("Didn't receive", j)
					}
				}
				if err {
					reply := new(masterproto.GetLeaderReply)
					master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
					leader = reply.LeaderId // Cannot contact leader, figure out new leader
					log.Printf("New leader is replica %d\n", leader)
				}
			} else {
				log.Println("Paxos....")
				go waitReplies(readers, leader, &rsp, done) // number of reqs this round * threads
				var arg genericsmrproto.Propose
				before := time.Now()
				// Single issue
				for i := 0; i < f; i++ {
					arg = genericsmrproto.Propose{int32(i), state.Command{state.PUT, 0, state.Value(i)}, 0}
					writers[leader].WriteByte(genericsmrproto.PROPOSE)
					arg.Marshal(writers[leader])
					writers[leader].Flush()
					err := <-done //SINGLE DISPATCH
					if err {
						reply := new(masterproto.GetLeaderReply)
						master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
						leader = reply.LeaderId // Cannot contact leader, figure out new leader
						log.Printf("New leader is replica %d\n", leader)
					}
				}
				after := time.Now()
				//log.Printf("Round took %v\n", after.Sub(before))
				time_results[t] = after.Sub(before)
				/////////////////////////////////////
				// Do some checks on the results
				///////////////////////////////////
				if *check {
					for j := 0; j < f; j++ {
						if rsp[j] == -1 {
							log.Println("Didn't receive", j)
						}
					}
				}
			}
		}
		// Check correctnessssss
		successes := 0
		for _, succ := range successful {
			successes += succ
		}
		fails := 0
		for _, fai := range failed {
			fails += fai
		}
		//Log the avg across trials for this fanout
		log.Printf("Successful: %d, Failed: %d\n", successes, fails)
		total := int64(summ(time_results).Milliseconds()) // convert time duration to milliseconds as integer
		log.Printf("The total, predivide %v %d", summ(time_results).Milliseconds(), total)
		log.Printf("The AVERAGE is %d", total/int64(*trials))
		file.WriteString(fmt.Sprintf("Fanout %d took %v\n", f, total/int64(*trials)))
		base += f
	}
	for _, client := range servers {
		if client != nil {
			client.Close()
		}
	}
	master.Close()
}

func summ(a []time.Duration) time.Duration {
	total := 0*time.Millisecond
	for _, e := range a {
		log.Printf("SUMM, a time result is %v", e)
		total += e
	}
	log.Printf("The total returned sum %v", total)
	return total
}

func newResponseArray(f int) []int {
	rsp := make([]int, f)
	for i := 0; i < f; i++ {
		rsp[i] = -1
	}
	return rsp
}

func waitRepliesMDL(readers []*bufio.Reader, leader int, rsp *[]int, done chan bool) {
	e := false

	reply := new(mdlinproto.ProposeReply)

	var err error
	n := len(*rsp)
	for i := 0; i < n; i++ {
		if err = reply.Unmarshal(readers[leader]); err != nil {
			log.Println("Error when reading:", err)
			e = true
			continue
		}

		//log.Printf("Reply.OK = %d, CommandId = %d, PID = %d, Timestamp = %d, SeqNo = %d", reply.OK, reply.CommandId, reply.Value, reply.Timestamp, reply.ExpectedSeqNo)
		//log.Printf("rsp len %d and commandID was %d", len(rsp), reply.CommandId)
		if reply.OK == 0 {
			log.Println("Client request failed")
			failed[leader]++
			continue
		}
		if (*rsp)[reply.CommandId] != -1 {
			log.Println("Duplicate reply", reply.CommandId)
			failed[leader]++
			continue
		}
		(*rsp)[reply.CommandId] = int(reply.Value)
		// log.Printf("Success! expected seqno = %d", reply.ExpectedSeqNo)
		successful[leader]++
	}
	done <- e
}
func waitReplies(readers []*bufio.Reader, leader int, rsp *[]int, done chan bool) {
	e := false

	reply := new(genericsmrproto.ProposeReply)

	var err error
	n := len(*rsp)
	for i := 0; i < n; i++ {
		if err = reply.Unmarshal(readers[leader]); err != nil {
			log.Println("Error when reading:", err)
			e = true
			continue
		}

		//log.Printf("Reply.OK = %d, CommandId = %d, PID = %d, Timestamp = %d", reply.OK, reply.CommandId, reply.Value, reply.Timestamp)
		//log.Printf("rsp len %d and commandID was %d", len(rsp), reply.CommandId)
		if reply.OK == 0 {
			log.Println("Client request failed")
			failed[leader]++
			continue
		} else {
			if (*rsp)[reply.CommandId] != -1 {
				log.Println("Duplicate reply", reply.CommandId)
				failed[leader]++
				continue
			}
			(*rsp)[reply.CommandId] = int(reply.Value)
			// log.Printf("Success!")
			successful[leader]++
		}
		done <- e
	}
}
