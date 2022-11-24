package main

import (
  "os"
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
  "math/rand"
  "strings"
  "sync"
)

// client -q 5 -r 5 -T 2

var coordAddr *string = flag.String("caddr", "", "Coordinator address. Defaults to localhost")
var coordPort *int = flag.Int("cport", 7097, "Coordinator port. Defaults to 7097.")
var fanout *int = flag.Int("fo", 5, "Fanout. Defaults to 5.")
var writes *int = flag.Int("w", 50, "Percentage of updates (writes). Defaults to 50%.")
var mdlin *bool = flag.Bool("mdl", false, "Multi-dispatch Linearizability: allow clients to issue multiple outstanding requests. Defaults to true")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var clients *int = flag.Int("c", 1, "Number of clients. Defaults to 1.")
var keys *int = flag.Int("k", 10, "Number of keys. Defaults to 10.")
var check = flag.Bool("check", false, "Check correctness. Defaults to false.")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")

var N int

var successful []int
var failed []int

var karray []int64
var rarray []int64
var reqarray []state.Operation

var numshards int64
var total_conflicts []int64
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
    go shardListener(readers, i)
	}

	successful = make([]int, N)
	failed = make([]int, N)
  complete := make(chan int64, *clients)

  numshards = int64(len(shard_leaders))
  total_conflicts = make([]int64, numshards)
  log.Printf("Fanout = %d, writes = %d, clients = %d, keys = %d", *fanout, *writes, *clients, *keys)
  ////////////////////////////////////////////////
	// Prepare the requests! 
	////////////////////////////////////////////////
  num_requests := (*fanout)*(*clients)
  karray = make([]int64, num_requests)
  rarray = make([]int64, num_requests)
  reqarray = make([]state.Operation, num_requests)
  for i:=0; i<num_requests; i++ {
    karray[i] = int64(rand.Intn(*keys)) //TODO change me when not uniform
    n := rand.Intn(100)
    if n < *writes {
      reqarray[i] = state.PUT
    } else {
      reqarray[i] = state.GET
    }
  }
  start := 0
  file, ferr := os.OpenFile("/home/anja/Desktop/MDLin/ms-test.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	if ferr != nil {
		log.Println("file error oh no", ferr)
		return
	}
	defer file.Close()
  buflock := new(sync.Mutex)
  for t:=0;t < *clients;t++ {
    go runTestMDL(int64(t), *fanout, start, start + *fanout, writers, file, complete, buflock)
    start += *fanout
  }

  for i:=0;i<*clients;i++ {
    pid:= <-complete
    log.Printf("Client %d completed", pid)
  }

  /*
  seqno := make(map[int64]int64, 0)
  for k:=0; k<*keys;k++ {
    leader := int64(k)%numshards
    if _, ok := seqno[leader]; !ok {
      seqno[leader] = 0
    } else {
      seqno[leader]++
    }
    arg := mdlinproto.Propose{0, state.Command{state.GET, state.Key(k), -1}, 0, seqno[leader], int64(*clients+1), nil}
    buflock.Lock()
    writers[leader].WriteByte(mdlinproto.PROPOSE)
    arg.Marshal(writers[leader])
    writers[leader].Flush()
    buflock.Unlock()
  }*/

  for i:=0; i<int(numshards); i++ {
    log.Printf("The total number of conflicts found on shard %d was %d", i, total_conflicts[i])
  }
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

func runTestMDL(pid int64, f int, start_index int, end_index int, writers []*bufio.Writer, file *os.File, complete chan int64, buflock *sync.Mutex) {
  done := make(chan bool)
  go waitRepliesMDL(start_index, end_index, done)
  before_total := time.Now()
  var arg mdlinproto.Propose
  deps := make([]mdlinproto.Tag, 0)
  seqno := make(map[int64]int64, 0)
  tab := strings.Repeat("    ", int(pid))
  for i:= start_index; i<end_index; i++ {
    leader := karray[i]%numshards
    if _, ok := seqno[leader]; !ok {
      seqno[leader] = 0
    } else {
      seqno[leader]++
    }
    arg = mdlinproto.Propose{int32(i), state.Command{reqarray[i], state.Key(karray[i]), state.Value(i)}, 0, seqno[leader], pid, deps}
    buflock.Lock()
		writers[leader].WriteByte(mdlinproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    log.Printf("%sClient %d, CommandId %d: %s", tab, pid, i, commandToStr(arg.Command))
    buflock.Unlock()
    deps = append(deps, mdlinproto.Tag{state.Key(karray[i]), -1, pid, int32(i)})
  }
  <-done
  after_total := time.Now()
  tot := after_total.Sub(before_total)
  log.Printf("Test took %v\n", tot)
  file.WriteString(fmt.Sprintf("MDL Fanout %d on client %d took %v\n", f, pid, tot.Milliseconds()))
  complete <- pid
}

func commandToStr(c state.Command) string {
  var s string
  if c.Op == state.GET {
    s = fmt.Sprintf("R(%d)", c.K)
  } else {
    s = fmt.Sprintf("W(%d) = %v", c.K, c.V)
  }
  return s
}

func runTestSDL(pid int64, f int, start_index int, end_index int, done chan bool, writers []*bufio.Writer) {
  go waitRepliesMDL(start_index, end_index, done)
  before_total := time.Now()
  var arg genericsmrproto.Propose
  for i:= start_index; i<end_index; i++ {
    leader := karray[i]%numshards
    arg = genericsmrproto.Propose{int32(i), state.Command{reqarray[i], state.Key(karray[i]), state.Value(i)}, 0}
		writers[leader].WriteByte(genericsmrproto.PROPOSE)
		arg.Marshal(writers[leader])
		writers[leader].Flush()
    <-done
  }
  after_total := time.Now()
  log.Printf("Test took %v\n", after_total.Sub(before_total))
}

func waitRepliesMDL(start int, end int, done chan bool) {
	for true {
    total := 0
    for i:=start; i<end; i++ {
      total+=int(rarray[i])
    }
    if total == *fanout {
      break
    }
  }
  done <- true
}

func shardListener(readers []*bufio.Reader, shard int) {
  reply := new(mdlinproto.ProposeReply)

	var err error
	var msgType byte
	for true {
		if msgType, err = readers[shard].ReadByte(); err != nil ||
			msgType != mdlinproto.PROPOSE_REPLY {
			//log.Printf("Error when reading response #%d from shard %d: %v", i, shard, err)
			continue
		}
		if err = reply.Unmarshal(readers[shard]); err != nil {
			log.Printf("Error when unmarshalling response from shard %d: %v", shard, err)
			continue
		}

		//log.Printf("Shard %d: Reply.OK = %d, CommandId = %d, VALUE = %d, Timestamp = %d", shard, reply.OK, reply.CommandId, reply.Value, reply.Timestamp)
		log.Printf("CommandId = %d, VALUE = %d", reply.CommandId, reply.Value)
    rarray[reply.CommandId] = 1
    if reply.NumConf > total_conflicts[shard] {
      total_conflicts[shard] = reply.NumConf
    }
  }
}

func waitReplies(readers []*bufio.Reader, shard int, rsp *[]int, done chan bool) {
	e := false

	reply := new(genericsmrproto.ProposeReply)

	var err error
	for i := 0; i < *fanout; i++ {
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
