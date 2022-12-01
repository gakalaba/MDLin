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
  "sync"
)

// client -q 5 -r 5 -T 2

var coordAddr *string = flag.String("caddr", "", "Coordinator address. Defaults to localhost")
var coordPort *int = flag.Int("cport", 7097, "Coordinator port. Defaults to 7097.")
var pid_base *int = flag.Int("pid", 0, "Client base pid. Defaults to 0.")
var fanout *int = flag.Int("fo", 5, "Fanout. Defaults to 5.")
var writes *int = flag.Int("w", 50, "Percentage of updates (writes). Defaults to 50%.")
var mdlin *bool = flag.Bool("mdl", false, "Multi-dispatch Linearizability: allow clients to issue multiple outstanding requests. Defaults to true")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var clients *int = flag.Int("c", 1, "Number of clients per this client process. Defaults to 1.")
var keys *int = flag.Int("k", 10, "Number of keys. Defaults to 10.")
var check = flag.Bool("check", false, "Check correctness. Defaults to false.")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var trials *int = flag.Int("t", 10, "Trials. Defaults to 10")
var N int

var successful []int
var failed []int

var karray []int64
var rarray []int64
var reqarray []state.Operation

var numshards int64
var total_conflicts []int64

var total_trials []int64

var listenforshards bool
var seqno map[int64](map[int64]int64)
func newResponseArray(f int) []int {
	rsp := make([]int, f)
	for i := 0; i < f; i++ {
		rsp[i] = -1
	}
	return rsp
}

func getaverage(f []int64) float64 {
  var total int64 = 0
  for _, e := range f {
    total += e
  }
  return float64(total)/float64(len(f))
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

  listenforshards = true
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

  numshards = int64(len(shard_leaders))
  total_conflicts = make([]int64, numshards)
  //log.Printf("Fanout = %d, writes = %d, clients = %d, keys = %d", *fanout, *writes, *clients, *keys)

  seqno = make(map[int64](map[int64]int64), 0)
  file, ferr := os.OpenFile("/users/akalaba/MDLin/ms-test.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
  if ferr != nil {
    log.Println("file error oh no", ferr)
    return
  }
  defer file.Close()
  for trial:=0; trial<(*trials); trial++ {
    listenforshards = true
    ////////////////////////////////////////////////
    // Prepare the requests! 
    ////////////////////////////////////////////////
    num_requests := (*fanout)*(*clients)
    karray = make([]int64, num_requests)
    rarray = make([]int64, num_requests)
    reqarray = make([]state.Operation, num_requests)
    for i:=0; i<num_requests; i++ {
      rand.Seed(time.Now().UnixNano())
      karray[i] = int64(rand.Intn(*keys)) //TODO change me when not uniform
      n := rand.Intn(100)
      if n < *writes {
        reqarray[i] = state.PUT
      } else {
        reqarray[i] = state.GET
      }
    }
    start := 0
    reqs := make([]mdlinproto.Propose, num_requests)
    reqs_sdl := make([]genericsmrproto.Propose, num_requests)
    larray := make([]int64, num_requests)
    complete := make(chan struct {time.Time; int64}, *clients)
    submitted := make(chan int64, *clients)
    buflock := new(sync.Mutex)
    for t:=0;t < *clients;t++ {
      if (*mdlin) {
        go runTestMDL(int64(*pid_base+t), *fanout, start, start + *fanout, file, complete, submitted, &reqs, &larray)
      } else {
        go runTestSDL(int64(*pid_base+t), start, start + *fanout, complete, submitted, &reqs_sdl, &larray, writers, buflock)
      }
      start += *fanout
    }

    for i:=0;i<*clients;i++ {
      <-submitted
    }


    // shuffle requests
    if (*mdlin && *clients > 1) {
      rand.Seed(time.Now().UnixNano())
      for i := range reqs {
        j := rand.Intn(i + 1)
        reqs[i], reqs[j] = reqs[j], reqs[i]
        larray[i], larray[j] = larray[j], larray[i]
      }
    }

    // Nicely print the batches and the shard arrival logs
    for j:=0; j<*clients;j++ {
      log.Printf("Client %d:", j+*pid_base)
      for k:=0; k<*fanout; k++ {
        for i:=0; i<num_requests; i++ {
          arg := reqs[i]
          argsdl := reqs_sdl[i]
          if arg.PID == int64(j+*pid_base) && int(arg.CommandId - int32(*pid_base*10000)) == k+j*(*fanout){
            log.Printf("    CommandId %d-->%d: %s", arg.CommandId, larray[i], commandToStr(arg.Command))
          } else if (*mdlin == false) {
            if (int(argsdl.CommandId - int32(*pid_base*10000)) == k+j*(*fanout)) {
              log.Printf("    CommandId %d-->%d: %s", argsdl.CommandId, larray[i], commandToStr(argsdl.Command))
            }
          }
        }
      }
    }
    if (*mdlin && *clients > 1) {
      for j:=0; int64(j)<numshards;j++ {
        log.Printf("Shard %d Log:", j)
        for i:=0; i<num_requests; i++ {
          arg := reqs[i]
          leader := larray[i]
          if leader == int64(j) {
            log.Printf("     Client %d, CommandId %d: %s", arg.PID, arg.CommandId, commandToStr(arg.Command))
          }
        }
      }
    }

    if (*mdlin) {
      before_total := time.Now()
      for i:=0; i<num_requests; i++ {
        leader := larray[i]
        arg := reqs[i]
        writers[leader].WriteByte(mdlinproto.PROPOSE)
        arg.Marshal(writers[leader])
        writers[leader].Flush()
      }
      for i:=0;i<*clients;i++ {
        p := <-complete
        //log.Printf("Client %d completed", p.int64)
        tot := (p.Time).Sub(before_total)
        //log.Printf("Test took %v\n", tot)
        file.WriteString(fmt.Sprintf("MDL Fanout %d on client %d took %v\n", *fanout, p.int64, tot.Milliseconds()))

      }
      for i:=0; i<int(numshards); i++ {
        log.Printf("The total number of conflicts found on shard %d was %d", i, total_conflicts[i])
      }
    } else {
      before_total := time.Now()
      for i:=0; i<*clients; i++ {
        leader := larray[i*(*fanout)]
        arg := reqs_sdl[i*(*fanout)]
        writers[leader].WriteByte(genericsmrproto.PROPOSE)
        arg.Marshal(writers[leader])
        writers[leader].Flush()
      }
      for i:=0;i<*clients;i++ {
        p := <-complete
        //log.Printf("Client %d completed", p.int64)
        tot := (p.Time).Sub(before_total)
        //log.Printf("Test took %v\n", tot)
        //file.WriteString(fmt.Sprintf("SDL Fanout %d on client %d took %v\n", *fanout, p.int64, tot.Milliseconds()))
        total_trials[trial] = tot.Milliseconds()
      }
    }
    listenforshards = false
  }
  file.WriteString(fmt.Sprintf("SDL Fanout %d on client %d took %v\n", *fanout, *pid_base, getaverage(total_trials)))
  log.Printf("Fanout %d, PID %d took %v", *fanout, *pid_base, total_trials)
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

//////////////////////////////
//////// MDL /////////////////
//////////////////////////////
func runTestMDL(pid int64, f int, start_index int, end_index int, file *os.File, completed chan struct {time.Time; int64}, submitted chan int64, reqs *[]mdlinproto.Propose, larray *[]int64) {
  go waitRepliesMDL(start_index, end_index, completed, pid)
  var arg mdlinproto.Propose
  deps := make([]mdlinproto.Tag, 0)
  if _, ok := seqno[pid]; !ok {
    seqno[pid] = make(map[int64]int64, 0)
  }
  for i:= start_index; i<end_index; i++ {
    leader := karray[i]%numshards
    if _, ok := seqno[pid][leader]; !ok {
      seqno[pid][leader] = 0
    } else {
      seqno[pid][leader]++
    }
    arg = mdlinproto.Propose{int32(i+(*pid_base*10000)), state.Command{reqarray[i], state.Key(karray[i]), state.Value(i)}, 0, seqno[pid][leader], pid, deps}
    (*reqs)[i] = arg
    (*larray)[i] = leader
    deps = append(deps, mdlinproto.Tag{state.Key(karray[i]), -1, pid, int32(i+(*pid_base*10000))})
  }
  submitted<-pid
}

func waitRepliesMDL(start int, end int, complete chan struct {time.Time; int64}, pid int64) {
	for true {
    total := 0
    for i:=start; i<end; i++ {
      total+=int(rarray[i])
    }
    if total == *fanout {
      break
    }
  }
  after_total := time.Now()
  complete <- struct {time.Time; int64}{after_total, pid}
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

//////////////////////////////
//////// SDL /////////////////
//////////////////////////////
func runTestSDL(pid int64, start_index int, end_index int, completed chan struct {time.Time; int64}, submitted chan int64, 
          reqs_sdl *[]genericsmrproto.Propose, larray *[]int64, writers []*bufio.Writer, buflock *sync.Mutex) {
  go waitRepliesSDL(start_index, end_index, completed, pid, writers, larray, reqs_sdl, buflock)
  var arg genericsmrproto.Propose
  for i:= start_index; i<end_index; i++ {
    leader := karray[i]%numshards
    arg = genericsmrproto.Propose{int32(i+(*pid_base*10000)), state.Command{reqarray[i], state.Key(karray[i]), state.Value(i)}, 0}
    (*reqs_sdl)[i] = arg
    (*larray)[i] = leader
  }
  submitted<-pid
}

func waitRepliesSDL(start int, end int, complete chan struct {time.Time; int64}, pid int64, writers []*bufio.Writer, larray *[]int64, reqs_sdl *[]genericsmrproto.Propose, buflock *sync.Mutex) {
  i := start
  for true {
    if rarray[i] == 0 {
      continue
    }
    i++
    if i == end {
      break
    }
    leader := (*larray)[i]
    arg := (*reqs_sdl)[i]
    buflock.Lock()
    writers[leader].WriteByte(genericsmrproto.PROPOSE)
    arg.Marshal(writers[leader])
    writers[leader].Flush()
    buflock.Unlock()
  }
  after_total := time.Now()
  complete <- struct {time.Time; int64}{after_total, pid}
}

//////////////////////////////
/// Generic Shard Listener ///
//////////////////////////////
func shardListener(readers []*bufio.Reader, shard int) {
  reply := new(mdlinproto.ProposeReply)
  replysdl := new(genericsmrproto.ProposeReply)
	var err error
	var msgType byte
	for listenforshards {
		if (*mdlin) {
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
		  //log.Printf("CommandId = %d, VALUE = %d", reply.CommandId, reply.Value)
      rarray[reply.CommandId-int32(*pid_base*10000)] = 1
      if reply.NumConf > total_conflicts[shard] {
        total_conflicts[shard] = reply.NumConf
      }
    } else {
      if err = replysdl.Unmarshal(readers[shard]); err != nil {
        //log.Printf("Error when unmarshalling response from shard %d: %v", shard, err)
        continue
      }

      //log.Printf("CommandId = %d, VALUE = %d", replysdl.CommandId, replysdl.Value)
      rarray[replysdl.CommandId-int32(*pid_base*10000)] = 1
    }
  }
}

