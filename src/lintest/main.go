package main

import (
  "flag"
  "clients"
  "log"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")

var linearizability *bool = flag.Bool("lin", true, "Test linearizability. Defaults to true.")
var replicationProtocol *string = flag.String("repl", "", "Replication protocol used by clients and servers. Defaults to ''")

func createClient(clientId int32) clients.Client {
  switch *replicationProtocol {
    case "abd":
      return clients.NewAbdClient(clientId, *masterAddr, *masterPort, -1, "",
          !*linearizability)
    case "gryff":
      return clients.NewGryffClient(clientId, *masterAddr, *masterPort, -1, "",
          !*linearizability, !*linearizability, false, false, true, false)
    default:
      return clients.NewProposeClient(clientId, *masterAddr, *masterPort, -1,
          "", false, false)
  }
}

func main() {
	flag.Parse()

  cs := make([]clients.Client, 3)
  for i := 0; i < len(cs); i++ {
    cs[i] = createClient(int32(i))
    cs[i].ConnectToMaster()
    cs[i].ConnectToReplicas()
    cs[i].DetermineLeader()
  }

  var success bool
  var successRead bool
  var readVal int64
  // WR test
  log.Printf("[WR] Starting...\n")
  log.Printf("[WR][0] Writing (1,1)\n")
  success = cs[0].Write(1, 1)
  log.Printf("[WR][1] Reading (1)\n")
  successRead, readVal = cs[1].Read(1)
  success = success && successRead
  if success && readVal == 1 {
    log.Printf("[WR] Success\n")
  } else if !success {
    log.Printf("[WR] Error during network communication.\n")
  } else {
    log.Printf("[WR] Failure (read %d when expecting %d)\n", readVal, 1)
  }

  // WW test
  log.Printf("[WW] Starting...\n")
  log.Printf("[WW][0] Writing (2,1)\n")
  success = cs[0].Write(2, 1)
  log.Printf("[WW][1] Writing (2,2)\n")
  success = success && cs[1].Write(2, 2)
  log.Printf("[WW][2] Reading (2)\n")
  successRead, readVal = cs[2].Read(2)
  success = success && successRead
  if success && readVal == 2 {
    log.Printf("[WW] Success\n")
  } else if !success {
    log.Printf("[WW] Error during network communication.\n")
  } else {
    log.Printf("[WW] Failure (read %d when expecting %d)\n", readVal, 2)
  }
  
  // RW test
  log.Printf("[RW] Starting...\n")
  log.Printf("[RW][0] Writing (3,1)\n")
  success = cs[0].Write(3, 1)
  log.Printf("[RW][1] Reading (3)\n")
  successRead, readVal = cs[1].Read(3)
  log.Printf("[RW][2] Writing (3,2)\n")
  success = success && successRead && cs[2].Write(3, 2)
  if success && readVal == 1 {
    log.Printf("[RW] Success\n")
  } else if !success {
    log.Printf("[RW] Error during network communication.\n")
  } else {
    log.Printf("[RW] Failure (read %d when expecting %d)\n", readVal, 1)
  }
  
  if *linearizability {
    // RR test
    log.Printf("[RR] Starting...\n")
    log.Printf("[RR][0] Writing (4,1)\n")
    success = cs[0].Write(4, 1)
    log.Printf("[RR][1] Reading (4)\n")
    successRead1, readVal1 := cs[1].Read(4)
    log.Printf("[RR][2] Reading (4)\n")
    successRead2, readVal2 := cs[2].Read(4)
    success = success && successRead1 && successRead2 
    if success && readVal1 == 1 && readVal2 == 1 {
      log.Printf("[RR] Success\n")
    } else if !success {
      log.Printf("[RR] Error during network communication.\n")
    } else {
      log.Printf("[RR] Failure (read %d and %d when expecting %d)\n", readVal1,
        readVal2, 1)
    }
  }

  log.Printf("[CAS] Starting...\n")
  log.Printf("[CAS][0] Writing (5,1)\n")
  success = cs[0].Write(5, 1)
  log.Printf("[CAS][1] CompareAndSwapping(5,1,2)\n")
  successRead1, readVal1 := cs[1].CompareAndSwap(5, 1, 2)
  log.Printf("[CAS][2] CompareAndSwapping(5,1,3)\n")
  successRead2, readVal2 := cs[2].CompareAndSwap(5, 1, 3)
  log.Printf("[CAS][0] Reading (5)\n")
  successRead3, readVal3 := cs[0].Read(5)
  success = success && successRead1 && successRead2 && successRead3
  if success && readVal1 == 1 && readVal2 == 2 && readVal3 == 2 {
    log.Printf("[CAS] Success\n")
  } else if !success {
    log.Printf("[CAS] Error during network communication.\n")
  } else {
    log.Printf("[CAS] Failure (read %d, %d, and %d when expecting %d, %d and %d\n", readVal1, readVal2, readVal3, 1, 2, 2)
  }

  for i := 0; i < len(cs); i++ {
    cs[i].Finish()
  }
}
