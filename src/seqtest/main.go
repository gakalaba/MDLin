package main

import (
  "flag"
  "clients"
  "log"
  "dlog"
  "clientproto"
  "time"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")

func main() {
	flag.Parse()

  dlog.DLOG = true

  cs := make([]clients.Client, 3)
  for i := 0; i < len(cs); i++ {
    cs[i] = clients.NewGryffClient(int32(i), *masterAddr, *masterPort, -1, "",
        true, true, false, false, true, false)
    cs[i].ConnectToMaster()
    cs[i].ConnectToReplicas()
    cs[i].DetermineLeader()
  }

  log.Printf("Testing stale reads with different clients...\n")
  cs[0].DelayRPC(1, clientproto.Gryff_WRITE_2)
  cs[0].DelayRPC(2, clientproto.Gryff_WRITE_2)
  go func () {
    cs[0].Write(0, 1)
  }()
  time.Sleep(500 * time.Millisecond)

  cs[1].DelayRPC(2, clientproto.Gryff_READ_1)
  _, v1 := cs[1].Read(0)
  if v1 != 1 {
    log.Fatalf("[1] r(0) returned wrong value %d (%d expected).\n", v1, 1)
  }

  cs[1].DelayRPC(0, clientproto.Gryff_WRITE_2)
  cs[1].Write(1, 2)

  cs[2].DelayRPC(0, clientproto.Gryff_READ_1)
  _, v2 := cs[2].Read(1)
  if v2 != 2 {
    log.Fatalf("[2] r(1) returned wrong value %d (%d expected).\n", v2, 2)
  }

  cs[2].DelayRPC(0, clientproto.Gryff_READ_1)
  _, v3 := cs[2].Read(0)
  if v3 != 1 {
    log.Fatalf("[2] r(0) returned wrong value %d (%d expected).\n", v3, 1)
  }
  log.Printf("Success!\n")

  cs[0].Finish()
  cs[0] = clients.NewGryffClient(int32(0), *masterAddr, *masterPort, -1, "", true, true, false, false, true, false)
  cs[0].ConnectToMaster()
  cs[0].ConnectToReplicas()
  cs[0].DetermineLeader()

  log.Printf("Testing stale reads with same clients...\n")
  cs[0].DelayRPC(1, clientproto.Gryff_WRITE_2)
  cs[0].DelayRPC(2, clientproto.Gryff_WRITE_2)
  go func() {
    cs[0].Write(2, 1)
  }()
  time.Sleep(500 * time.Millisecond)

  cs[1].DelayRPC(2, clientproto.Gryff_READ_1)
  _, v4 := cs[1].Read(2)
  if v4 != 1 {
    log.Fatalf("[1] r(2) returned wrong value %d (%d expected).\n", v4, 1)
  }

  cs[1].DelayRPC(0, clientproto.Gryff_READ_1)
  _, v5 := cs[1].Read(2)
  if v5 != 1 {
    log.Fatalf("[1] r(2) returned wrong value %d (%d expected).\n", v5, 1)
  }
  log.Printf("Success!\n")

  for i := 0; i < len(cs); i++ {
    cs[i].Finish()
  }
}
