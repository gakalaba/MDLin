package gryffcommon

import (
  "gryffproto"
)

type IRMWHandler interface {
  Loop(slowClockChan chan bool)
  HandleRMW(rmw *gryffproto.RMW)
}
