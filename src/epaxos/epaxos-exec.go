package epaxos

import (
	//	"state"
	"epaxosproto"
	"genericsmrproto"
	"sort"
	"time"
	"dlog"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == epaxosproto.EXECUTED {
		return true
	}
	if inst.Status != epaxosproto.COMMITTED {
		return false
	}

	dlog.Printf("[%d.%d] Finding scc and then executing.\n", replica, instance)
	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	dlog.Printf("[%d] SCC %d %d.\n", e.r.Id, v.Slot, v.Seq)
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			for e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil || v.Cmds == nil {
				if v.lb != nil && v.lb.blockStartTime.IsZero() {
					v.lb.blockStartTime = time.Now()
				}
				dlog.Printf("Waiting for dep %d.%d predecessor %d.%d to exist.\n", q, inst, q, i)
				time.Sleep(1000 * 1000)
			}
			/*	  if !state.Conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
				  continue
				  }
			*/
			if e.r.InstanceSpace[q][i].Status == epaxosproto.EXECUTED {
				continue
			}
			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
				if v.lb != nil && v.lb.blockStartTime.IsZero() {
					v.lb.blockStartTime = time.Now()
				}
				dlog.Printf("Waiting for dep %d.%d predecessor %d.%d to be committed.\n", q, inst, q, i)
				time.Sleep(1000 * 1000)
			}
			w := e.r.InstanceSpace[q][i]

			if w.Index == 0 {
				//e.strongconnect(w, index)
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].Index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else { //if e.inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:len(stack)]

		dlog.Printf("SCC size: %d.\n", len(list))

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.Cmds == nil {
				time.Sleep(1000 * 1000)
			}
			if w.lb != nil {
				w.lb.executedTime = time.Now()
				blocked := int64(0)
				if !w.lb.blockStartTime.IsZero() {
					blocked = int64(w.lb.executedTime.Sub(w.lb.blockStartTime))
				}
				dlog.Printf("[%d.%d] Execute delay: %d (blocked %d).\n", e.r.Id, w.Slot, w.lb.executedTime.Sub(w.lb.committedTime), blocked)
			}
			for idx := 0; idx < len(w.Cmds); idx++ {
				if w.lb != nil {
					dlog.Printf("[%d.%d] Executing command %d.\n", e.r.Id, w.Slot, idx)
				}
				val := w.Cmds[idx].Execute(e.r.State)
				if e.r.NeedsWaitForExecute(&w.Cmds[idx]) && w.lb != nil && w.lb.clientProposals != nil {
					e.r.ReplyProposeTS(
						&genericsmrproto.ProposeReplyTS{
							TRUE,
							w.lb.clientProposals[idx].CommandId,
							val,
							w.lb.clientProposals[idx].Timestamp},
						w.lb.clientProposals[idx].Reply)
				}
			}
			w.Status = epaxosproto.EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
