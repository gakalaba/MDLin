package state

import "sync"

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	PUT_BLIND // Result not needed immediately
)

type Value int64

const NIL Value = 0

type Key int64
type Version int64

type Command struct {
	Op Operation
	K  Key
	V  Value
}

var versions map[Key]Version
var vlock *sync.Mutex

func GetVersion(command *Command) Version {
	vlock.Lock()
	defer vlock.Unlock()

	if _, ok := versions[command.K]; !ok {
		versions[command.K] = 0
	}
	return versions[command.K]
}

func IncrVersion(command *Command) Version {
	vlock.Lock()
	defer vlock.Unlock()

	if _, ok := versions[command.K]; !ok {
		versions[command.K] = 0
	}
	versions[command.K]++
	return versions[command.K]
}

// Key-Value hashmap (ints->ints)
type State struct {
	Store map[Key]Value
}

func InitState() *State {
	versions = make(map[Key]Version)
	vlock = new(sync.Mutex)
	return &State{make(map[Key]Value)}
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func IsRead(command *Command) bool {
	return command.Op == GET
}

// Execute some kvstore command - PUT(key, value) or value = GET(key)
func (c *Command) Execute(st *State) Value {
	switch c.Op {
	case PUT, PUT_BLIND:
		st.Store[c.K] = c.V
		return c.V

	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}
	}

	return NIL
}

func AllReads(cmds []Command) bool {
	for i := range cmds {
		if cmds[i].Op != GET {
			return false
		}
	}
	return true
}

func AllWrites(cmds []Command) bool {
	for i := range cmds {
		if cmds[i].Op != PUT {
			return false
		}
	}
	return true
}

func AllBlindWrites(cmds []Command) bool {
	for i := range cmds {
		if cmds[i].Op != PUT_BLIND {
			return false
		}
	}
	return true
}
