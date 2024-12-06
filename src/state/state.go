package state

import (
	"log"
	"strconv"
	"sync"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	DELETE
	RLOCK
	WLOCK
	CAS
	SET
	INCR
	SADD
	SCARD
	HMSET
	HMGET
	ZREVRANGE
	PUBSUB
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
	SetType
)

type Value struct {
	Type   ValueType
	String string
	List   []string
	Set    map[string]bool
}

func NewString(s string) Value {
	return Value{
		Type:   StringType,
		String: s,
	}
}

func NewList(l []string) Value {
	return Value{
		Type: ListType,
		List: l,
	}
}

func NewSet(s map[string]bool) Value {
	return Value{
		Type: SetType,
		Set:  s,
	}
}

var NIL = Value{Type: StringType, String: ""}

type Key int64

type Version int64

type Command struct {
	Op       Operation
	K        Key
	V        Value
	OldValue Value
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

func KeyModulo(k Key, n int) int64 {
	return int64(k) % int64(n)
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

type State struct {
	Store map[Key]Value
	//DB *leveldb.DB
}

func NewState() *State {
	/*
		 d, err := leveldb.Open("/Users/iulian/git/epaxos-batching/dpaxos/bin/db", nil)

		 if err != nil {
				 log.Printf("Leveldb open failed: %v\n", err)
		 }

		 return &State{d}
	*/

	versions = make(map[Key]Version)
	vlock = new(sync.Mutex)
	return &State{make(map[Key]Value)}
}

func AllOpTypes() []Operation {
	return []Operation{PUT, GET, CAS}
}

func GetConflictingOpTypes(op Operation) []Operation {
	switch op {
	case PUT:
		return []Operation{PUT, GET, CAS}
	case GET:
		return []Operation{PUT, GET, CAS}
	case CAS:
		return []Operation{PUT, GET, CAS}
	default:
		log.Fatalf("Unsupported op type: %d.\n", op)
		return nil
	}
}

func OpTypesConflict(op1 Operation, op2 Operation) bool {
	return op1 == PUT || op1 == CAS || op2 == PUT || op2 == CAS
}

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || gamma.Op == CAS || delta.Op == PUT || delta.Op == CAS {
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

func (command *Command) CanReplyWithoutExecute() bool {
	return command.Op == PUT
}

func IsRead(command *Command) bool {
	return command.Op == GET
}


func (c *Command) Execute(st *State) Value {
	switch c.Op {
	case PUT:
		st.Store[c.K] = c.V
		return c.V

	case SET:
		st.Store[c.K] = c.V
		return NewString("OK")

	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}

	case CAS:
		if val, present := st.Store[c.K]; present {
			if val.String == c.OldValue.String {
				st.Store[c.K] = c.V
				return val
			}
		}

	case INCR:
		if val, present := st.Store[c.K]; present {
			if val.String != "" {
				newVal := NewString(string(val.String + "1"))
				st.Store[c.K] = newVal
				return newVal
			}
		}
		st.Store[c.K] = NewString("1")
		return NewString("1")

	case SADD:
		if st.Store[c.K].Type != SetType {
			st.Store[c.K] = NewSet(make(map[string]bool))
		}
		st.Store[c.K].Set[c.V.String] = true
		return NewString(strconv.Itoa(len(st.Store[c.K].Set)))

	case SCARD:
		if st.Store[c.K].Type == SetType {
			return NewString(strconv.Itoa(len(st.Store[c.K].Set)))
		}
		return NewString("0")

	case HMSET:
		if c.V.Type != ListType {
			return NewString("")
		}
		st.Store[c.K] = c.V
		return NewString("OK")

	case HMGET:
		if val, exists := st.Store[c.K]; exists && val.Type == ListType {
			result := make([]string, 0)
			for _, field := range c.V.List {
				for _, v := range val.List {
					if v == field {
						result = append(result, v)
						break
					}
				}
			}
			return NewList(result)
		}
		return NewList([]string{})

	case PUBSUB:
		// mock?
		return NewString("OK")
	default:
		return NIL
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
