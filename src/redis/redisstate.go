package state

import (
	"log"
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

	INCR
	LLEN
	SCARD
	SADD
	SISMEMBER
	LRANGE
	SREM
	EXISTS
	LPUSH
	SMEMBERS
)

type ValueType int

const (
    SingleType ValueType = iota
    ListType
)

type Value struct {
	Type    ValueType
	Int     int64
	List    []int64
}

func NewInt(i int64) Value {
	return Value{
		Type: SingleType,
		Int:  i,
	}
}

func NewList(l []int64) Value {
	return Value{
		Type: ListType,
		List: l,
	}
}

var NIL = Value{Type: SingleType, Int: 0}

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
	ListStore map[Key][]Value
	SetStore  map[Key]map[Key]bool
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
	return &State{
		Store: make(map[Key]Value),
		ListStore: make(map[Key][]Value),
		SetStore:  make(map[Key]map[Key]bool),
	}
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

	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}

	case CAS:
		if val, present := st.Store[c.K]; present {
			if val.Int == c.OldValue.Int {
				st.Store[c.K] = c.V
				return val
			}
		}

	case INCR:
		if val, present := st.Store[c.K]; present {
			st.Store[c.K] = NewInt(val.Int + 1)
			return NewInt(val.Int + 1)
		}
		st.Store[c.K] = NewInt(1)
		return NewInt(1)

	case LPUSH:
		// supports single item push
		st.ListStore[c.K] = append([]Value{c.V}, st.ListStore[c.K]...)
		return NewInt(int64(len(st.ListStore[c.K])))

	case LLEN:
		return NewInt(int64(len(st.ListStore[c.K])))

	case LRANGE:
		// we use V as start, OldValue as end...
		list := st.ListStore[c.K]
		if len(list) == 0 {
			return NewList([]int64{})
		}
		
		start, end := int(c.V.Int), int(c.OldValue.Int)
		if start < 0 {
			start = len(list) + start
		}
		if end < 0 {
			end = len(list) + end
		}
		
		if start < 0 {
			start = 0
		}
		if start >= len(list) {
			return NewList([]int64{})
		}
		if end < start {
			return NewList([]int64{})
		}
		if end >= len(list) {
			end = len(list) - 1
		}
		
		// Convert []Value to []int64
		result := make([]int64, 0, end-start+1)
		for _, v := range list[start:end+1] {
			result = append(result, v.Int)
		}
		return NewList(result)

	case SADD:
		// supports single item push to set
		if st.SetStore[c.K] == nil {
			st.SetStore[c.K] = make(map[Key]bool)
		}
		st.SetStore[c.K][Key(c.V.Int)] = true
		return NewInt(int64(len(st.SetStore[c.K])))

	case SCARD:
		return NewInt(int64(len(st.SetStore[c.K])))

	case SISMEMBER:
		if st.SetStore[c.K] != nil {
			if _, exists := st.SetStore[c.K][Key(c.V.Int)]; exists {
				return NewInt(1)
			}
		}
		return NewInt(0)

	case SREM:
		if st.SetStore[c.K] != nil {
			if _, exists := st.SetStore[c.K][Key(c.V.Int)]; exists {
				delete(st.SetStore[c.K], Key(c.V.Int))
				return NewInt(1)
			}
		}
		return NewInt(0)

	case SMEMBERS:
		count := NewInt(0)
		if set, exists := st.SetStore[c.K]; exists {
			count = NewInt(int64(len(set)))
		}
		return count

	case EXISTS:
		if _, exists := st.Store[c.K]; exists {
			return NewInt(1)
		}
		if _, exists := st.ListStore[c.K]; exists {
			return NewInt(1)
		}
		if _, exists := st.SetStore[c.K]; exists {
			return NewInt(1)
		}
		return NewInt(0)
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
