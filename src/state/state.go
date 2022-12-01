package state

import (
	"log"
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
)

type Value int64

const NIL Value = 0

type Key int64
type Version int64

type Command struct {
	Op       Operation
	K        Key
	V        Value
	OldValue Value
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
	//log.Printf("Executing (%d, %d)\n", c.K, c.V)

	//var key, value [8]byte

	//    st.mutex.Lock()
	//    defer st.mutex.Unlock()

	switch c.Op {
	case PUT:
		/*
		 binary.LittleEndian.PutUint64(key[:], uint64(c.K))
		 binary.LittleEndian.PutUint64(value[:], uint64(c.V))
		 st.DB.Set(key[:], value[:], nil)
		*/

		st.Store[c.K] = c.V
		return c.V

	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}
	case CAS:
		if val, present := st.Store[c.K]; present {
			if val == c.OldValue {
				st.Store[c.K] = c.V
				return val
			}
		}
	}

	return NIL
}
