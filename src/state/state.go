package state

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"strings"
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
	SUBSCRIBE
	LISTEN
	PUBLISH
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
	SetType
	HashType
)

type Value struct {
	Type   ValueType
	String string
	List   []string
	Set    map[string]bool
	Hash   map[string]string
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

func NewHash(h map[string]string) Value {
	return Value{
		Type: HashType,
		Hash: h,
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

	//Type: string, Return: string
	case PUT:
		st.Store[c.K] = c.V
		return c.V

	//Type: string, Return: string
	case SET:
		st.Store[c.K] = c.V
		return NewString("OK")

	//Type: string, Return: List
	case GET:
		if val, present := st.Store[c.K]; present {
			return val
		}

	// Type: string, Return: string
	case CAS:
		if val, present := st.Store[c.K]; present {
			if val.String == c.OldValue.String {
				st.Store[c.K] = c.V
				return val
			}
		}

	// Type: string, Return: string
	// Increment the value stored at key by 1
	case INCR:
		if val, present := st.Store[c.K]; present {
			if val.String != "" {
				// Convert string to integer, increment, then back to string
				num, err := strconv.Atoi(val.String)
				if err != nil {
					return NewString("0") // Return 0 if not a valid number
				}
				newVal := NewString(strconv.Itoa(num + 1))
				st.Store[c.K] = newVal
				return newVal
			}
		}
		st.Store[c.K] = NewString("1")
		return NewString("1")
	
	// Type: string, Return: string
	// Add specified member(s)? to the set stored at key
	case SADD:
		if st.Store[c.K].Type != SetType {
			st.Store[c.K] = NewSet(make(map[string]bool))
		}
		st.Store[c.K].Set[c.V.String] = true
		return NewString(strconv.Itoa(len(st.Store[c.K].Set)))
	
	// Type: string, Return: string
	// Return number of elements of the set stored at key 
	case SCARD:
		if st.Store[c.K].Type == SetType {
			return NewString(strconv.Itoa(len(st.Store[c.K].Set)))
		}
		return NewString("0")
	
	// Type: list, Return: string
	// Set specified field to their respective value
	case HMSET:
		
		// Initialize hash if it doesn't exist or isn't a hash
		if st.Store[c.K].Type != HashType {
			st.Store[c.K] = NewHash(make(map[string]string))
		}
		
		// Set field-value pairs
		st.Store[c.K].Hash[c.V.String] = c.OldValue.String
		return NewString("OK")

	// Type: list, Return: list
	case HMGET:
		if st.Store[c.K].Type != HashType {
			st.Store[c.K] = NewHash(make(map[string]string))
		}
		return NewString(st.Store[c.K].Hash[c.V.String])

	// Type: string, Return: string
	// Initialize client's index for a queue
	case SUBSCRIBE:
		indexKey := Key(stringToInt64Hash(fmt.Sprintf("client_%v_index", c.K)))
		// Initialize index to 0 if it doesn't exist
		if _, exists := st.Store[indexKey]; !exists {
			st.Store[indexKey] = NewString("0")
		}
		// Initialize queue if it doesn't exist
		if _, exists := st.Store[c.K]; !exists {
			st.Store[c.K] = NewList([]string{})
		}
		return NewString("OK")

	// Type: string, Return: list
	// Get messages from index to current position
	case LISTEN:
		// Get index key
		indexKey := Key(stringToInt64Hash(fmt.Sprintf("client_%v_index", c.K)))
		if _, exists := st.Store[indexKey]; !exists {
			// if key doesn't exist, return empty list
			return NewList([]string{}) 
		}

		// Get current index
		currentIndex, _ := strconv.Atoi(st.Store[indexKey].String)
		
		// Get queue
		if queue, exists := st.Store[c.K]; exists && queue.Type == ListType {
			// get all messages from index to end
			messages := queue.List[currentIndex:]
			result := NewList(messages)
			// Update index to be current index plus number of messages we just read
			st.Store[indexKey] = NewString(strconv.Itoa(currentIndex + len(messages)))
			return result
		}
		return NewList([]string{})

	// Type: string, Return: string
	// Append message to queue
	case PUBLISH:
		// Initialize queue if it doesn't exist
		if _, exists := st.Store[c.K]; !exists {
			st.Store[c.K] = NewList([]string{})
		} 
		
		// grab list and append new element	
		currentList := st.Store[c.K].List
		newList := append(currentList, c.V.String)
		// store as new struct
		st.Store[c.K] = NewList(newList)
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

func stringToInt64Hash(input string) int64 {
	// Normalize the input string
	normalized := strings.ToLower(strings.TrimSpace(input))

	// Generate MD5 hash
	hash := md5.Sum([]byte(normalized))

	// Convert first 8 bytes of MD5 hash to int64
	return int64(binary.BigEndian.Uint64(hash[:8]))
}