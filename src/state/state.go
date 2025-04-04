package state

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
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
	HSET
	HMGET
	SUBSCRIBE
	LISTEN
	PUBLISH
	EXISTS
	ZADD
	SREM
	SISMEMBER
	ZREVRANGE
	ZRANGE
	ZINCRBY
	ZSCORE
	HGETALL
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
	return []Operation{PUT, GET, EXISTS, CAS}
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
		fmt.Println("Executing put")
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

	// Type: hash, Return: integer
	// Set the specified field to the given value in a hash.
	case HSET:
		// Initialize hash if it doesn't exist or isn't a hash
		if st.Store[c.K].Type != HashType {
			st.Store[c.K] = NewHash(make(map[string]string))
		}

		// Check if the field already exists
		fieldExists := "0"
		if _, exists := st.Store[c.K].Hash[c.V.String]; !exists {
			fieldExists = "1"
		}
		// Set field-value pair
		st.Store[c.K].Hash[c.V.String] = c.OldValue.String

		return NewString(fieldExists)

	// Type: list, Return: list
	case HMGET:
		if st.Store[c.K].Type != HashType {
			st.Store[c.K] = NewHash(make(map[string]string))
		}
		
		result := make(map[string]string)
		for _, field := range c.V.List {
			result[field] = st.Store[c.K].Hash[field]
		}
		
		return NewHash(result)

	case HGETALL:
		// Check if the key exists and is a hash
		val, exists := st.Store[c.K]

		// Create a map to store field-value pairs
		result := make(map[string]string, len(val.Hash))

		if !exists || val.Type != HashType {
			return NewHash(result)
		}

		for field, value := range val.Hash {
			result[field] = value
		}
		
		// Return the map as a hash
		return NewHash(result)

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
		// Debug log
		
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
			// Update index to be current index plus number of messages we just read
			result := NewList(messages)
			// Update index to be current index plus number of messages we just read
			st.Store[indexKey] = NewString(strconv.Itoa(currentIndex + len(messages)))
			return result
		}
		return NewList([]string{})	

	// Type: string, Return: string
	// Append message to queue
	case PUBLISH:
		// Debug logging

		// Determine the value to publish
		publishValue := c.V.String
		if publishValue == "" && c.V.Type == ListType && len(c.V.List) > 0 {
			publishValue = c.V.List[0]
		}
		
		// Initialize queue if it doesn't exist
		if _, exists := st.Store[c.K]; !exists {
			st.Store[c.K] = NewList([]string{})
		} 
		
		// grab list and append new element	
		currentList := st.Store[c.K].List
		newList := append(currentList, publishValue)
		// store as new struct
		st.Store[c.K] = NewList(newList)
		return NewString("OK")
	
	case EXISTS:
		// Check existence of one or more keys
		if _, exists := st.Store[c.K]; exists {
			return NewString("1")
		}
		// Return the number of keys that exist
		return NewString("0")
	
	case SREM:
		val, exists := st.Store[c.K]
		if !exists || val.Type != SetType {
			return NewString("0")
		}

		if _, ok := val.Set[c.V.String]; ok {
			delete(val.Set, c.V.String)
			st.Store[c.K] = NewSet(val.Set)
			return NewString("1")
		}

		return NewString("0")
	
	case SISMEMBER:
		val, exists := st.Store[c.K]
		if !exists {
			return NewString("0")
		}
		if val.Type != SetType {
			return NewString("0")
		}
		if _, ok := val.Set[c.V.String]; ok {
			return NewString("1")
		}
		return NewString("0")
	
	case ZADD:
		// Initialize hash if it doesn't exist
		if _, exists := st.Store[c.K]; !exists || st.Store[c.K].Type != HashType {
			st.Store[c.K] = NewHash(make(map[string]string))
		}
		
		member := c.V.String
		score := c.OldValue.String

		st.Store[c.K].Hash[member] = score
		
		// Return 1 to indicate success
		return NewString("1")
	
	case ZREVRANGE:
		// Check if the key exists
		val, exists := st.Store[c.K]
		if !exists {
			return NIL
		}
		
		if val.Type != HashType {
			fmt.Println("Key is not a hash type")
			return NIL
		}
		
		// Create a slice of member-score pairs
		type MemberScore struct {
			Member string
			Score  float64
		}
		
		pairs := make([]MemberScore, 0, len(val.Hash))
		for member, scoreStr := range val.Hash {
			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil {
				// fmt.Println("Error parsing score:", err)
				// If score can't be parsed as float, use 0
				score = 0
			}
			pairs = append(pairs, MemberScore{Member: member, Score: score})
		}
		
		// Sort by score
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].Score > pairs[j].Score
		})
	
		// Convert start and stop to integers
		start, err1 := strconv.Atoi(c.V.String)
		stop, err2 := strconv.Atoi(c.OldValue.String)
	
		if err1 != nil || err2 != nil {
			return NIL
		}
	
		// Adjust indices to handle negative indexing
		if start < 0 {
			start = len(pairs) + start
		}
		if stop < 0 {
			stop = len(pairs) + stop
		}
	
		// Ensure indices are within bounds
		if start < 0 {
			start = 0
		}
		if stop >= len(pairs) {
			stop = len(pairs) - 1
		}
	
		// If start is beyond stop, return empty list
		if start > stop || len(pairs) == 0 {
			return NewList([]string{})
		}
	
		// Extract the range of members
		result := make([]string, 0, stop-start+1)
		for i := start; i <= stop && i < len(pairs); i++ {
			result = append(result, pairs[i].Member)
		}
		
		return NewList(result)
		

	case ZRANGE:
		// Check if the key exists
		val, exists := st.Store[c.K]
		if !exists {
			return NIL
		}
		
		if val.Type != HashType {
			fmt.Println("Key is not a hash type")
			return NIL
		}
		
		// Create a slice of member-score pairs
		type MemberScore struct {
			Member string
			Score  float64
		}
		
		pairs := make([]MemberScore, 0, len(val.Hash))
		for member, scoreStr := range val.Hash {
			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil {
				fmt.Println("Error parsing score:", err)
				// If score can't be parsed as float, use 0
				score = 0
			}
			pairs = append(pairs, MemberScore{Member: member, Score: score})
		}
		
		// Sort by score
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].Score < pairs[j].Score
		})
	
		// Convert start and stop to integers
		start, err1 := strconv.Atoi(c.V.String)
		stop, err2 := strconv.Atoi(c.OldValue.String)
	
		if err1 != nil || err2 != nil {
			return NIL
		}
	
		// Adjust indices to handle negative indexing
		if start < 0 {
			start = len(pairs) + start
		}
		if stop < 0 {
			stop = len(pairs) + stop
		}
	
		// Ensure indices are within bounds
		if start < 0 {
			start = 0
		}
		if stop >= len(pairs) {
			stop = len(pairs) - 1
		}
	
		// If start is beyond stop, return empty list
		if start > stop || len(pairs) == 0 {
			return NewList([]string{})
		}
	
		// Extract the range of members
		result := make([]string, 0, stop-start+1)
		for i := start; i <= stop && i < len(pairs); i++ {
			result = append(result, pairs[i].Member)
		}
		
		return NewList(result)
		
	case ZSCORE:
		// Check if the key exists and is a hash (sorted set)
		val, exists := st.Store[c.K]
		if !exists || val.Type != HashType {
			return NIL
		}
	
		// Get the score for the specified member
		scoreStr, exists := val.Hash[c.OldValue.String]
		if !exists {
			return NIL
		}
	
		// Return the score as a string
		return NewString(scoreStr)

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
