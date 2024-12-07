package main

// #include <stdlib.h>
import "C"
import (
	"C"
	"clients"
	"encoding/json"
	"flag"
	"fmt"
	"state"
)
import (
	"crypto/md5"
	"encoding/binary"
	"strings"
)

type Response struct {
	Success bool        `json:"success"`
	Result  interface{} `json:"result"`
}

var clientId *int = flag.Int(
	"clientId",
	0,
	"Client identifier for use in replication protocols.")

var conflicts *int = flag.Int(
	"conflicts",
	-1,
	"Percentage of conflicts. If < 0, a zipfian distribution will be used for "+
		"choosing keys.")

var conflictsDenom *int = flag.Int(
	"conflictsDenom",
	100,
	"Denominator of conflict fraction when conflicts >= 0.")

var cpuProfile *string = flag.String(
	"cpuProfile",
	"",
	"Name of file for CPU profile. If empty, no profile is created.")

var debug *bool = flag.Bool(
	"debug",
	true,
	"Enable debug output.")

var defaultReplicaOrder *bool = flag.Bool(
	"defaultReplicaOrder",
	false,
	"Use default replica order for Gryff coordination.")

var epaxosMode *bool = flag.Bool(
	"epaxosMode",
	false,
	"Run Gryff with same message pattern as EPaxos.")

var expLength *int = flag.Int(
	"expLength",
	30,
	"Length of the timed experiment (in seconds).")

var fastPaxos *bool = flag.Bool(
	"fastPaxos",
	false,
	"Send message directly to all replicas a la Fast Paxos.")

var forceLeader *int = flag.Int(
	"forceLeader",
	-1,
	"Replica ID to which leader-based operations will be sent. If < 0, an "+
		"appropriate leader is chosen by default.")

var coordinatorAddr *string = flag.String(
	"caddr",
	"",
	"Coordinator address.")

var coordinatorPort *int = flag.Int(
	"cport",
	7097,
	"Coordinator port.")

var maxProcessors *int = flag.Int(
	"maxProcessors",
	2,
	"GOMAXPROCS. Defaults to 2")

var numKeys *uint64 = flag.Uint64(
	"numKeys",
	10000,
	"Number of keys in simulated store.")

var proxy *bool = flag.Bool(
	"proxy",
	false,
	"Proxy writes at local replica.")

var rampDown *int = flag.Int(
	"rampDown",
	5,
	"Length of the cool-down period after statistics are measured (in seconds).")

var rampUp *int = flag.Int(
	"rampUp",
	5,
	"Length of the warm-up period before statistics are measured (in seconds).")

var randSleep *int = flag.Int(
	"randSleep",
	1,
	"Max number of milliseconds to sleep after operation completed.")

var randomLeader *bool = flag.Bool(
	"randomLeader",
	false,
	"Egalitarian (no leader).")

var reads *int = flag.Int(
	"reads",
	0,
	"Percentage of reads.")

var regular *bool = flag.Bool(
	"regular",
	false,
	"Perform operations with regular consistency. (only for applicable protocols)")

var replProtocol *string = flag.String(
	"replProtocol",
	"",
	"Replication protocol used by clients and servers.")

var rmws *int = flag.Int(
	"rmws",
	0,
	"Percentage of rmws.")

var sequential *bool = flag.Bool(
	"sequential",
	true,
	"Perform operations with sequential consistency. "+
		"(only for applicable protocols")

var statsFile *string = flag.String(
	"statsFile",
	"",
	"Export location for collected statistics. If empty, no file file is written.")

var fanout *int = flag.Int(
	"fanout",
	1,
	"Fanout. Defaults to 1.")

var singleShardAware *bool = flag.Bool(
	"SSA",
	false,
	"Single shard awareness optimization. Defaults to false.")

var thrifty *bool = flag.Bool(
	"thrifty",
	false,
	"Only initially send messages to nearest quorum of replicas.")

var writes *int = flag.Int(
	"writes",
	1000,
	"Percentage of updates (writes).")

var zipfS = flag.Float64(
	"zipfS",
	2,
	"Zipfian s parameter. Generates values k∈ [0, numKeys] such that P(k) is "+
		"proportional to (v + k) ** (-s)")


type RetwisOp uint8
const (
        NONE RetwisOp = iota
	POST
)



func createClient() clients.Client {
	return clients.NewAsynchClient(int32(*clientId), *coordinatorAddr, *coordinatorPort, *forceLeader,
		*statsFile, false, true, *singleShardAware)
}

var client clients.Client

func init() {
    flag.Parse()
    *replProtocol = "async-mdl"  // async mdl
    *coordinatorAddr = "localhost"
    *coordinatorPort = 7087
    
    client = createClient()
    if client == nil {
        fmt.Println("Failed to create client during initialization")
        return
    }
    // fmt.Println("Client initialized successfully")
}

//export AsyncAppRequest
func AsyncAppRequest(opTypesJSON *C.char, keysJSON *C.char, value *C.char, oldValue *C.char) (bool, *C.char) {
    // Decode operation type
    var opType string
    err := json.Unmarshal([]byte(C.GoString(opTypesJSON)), &opType)
    if err != nil {
        fmt.Printf("Failed to parse op_type: %v\n", err)
        return false, C.CString("")
    }

    // Decode key
    var keyStr string
    err = json.Unmarshal([]byte(C.GoString(keysJSON)), &keyStr)
    if err != nil {
        fmt.Printf("Failed to parse key: %v\n", err)
        return false, C.CString("")
    }

    // Convert operation type to state.Operation
    var op state.Operation
    switch opType {
    case "GET":
        op = state.GET
    case "PUT":
        op = state.PUT
    case "CAS":
        op = state.CAS
    case "INCR":
        op = state.INCR
    case "SCARD":
        op = state.SCARD
    case "SADD":
        op = state.SADD
    case "ZREVRANGE":
        op = state.ZREVRANGE
    case "HMGET":
        op = state.HMGET
    case "HMSET":
        op = state.HMSET
    case "SET":
        op = state.SET
    case "PUBSUB":
        op = state.PUBSUB
    default:
        fmt.Printf("Invalid operation type: %s\n", opType)
        return false, C.CString("")
    }

    // Helper function to convert JSON to state.Value
    convertJSONToValue := func(jsonStr string) state.Value {
        var value interface{}
        if err := json.Unmarshal([]byte(jsonStr), &value); err != nil {
            fmt.Printf("Error unmarshaling JSON: %v\n", err)
            return state.NIL
        }
        
        switch v := value.(type) {
        case []interface{}:
            fmt.Printf("Converting list value: %v\n", v)
            strList := make([]string, len(v))
            for i, item := range v {
                strList[i] = fmt.Sprintf("%v", item)
            }
            return state.NewList(strList)
        case map[string]interface{}:
            fmt.Printf("Converting set value: %v\n", v)
            strSet := make(map[string]bool)
            for key := range v {
                strSet[key] = true
            }
            return state.NewSet(strSet)
        case string:
            return state.NewString(v)
        default:
            return state.NewString(fmt.Sprintf("%v", v))
        }
    }

    // Convert values
    var valueObj, oldValueObj state.Value
    if value != nil {
        jsonStr := C.GoString(value)
        valueObj = convertJSONToValue(jsonStr)
    }

    if oldValue != nil {
        jsonStr := C.GoString(oldValue)
        oldValueObj = convertJSONToValue(jsonStr)
    }

    // Create command
    keyInt64 := stringToInt64Hash(keyStr)

    // Execute command and return success and value as string
    success, val := client.AppRequest([]state.Operation{op}, []int64{keyInt64}, []state.Value{oldValueObj}, []state.Value{valueObj})
    if !success {
        return false, C.CString("")
    }
    
    return true, C.CString(val.String)
}

//export AsyncAppResponse
func AsyncAppResponse(keysJSON *C.char) *C.char {
	fmt.Println("Go: Starting AppResponse")
	defer fmt.Println("Go: Finishing AppResponse")

	var key int32
	// Convert C string to Go string
	keysStr := C.GoString(keysJSON)
	fmt.Printf("Go: Received string - keys: %s\n", keysStr)

	// Unmarshal JSON
	if err := json.Unmarshal([]byte(keysStr), &key); err != nil {
		fmt.Printf("Go: Error unmarshaling key: %v\n", err)
		response := Response{
			Success: false,
			Result:  err.Error(),
		}
		jsonResponse, _ := json.Marshal(response)
		return C.CString(string(jsonResponse))
	}

	// Call AsynchClient's AppResponse
	result, success := client.AppResponse(key)
	response := Response{
		Success: success != 0, // Convert uint8 to bool
		Result:  result,
	}

	// Marshal response to JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		fmt.Printf("Go: Error marshaling response: %v\n", err)
		errResponse := Response{
			Success: false,
			Result:  err.Error(),
		}
		jsonErrorResponse, _ := json.Marshal(errResponse)
		return C.CString(string(jsonErrorResponse))
	}

	return C.CString(string(jsonResponse))
}


func stringToInt64Hash(input string) int64 {
	// Normalize the input string
	normalized := strings.ToLower(strings.TrimSpace(input))

	// Generate MD5 hash
	hash := md5.Sum([]byte(normalized))

	// Convert first 8 bytes of MD5 hash to int64
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func main() {}