package state

import (
	"encoding/binary"
	"io"
	"strings"
	"sync"
)

func (t *Command) BinarySize() (nbytes int, sizeKnown bool) {
	return 25, true
}

type CommandCache struct {
	mu    sync.Mutex
	cache []*Command
}

func NewCommandCache() *CommandCache {
	c := &CommandCache{}
	c.cache = make([]*Command, 0)
	return c
}

func (p *CommandCache) Get() *Command {
	var t *Command
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Command{}
	}
	return t
}
func (p *CommandCache) Put(t *Command) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

// Marshal writes the contents of the command to the given wire writer.
// The output is in the following format:
//  - 1 byte: the operation type (Op)
//  - 8 bytes: the length of key (K) in bytes
//  - 8 bytes: the length of value (V) in bytes
//  - 8 bytes: the length of old value (OldValue) in bytes
//  - K bytes: the actual string contents of key (K)
//  - V bytes: the actual string contents of value (V)
//  - OldValue bytes: the actual string contents of old value (OldValue)
func (t *Command) Marshal(wire io.Writer) {
	var b [25]byte
	var bs []byte
	bs = b[:25]
	bs[0] = byte(t.Op)
	tmp64 := uint64(t.K)
	bs[1] = byte(tmp64)
	bs[2] = byte(tmp64 >> 8)
	bs[3] = byte(tmp64 >> 16)
	bs[4] = byte(tmp64 >> 24)
	bs[5] = byte(tmp64 >> 32)
	bs[6] = byte(tmp64 >> 40)
	bs[7] = byte(tmp64 >> 48)
	bs[8] = byte(tmp64 >> 56)

	// Marshal V based on its type
	var vStr string
	if t.V.Type == StringType {
		vStr = t.V.String
	} else if t.V.Type == ListType {
		vStr = strings.Join(t.V.List, ",")
	} else if t.V.Type == SetType {
		var setItems []string
		for item := range t.V.Set {
			setItems = append(setItems, item)
		}
		vStr = strings.Join(setItems, ",")
	} else {
		vStr = ""
	}
	tmp64 = uint64(len(vStr))
	bs[9] = byte(tmp64)
	bs[10] = byte(tmp64 >> 8)
	bs[11] = byte(tmp64 >> 16)
	bs[12] = byte(tmp64 >> 24)
	bs[13] = byte(tmp64 >> 32)
	bs[14] = byte(tmp64 >> 40)
	bs[15] = byte(tmp64 >> 48)
	bs[16] = byte(tmp64 >> 56)

	// Marshal OldValue based on its type
	var oldValueStr string
	if t.OldValue.Type == StringType {
		oldValueStr = t.OldValue.String
	} else if t.OldValue.Type == ListType {
		oldValueStr = strings.Join(t.OldValue.List, ",")
	} else if t.OldValue.Type == SetType {
		var setItems []string
		for item := range t.OldValue.Set {
			setItems = append(setItems, item)
		}
		oldValueStr = strings.Join(setItems, ",")
	} else {
		oldValueStr = ""
	}
	tmp64 = uint64(len(oldValueStr))
	bs[17] = byte(tmp64)
	bs[18] = byte(tmp64 >> 8)
	bs[19] = byte(tmp64 >> 16)
	bs[20] = byte(tmp64 >> 24)
	bs[21] = byte(tmp64 >> 32)
	bs[22] = byte(tmp64 >> 40)
	bs[23] = byte(tmp64 >> 48)
	bs[24] = byte(tmp64 >> 56)

	wire.Write(bs)
	
	// Write the actual string contents
	wire.Write([]byte(vStr))
	wire.Write([]byte(oldValueStr))
}

func (t *Command) Unmarshal(wire io.Reader) error {
	var b [25]byte
	var bs []byte
	bs = b[:25]
	if _, err := io.ReadAtLeast(wire, bs, 25); err != nil {
		return err
	}
	t.Op = Operation(bs[0])
	t.K = Key((uint64(bs[1]) | (uint64(bs[2]) << 8) | (uint64(bs[3]) << 16) | (uint64(bs[4]) << 24) | (uint64(bs[5]) << 32) | (uint64(bs[6]) << 40) | (uint64(bs[7]) << 48) | (uint64(bs[8]) << 56)))

	// Unmarshal V 
	vLen := int((uint64(bs[9]) | (uint64(bs[10]) << 8) | (uint64(bs[11]) << 16) | (uint64(bs[12]) << 24) | (uint64(bs[13]) << 32) | (uint64(bs[14]) << 40) | (uint64(bs[15]) << 48) | (uint64(bs[16]) << 56)))
	vBytes := make([]byte, vLen)
	if _, err := io.ReadFull(wire, vBytes); err != nil {
		return err
	}
	vStr := string(vBytes)
	
	// Determine V type based on content
	if vStr == "" {
		t.V = Value{Type: StringType, String: ""}
	} else if strings.Contains(vStr, ",") {
		// If contains comma, it's a list
		t.V = Value{
			Type: ListType,
			List: strings.Split(vStr, ","),
		}
	} else {
		t.V = Value{
			Type:   StringType,
			String: vStr,
		}
	}

	// Unmarshal OldValue 
	oldValueLen := int((uint64(bs[17]) | (uint64(bs[18]) << 8) | (uint64(bs[19]) << 16) | (uint64(bs[20]) << 24) | (uint64(bs[21]) << 32) | (uint64(bs[22]) << 40) | (uint64(bs[23]) << 48) | (uint64(bs[24]) << 56)))
	oldValueBytes := make([]byte, oldValueLen)
	if _, err := io.ReadFull(wire, oldValueBytes); err != nil {
		return err
	}
	oldValueStr := string(oldValueBytes)
	
	// Determine OldValue type based on content
	if oldValueStr == "" {
		t.OldValue = Value{Type: StringType, String: ""}
	} else if strings.Contains(oldValueStr, ",") {
		// If contains comma, it's a list
		t.OldValue = Value{
			Type: ListType,
			List: strings.Split(oldValueStr, ","),
		}
	} else {
		t.OldValue = Value{
			Type:   StringType,
			String: oldValueStr,
		}
	}

	return nil
}

func (t *Key) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}


func (t *Value) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	
	// Convert value to string representation based on type
	var valueStr string
	switch t.Type {
	case StringType:
		valueStr = t.String
	case ListType:
		valueStr = strings.Join(t.List, ",")
	case SetType:
		var setItems []string
		for item := range t.Set {
			setItems = append(setItems, item)
		}
		valueStr = strings.Join(setItems, ",")
	default:
		valueStr = ""
	}
	
	// Write value length first
	valueLen := len(valueStr)
	binary.LittleEndian.PutUint64(bs, uint64(valueLen))
	w.Write(bs)
	
	// Write each character individually
	for i := 0; i < valueLen; i++ {
		w.Write([]byte{valueStr[i]})
	}
}


func (t *Key) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Key(binary.LittleEndian.Uint64(bs))
	return nil
}

func (t *Value) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	valueLen := binary.LittleEndian.Uint64(bs)
	
	valueBytes := make([]byte, valueLen)
	for i := 0; i < int(valueLen); i++ {
		if _, err := io.ReadFull(r, bs[:1]); err != nil {
			return err
		}
		valueBytes[i] = bs[0]
	}
	valueStr := string(valueBytes)
	
	if valueStr == "" {
		*t = Value{Type: StringType, String: ""}
	} else if strings.Contains(valueStr, ",") {
		// If contains comma, it's a list or set
		*t = Value{
			Type: ListType,
			List: strings.Split(valueStr, ","),
		}
	} else {
		*t = Value{
			Type:   StringType,
			String: valueStr,
		}
	}
	
	return nil
}

func (t *Version) Marshal(w io.Writer) {
	var b [8]byte
	bs := b[:8]
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
}

func (t *Version) Unmarshal(r io.Reader) error {
	var b [8]byte
	bs := b[:8]
	if _, err := io.ReadFull(r, bs); err != nil {
		return err
	}
	*t = Version(binary.LittleEndian.Uint64(bs))
	return nil
}
