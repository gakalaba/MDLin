package state

import (
	"io"
	"sync"
	"encoding/binary"
)

func (t *Command) BinarySize() (nbytes int, sizeKnown bool) {
	return 25, true
}

type CommandCache struct {
	mu	sync.Mutex
	cache	[]*Command
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
	tmp64 = uint64(t.V)
	bs[9] = byte(tmp64)
	bs[10] = byte(tmp64 >> 8)
	bs[11] = byte(tmp64 >> 16)
	bs[12] = byte(tmp64 >> 24)
	bs[13] = byte(tmp64 >> 32)
	bs[14] = byte(tmp64 >> 40)
	bs[15] = byte(tmp64 >> 48)
	bs[16] = byte(tmp64 >> 56)
	tmp64 = uint64(t.OldValue)
	bs[17] = byte(tmp64)
	bs[18] = byte(tmp64 >> 8)
	bs[19] = byte(tmp64 >> 16)
	bs[20] = byte(tmp64 >> 24)
	bs[21] = byte(tmp64 >> 32)
	bs[22] = byte(tmp64 >> 40)
	bs[23] = byte(tmp64 >> 48)
	bs[24] = byte(tmp64 >> 56)
	wire.Write(bs)
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
	t.V = Value((uint64(bs[9]) | (uint64(bs[10]) << 8) | (uint64(bs[11]) << 16) | (uint64(bs[12]) << 24) | (uint64(bs[13]) << 32) | (uint64(bs[14]) << 40) | (uint64(bs[15]) << 48) | (uint64(bs[16]) << 56)))
	t.OldValue = Value((uint64(bs[17]) | (uint64(bs[18]) << 8) | (uint64(bs[19]) << 16) | (uint64(bs[20]) << 24) | (uint64(bs[21]) << 32) | (uint64(bs[22]) << 40) | (uint64(bs[23]) << 48) | (uint64(bs[24]) << 56)))
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
	binary.LittleEndian.PutUint64(bs, uint64(*t))
	w.Write(bs)
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
	*t = Value(binary.LittleEndian.Uint64(bs))
	return nil
}
