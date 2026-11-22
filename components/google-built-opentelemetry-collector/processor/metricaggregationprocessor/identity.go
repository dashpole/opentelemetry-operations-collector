// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metricaggregationprocessor

import (
	"cmp"
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// FNV-1a 128-bit constants (Fowler-Noll-Vo spec: prime = 2^88 + 0x13b).
const (
	fnvOffset128Hi uint64 = 0x6c62272e07bb0142
	fnvOffset128Lo uint64 = 0x62b821756295c58d
	fnvPrime128Lo  uint64 = 0x000000000000013b
	canonicalNaN   uint64 = 0x7ff8000000000001
)

type kvEntry struct {
	key string
	val pcommon.Value
}

type keyBuilder struct {
	buf     []byte
	entries []kvEntry
}

var keyBuilderPool = sync.Pool{
	New: func() any {
		return &keyBuilder{
			buf:     make([]byte, 0, 256),
			entries: make([]kvEntry, 0, 16),
		}
	},
}

func acquireKeyBuilder() *keyBuilder {
	kb := keyBuilderPool.Get().(*keyBuilder)
	kb.buf = kb.buf[:0]
	kb.entries = kb.entries[:0]
	return kb
}

func releaseKeyBuilder(kb *keyBuilder) {
	clear(kb.entries[:cap(kb.entries)])
	if cap(kb.buf) > 1<<16 {
		kb.buf = make([]byte, 0, 256)
	}
	if cap(kb.entries) > 256 {
		kb.entries = make([]kvEntry, 0, 16)
	} else {
		kb.entries = kb.entries[:0]
	}
	keyBuilderPool.Put(kb)
}

func (kb *keyBuilder) writeTag(t byte) {
	kb.buf = append(kb.buf, t)
}

func (kb *keyBuilder) writeUint32(v uint32) {
	kb.buf = binary.LittleEndian.AppendUint32(kb.buf, v)
}

func (kb *keyBuilder) writeUint64(v uint64) {
	kb.buf = binary.LittleEndian.AppendUint64(kb.buf, v)
}

func (kb *keyBuilder) writeString(s string) {
	kb.writeUint32(uint32(len(s)))
	kb.buf = append(kb.buf, s...)
}

func (kb *keyBuilder) writeMap(m pcommon.Map) {
	n := m.Len()
	kb.writeUint32(uint32(n))
	if n == 0 {
		return
	}
	start := len(kb.entries)
	m.Range(func(k string, v pcommon.Value) bool {
		kb.entries = append(kb.entries, kvEntry{key: k, val: v})
		return true
	})
	end := len(kb.entries)
	sub := kb.entries[start:end]
	slices.SortStableFunc(sub, func(a, b kvEntry) int {
		return cmp.Compare(a.key, b.key)
	})
	for i := 0; i < end-start; i++ {
		kb.writeString(kb.entries[start+i].key)
		kb.writeValue(kb.entries[start+i].val)
	}
	clear(kb.entries[start:end])
	kb.entries = kb.entries[:start]
}

func (kb *keyBuilder) writeSlice(s pcommon.Slice) {
	n := s.Len()
	kb.writeUint32(uint32(n))
	for i := 0; i < n; i++ {
		kb.writeValue(s.At(i))
	}
}

func (kb *keyBuilder) writeValue(v pcommon.Value) {
	kb.writeTag(byte(v.Type()))
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
	case pcommon.ValueTypeStr:
		kb.writeString(v.Str())
	case pcommon.ValueTypeBool:
		if v.Bool() {
			kb.writeTag(1)
		} else {
			kb.writeTag(0)
		}
	case pcommon.ValueTypeInt:
		kb.writeUint64(uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		d := v.Double()
		switch {
		case math.IsNaN(d):
			kb.writeUint64(canonicalNaN)
		case d == 0:
			kb.writeUint64(0)
		default:
			kb.writeUint64(math.Float64bits(d))
		}
	case pcommon.ValueTypeBytes:
		bs := v.Bytes()
		n := bs.Len()
		kb.writeUint32(uint32(n))
		for i := 0; i < n; i++ {
			kb.buf = append(kb.buf, bs.At(i))
		}
	case pcommon.ValueTypeSlice:
		kb.writeSlice(v.Slice())
	case pcommon.ValueTypeMap:
		kb.writeMap(v.Map())
	}
}

// appendResourcePrefix writes the canonical Resource + SchemaUrl encoding into kb.buf.
func (kb *keyBuilder) appendResourcePrefix(res pcommon.Resource, schemaURL string) {
	kb.writeTag('R')
	kb.writeString(schemaURL)
	kb.writeMap(res.Attributes())
}

// appendScopePrefix writes the canonical Scope encoding into kb.buf.
func (kb *keyBuilder) appendScopePrefix(scope pcommon.InstrumentationScope, schemaURL string) {
	kb.writeTag('S')
	kb.writeString(schemaURL)
	kb.writeString(scope.Name())
	kb.writeString(scope.Version())
	kb.writeMap(scope.Attributes())
}

// appendMetricPrefix writes the canonical Metric identity encoding into kb.buf.
func (kb *keyBuilder) appendMetricPrefix(m pmetric.Metric, temporality pmetric.AggregationTemporality, isMonotonic bool) {
	kb.writeTag('M')
	kb.writeString(m.Name())
	kb.writeString(m.Unit())
	kb.writeTag(byte(m.Type()))
	kb.writeTag(byte(temporality))
	if isMonotonic {
		kb.writeTag(1)
	} else {
		kb.writeTag(0)
	}
}

// appendDataPointAttrs writes the canonical DataPoint attribute map into kb.buf.
func (kb *keyBuilder) appendDataPointAttrs(attrs pcommon.Map) {
	kb.writeTag('D')
	kb.writeMap(attrs)
}

// fnv1a128 computes the 128-bit FNV-1a hash of b for shard routing.
// Because map lookups inside each shard still compare the full canonical byte
// string (via zero-alloc Go map[string] lookup), hash collisions can never merge
// distinct metric streams.
func fnv1a128(b []byte) [2]uint64 {
	hi := fnvOffset128Hi
	lo := fnvOffset128Lo
	for _, c := range b {
		lo ^= uint64(c)
		// 128-bit multiply by prime = 2^88 + 0x13b:
		// (hi*2^64 + lo) * (2^88 + 0x13b) mod 2^128
		//   low 64 bits:  lo * 0x13b (with carry into high 64 bits)
		//   high 64 bits: hi * 0x13b + (lo << 24) + carry
		carry, newLo := bits.Mul64(lo, fnvPrime128Lo)
		newHi := hi*fnvPrime128Lo + (lo << 24) + carry
		hi = newHi
		lo = newLo
	}
	return [2]uint64{hi, lo}
}
