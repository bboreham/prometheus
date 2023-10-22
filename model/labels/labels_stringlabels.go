// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !slicelabels

package labels

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/common/model"
	"golang.org/x/exp/slices"
)

// Well-known label names used by Prometheus components.
const (
	MetricName   = "__name__"
	AlertName    = "alertname"
	BucketLabel  = "le"
	InstanceName = "instance"
)

var seps = []byte{'\xff'}

// Label is a key/value pair of strings.
type Label struct {
	Name, Value string
}

// Labels is implemented by a SymbolTable and string holding name/value
// pairs encoded as indexes into the table in varint encoding.
// Names are in alphabetical order.
type Labels struct {
	syms *SymbolTable
	data string
}

// idea: split SymbolTable into the part used by Labels and the part used by Builder.  Only the latter needs the map.

// SymbolTable is used to map strings into numbers so they can be packed together.
type SymbolTable struct {
	mx     sync.Mutex
	byNum  []string
	byName map[string]int
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{
		byName: make(map[string]int, 1024),
	}
}

// ToNum maps a string to an integer, adding the string to the table if it is not already there.
// Note: copies the string before adding, in case the caller passed part of
// a buffer that should not be kept alive by this SymbolTable.
func (t *SymbolTable) ToNum(name string) int {
	t.mx.Lock()
	defer t.mx.Unlock()
	if i, found := t.byName[name]; found {
		return i
	}
	i := len(t.byNum)
	name = strings.Clone(name)
	t.byNum = append(t.byNum, name)
	t.byName[name] = i
	return i
}

// ToName maps an integer to a string.
func (t *SymbolTable) ToName(num int) string {
	t.mx.Lock()
	defer t.mx.Unlock()
	return t.byNum[num]
}

func decodeVarint(data string, index int) (int, int) {
	// Fast-path for common case of a single byte, value 0..127.
	b := data[index]
	index++
	if b < 0x80 {
		return int(b), index
	}
	size := int(b & 0x7F)
	for shift := uint(7); ; shift += 7 {
		// Just panic if we go of the end of data, since all Labels strings are constructed internally and
		// malformed data indicates a bug, or memory corruption.
		b := data[index]
		index++
		size |= int(b&0x7F) << shift
		if b < 0x80 {
			break
		}
	}
	return size, index
}

func decodeString(t *SymbolTable, data string, index int) (string, int) {
	var num int
	num, index = decodeVarint(data, index)
	return t.ToName(num), index
}

func (ls Labels) String() string {
	var b bytes.Buffer

	b.WriteByte('{')
	for i := 0; i < len(ls.data); {
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		var name, value string
		name, i = decodeString(ls.syms, ls.data, i)
		value, i = decodeString(ls.syms, ls.data, i)
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(value))
	}
	b.WriteByte('}')
	return b.String()
}

// Bytes returns ls as a byte slice.
// It uses non-printing characters and so should not be used for printing.
func (ls Labels) Bytes(buf []byte) []byte {
	b := bytes.NewBuffer(buf[:0])
	for i := 0; i < len(ls.data); {
		if i > 0 {
			b.WriteByte(seps[0])
		}
		var name, value string
		name, i = decodeString(ls.syms, ls.data, i)
		value, i = decodeString(ls.syms, ls.data, i)
		b.WriteString(name)
		b.WriteByte(seps[0])
		b.WriteString(value)
	}
	return b.Bytes()
}

// MarshalJSON implements json.Marshaler.
func (ls Labels) MarshalJSON() ([]byte, error) {
	return json.Marshal(ls.Map())
}

// UnmarshalJSON implements json.Unmarshaler.
func (ls *Labels) UnmarshalJSON(b []byte) error {
	var m map[string]string

	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	*ls = FromMap(m)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (ls Labels) MarshalYAML() (interface{}, error) {
	return ls.Map(), nil
}

// IsZero implements yaml.IsZeroer - if we don't have this then 'omitempty' fields are always omitted.
func (ls Labels) IsZero() bool {
	return len(ls.data) == 0
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (ls *Labels) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var m map[string]string

	if err := unmarshal(&m); err != nil {
		return err
	}

	*ls = FromMap(m)
	return nil
}

// MatchLabels returns a subset of Labels that matches/does not match with the provided label names based on the 'on' boolean.
// If on is set to true, it returns the subset of labels that match with the provided label names and its inverse when 'on' is set to false.
// TODO: This is only used in printing an error message
func (ls Labels) MatchLabels(on bool, names ...string) Labels {
	b := NewBuilder(ls)
	if on {
		b.Keep(names...)
	} else {
		b.Del(MetricName)
		b.Del(names...)
	}
	return b.Labels()
}

// Hash returns a hash value for the label set.
// Note: the result is not guaranteed to be consistent across different runs of Prometheus.
func (ls Labels) Hash() uint64 {
	// Use xxhash.Sum64(b) for fast path as it's faster.
	b := make([]byte, 0, 1024)
	for pos := 0; pos < len(ls.data); {
		name, newPos := decodeString(ls.syms, ls.data, pos)
		value, newPos := decodeString(ls.syms, ls.data, newPos)
		if len(b)+len(name)+len(value)+2 >= cap(b) {
			// If labels entry is 1KB+, hash the rest of them via Write().
			h := xxhash.New()
			_, _ = h.Write(b)
			for pos < len(ls.data) {
				name, pos = decodeString(ls.syms, ls.data, pos)
				value, pos = decodeString(ls.syms, ls.data, pos)
				_, _ = h.WriteString(name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}

		b = append(b, name...)
		b = append(b, seps[0])
		b = append(b, value...)
		b = append(b, seps[0])
		pos = newPos
	}
	return xxhash.Sum64(b)
}

// HashForLabels returns a hash value for the labels matching the provided names.
// 'names' have to be sorted in ascending order.
func (ls Labels) HashForLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	for i := 0; i < len(ls.data); {
		var name, value string
		name, i = decodeString(ls.syms, ls.data, i)
		value, i = decodeString(ls.syms, ls.data, i)
		for j < len(names) && names[j] < name {
			j++
		}
		if j == len(names) {
			break
		}
		if name == names[j] {
			b = append(b, name...)
			b = append(b, seps[0])
			b = append(b, value...)
			b = append(b, seps[0])
		}
	}

	return xxhash.Sum64(b), b
}

// HashWithoutLabels returns a hash value for all labels except those matching
// the provided names.
// 'names' have to be sorted in ascending order.
func (ls Labels) HashWithoutLabels(b []byte, names ...string) (uint64, []byte) {
	b = b[:0]
	j := 0
	for i := 0; i < len(ls.data); {
		var name, value string
		name, i = decodeString(ls.syms, ls.data, i)
		value, i = decodeString(ls.syms, ls.data, i)
		for j < len(names) && names[j] < name {
			j++
		}
		if name == MetricName || (j < len(names) && name == names[j]) {
			continue
		}
		b = append(b, name...)
		b = append(b, seps[0])
		b = append(b, value...)
		b = append(b, seps[0])
	}
	return xxhash.Sum64(b), b
}

// BytesWithLabels is just as Bytes(), but only for labels matching names.
// 'names' have to be sorted in ascending order.
func (ls Labels) BytesWithLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	j := 0
	for pos := 0; pos < len(ls.data); {
		lName, newPos := decodeString(ls.syms, ls.data, pos)
		lValue, newPos := decodeString(ls.syms, ls.data, newPos)
		for j < len(names) && names[j] < lName {
			j++
		}
		if j == len(names) {
			break
		}
		if lName == names[j] {
			if b.Len() > 1 {
				b.WriteByte(seps[0])
			}
			b.WriteString(lName)
			b.WriteByte(seps[0])
			b.WriteString(lValue)
		}
		pos = newPos
	}
	return b.Bytes()
}

// BytesWithoutLabels is just as Bytes(), but only for labels not matching names.
// 'names' have to be sorted in ascending order.
func (ls Labels) BytesWithoutLabels(buf []byte, names ...string) []byte {
	b := bytes.NewBuffer(buf[:0])
	j := 0
	for pos := 0; pos < len(ls.data); {
		lName, newPos := decodeString(ls.syms, ls.data, pos)
		lValue, newPos := decodeString(ls.syms, ls.data, newPos)
		for j < len(names) && names[j] < lName {
			j++
		}
		if j == len(names) || lName != names[j] {
			if b.Len() > 1 {
				b.WriteByte(seps[0])
			}
			b.WriteString(lName)
			b.WriteByte(seps[0])
			b.WriteString(lValue)
		}
		pos = newPos
	}
	return b.Bytes()
}

// Copy returns a copy of the labels.
func (ls Labels) Copy() Labels {
	buf := append([]byte{}, ls.data...)
	return Labels{syms: ls.syms, data: yoloString(buf)}
}

// Get returns the value for the label with the given name.
// Returns an empty string if the label doesn't exist.
func (ls Labels) Get(name string) string {
	if name == "" { // Avoid crash in loop if someone asks for "".
		return "" // Prometheus does not store blank label names.
	}
	for i := 0; i < len(ls.data); {
		var lName, lValue string
		lName, i = decodeString(ls.syms, ls.data, i)
		if lName == name {
			lValue, _ = decodeString(ls.syms, ls.data, i)
			return lValue
		} else if lName[0] > name[0] { // Stop looking if we've gone past.
			break
		}
		_, i = decodeVarint(ls.data, i)
	}
	return ""
}

// Has returns true if the label with the given name is present.
func (ls Labels) Has(name string) bool {
	if name == "" { // Avoid crash in loop if someone asks for "".
		return false // Prometheus does not store blank label names.
	}
	for i := 0; i < len(ls.data); {
		var lName string
		lName, i = decodeString(ls.syms, ls.data, i)
		if lName == name {
			return true
		} else if lName[0] > name[0] { // Stop looking if we've gone past.
			break
		}
		_, i = decodeVarint(ls.data, i)
	}
	return false
}

// HasDuplicateLabelNames returns whether ls has duplicate label names.
// It assumes that the labelset is sorted.
func (ls Labels) HasDuplicateLabelNames() (string, bool) {
	prevNum := -1
	for i := 0; i < len(ls.data); {
		var lNum int
		lNum, i = decodeVarint(ls.data, i)
		_, i = decodeVarint(ls.data, i)
		if lNum == prevNum {
			return ls.syms.ToName(lNum), true
		}
		prevNum = lNum
	}
	return "", false
}

// WithoutEmpty returns the labelset without empty labels.
// May return the same labelset.
func (ls Labels) WithoutEmpty() Labels {
	if ls.IsEmpty() {
		return ls
	}
	blank, ok := ls.syms.byName[""]
	if !ok { // Symbol table has no entry for blank - none of the values can be blank.
		return ls
	}
	for pos := 0; pos < len(ls.data); {
		_, newPos := decodeVarint(ls.data, pos)
		lValue, newPos := decodeVarint(ls.data, newPos)
		if lValue != blank {
			pos = newPos
			continue
		}
		// Do not copy the slice until it's necessary.
		// TODO: could optimise the case where all blanks are at the end.
		// Note: we size the new buffer on the assumption there is exactly one blank value.
		buf := make([]byte, pos, pos+(len(ls.data)-newPos))
		copy(buf, ls.data[:pos]) // copy the initial non-blank labels
		pos = newPos             // move past the first blank value
		for pos < len(ls.data) {
			var newPos int
			_, newPos = decodeVarint(ls.data, pos)
			lValue, newPos = decodeVarint(ls.data, newPos)
			if lValue != blank {
				buf = append(buf, ls.data[pos:newPos]...)
			}
			pos = newPos
		}
		return Labels{syms: ls.syms, data: yoloString(buf)}
	}
	return ls
}

// IsValid checks if the metric name or label names are valid.
func (ls Labels) IsValid() bool {
	err := ls.Validate(func(l Label) error {
		if l.Name == model.MetricNameLabel && !model.IsValidMetricName(model.LabelValue(l.Value)) {
			return strconv.ErrSyntax
		}
		if !model.LabelName(l.Name).IsValid() || !model.LabelValue(l.Value).IsValid() {
			return strconv.ErrSyntax
		}
		return nil
	})
	return err == nil
}

// Equal returns whether the two label sets are equal.
func Equal(a, b Labels) bool {
	if a.syms == b.syms {
		return a.data == b.data
	}

	la, lb := len(a.data), len(b.data)
	ia, ib := 0, 0
	for ia < la && ib < lb {
		var aName, bName string
		aName, ia = decodeString(a.syms, a.data, ia)
		bName, ib = decodeString(b.syms, b.data, ib)
		if aName != bName {
			return false
		}
		var aValue, bValue string
		aValue, ia = decodeString(a.syms, a.data, ia)
		bValue, ib = decodeString(b.syms, b.data, ib)
		if aValue != bValue {
			return false
		}
	}
	if ia != la || ib != lb {
		return false
	}
	return true
}

// DeepEqual implements the contract of package deepequal for tests.
func (ls Labels) DeepEqual(b Labels) bool {
	return Equal(ls, b)
}

// Map returns a string map of the labels.
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls.data))
	for i := 0; i < len(ls.data); {
		var lName, lValue string
		lName, i = decodeString(ls.syms, ls.data, i)
		lValue, i = decodeString(ls.syms, ls.data, i)
		m[lName] = lValue
	}
	return m
}

// EmptyLabels returns an empty Labels value, for convenience.
func EmptyLabels() Labels {
	return Labels{}
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

// New returns a sorted Labels from the given labels.
// The caller has to guarantee that all label names are unique.
// Note this function is not efficient; should not be used in performance-critical places.
func New(ls ...Label) Labels {
	slices.SortFunc(ls, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	syms := NewSymbolTable()
	size := labelsSize(syms, ls)
	buf := make([]byte, size)
	marshalLabelsToSizedBuffer(syms, ls, buf)
	return Labels{syms: syms, data: yoloString(buf)}
}

// FromMap returns new sorted Labels from the given map.
func FromMap(m map[string]string) Labels {
	l := make([]Label, 0, len(m))
	for k, v := range m {
		l = append(l, Label{Name: k, Value: v})
	}
	return New(l...)
}

// FromStrings creates new labels from pairs of strings.
func FromStrings(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	ls := make([]Label, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		ls = append(ls, Label{Name: ss[i], Value: ss[i+1]})
	}

	return New(ls...)
}

// Compare compares the two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
func Compare(a, b Labels) int {
	la, lb := len(a.data), len(b.data)
	ia, ib := 0, 0
	for ia < la && ib < lb {
		var aName, bName string
		aName, ia = decodeString(a.syms, a.data, ia)
		bName, ib = decodeString(b.syms, b.data, ib)
		if aName != bName {
			if aName < bName {
				return -1
			}
			return 1
		}
		var aValue, bValue string
		aValue, ia = decodeString(a.syms, a.data, ia)
		bValue, ib = decodeString(b.syms, b.data, ib)
		if aValue != bValue {
			if aValue < bValue {
				return -1
			}
			return 1
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return (la - ia) - (lb - ib)
}

// Copy labels from b on top of whatever was in ls previously, reusing memory or expanding if needed.
func (ls *Labels) CopyFrom(b Labels) {
	*ls = b // Straightforward memberwise copy is all we need.
}

// IsEmpty returns true if ls represents an empty set of labels.
func (ls Labels) IsEmpty() bool {
	return len(ls.data) == 0
}

// Len returns the number of labels; it is relatively slow.
func (ls Labels) Len() int {
	count := 0
	for i := 0; i < len(ls.data); {
		_, i = decodeVarint(ls.data, i)
		_, i = decodeVarint(ls.data, i)
		count++
	}
	return count
}

// Range calls f on each label.
func (ls Labels) Range(f func(l Label)) {
	for i := 0; i < len(ls.data); {
		var lName, lValue string
		lName, i = decodeString(ls.syms, ls.data, i)
		lValue, i = decodeString(ls.syms, ls.data, i)
		f(Label{Name: lName, Value: lValue})
	}
}

// Validate calls f on each label. If f returns a non-nil error, then it returns that error cancelling the iteration.
func (ls Labels) Validate(f func(l Label) error) error {
	for i := 0; i < len(ls.data); {
		var lName, lValue string
		lName, i = decodeString(ls.syms, ls.data, i)
		lValue, i = decodeString(ls.syms, ls.data, i)
		err := f(Label{Name: lName, Value: lValue})
		if err != nil {
			return err
		}
	}
	return nil
}

// InternStrings calls intern on every string value inside ls, replacing them with what it returns.
func (ls *Labels) InternStrings(intern func(string) string) {
	// TODO: remove these calls as there is nothing to do.
}

// ReleaseStrings calls release on every string value inside ls.
func (ls Labels) ReleaseStrings(release func(string)) {
	// TODO: remove these calls as there is nothing to do.
}

// Builder allows modifying Labels.
type Builder struct {
	base Labels
	del  []string
	add  []Label
}

// NewBuilder returns a new LabelsBuilder.
func NewBuilder(base Labels) *Builder {
	b := &Builder{
		del: make([]string, 0, 5),
		add: make([]Label, 0, 5),
	}
	b.Reset(base)
	return b
}

// Reset clears all current state for the builder.
func (b *Builder) Reset(base Labels) {
	if base.syms == nil { // If base has a symbol table, use that.
		if b.base.syms != nil {
			base.syms = b.base.syms // Or continue using previous symbol table in builder.
		} else {
			base.syms = NewSymbolTable() // Don't do this in performance-sensitive code.
		}
	}
	b.base = base
	b.del = b.del[:0]
	b.add = b.add[:0]
	base.Range(func(l Label) {
		if l.Value == "" {
			b.del = append(b.del, l.Name)
		}
	})
}

// Del deletes the label of the given name.
func (b *Builder) Del(ns ...string) *Builder {
	for _, n := range ns {
		for i, a := range b.add {
			if a.Name == n {
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n)
	}
	return b
}

// Keep removes all labels from the base except those with the given names.
func (b *Builder) Keep(ns ...string) *Builder {
	b.base.Range(func(l Label) {
		for _, n := range ns {
			if l.Name == n {
				return
			}
		}
		b.del = append(b.del, l.Name)
	})
	return b
}

// Set the name/value pair as a label. A value of "" means delete that label.
func (b *Builder) Set(n, v string) *Builder {
	if v == "" {
		// Empty labels are the same as missing labels.
		return b.Del(n)
	}
	for i, a := range b.add {
		if a.Name == n {
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, Label{Name: n, Value: v})

	return b
}

func (b *Builder) Get(n string) string {
	// Del() removes entries from .add but Set() does not remove from .del, so check .add first.
	for _, a := range b.add {
		if a.Name == n {
			return a.Value
		}
	}
	if slices.Contains(b.del, n) {
		return ""
	}
	return b.base.Get(n)
}

// Range calls f on each label in the Builder.
func (b *Builder) Range(f func(l Label)) {
	// Stack-based arrays to avoid heap allocation in most cases.
	var addStack [128]Label
	var delStack [128]string
	// Take a copy of add and del, so they are unaffected by calls to Set() or Del().
	origAdd, origDel := append(addStack[:0], b.add...), append(delStack[:0], b.del...)
	b.base.Range(func(l Label) {
		if !slices.Contains(origDel, l.Name) && !contains(origAdd, l.Name) {
			f(l)
		}
	})
	for _, a := range origAdd {
		f(a)
	}
}

func contains(s []Label, n string) bool {
	for _, a := range s {
		if a.Name == n {
			return true
		}
	}
	return false
}

// Labels returns the labels from the builder.
// If no modifications were made, the original labels are returned.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		return b.base
	}

	slices.SortFunc(b.add, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	slices.Sort(b.del)
	a, d := 0, 0

	bufSize := len(b.base.data) + labelsSize(b.base.syms, b.add)
	buf := make([]byte, 0, bufSize)
	for pos := 0; pos < len(b.base.data); {
		oldPos := pos
		var lName string
		lName, pos = decodeString(b.base.syms, b.base.data, pos)
		_, pos = decodeVarint(b.base.data, pos)
		for d < len(b.del) && b.del[d] < lName {
			d++
		}
		if d < len(b.del) && b.del[d] == lName {
			continue // This label has been deleted.
		}
		for ; a < len(b.add) && b.add[a].Name < lName; a++ {
			buf = appendLabelTo(b.base.syms, buf, &b.add[a]) // Insert label that was not in the base set.
		}
		if a < len(b.add) && b.add[a].Name == lName {
			buf = appendLabelTo(b.base.syms, buf, &b.add[a])
			a++
			continue // This label has been replaced.
		}
		buf = append(buf, b.base.data[oldPos:pos]...)
	}
	// We have come to the end of the base set; add any remaining labels.
	for ; a < len(b.add); a++ {
		buf = appendLabelTo(b.base.syms, buf, &b.add[a])
	}
	return Labels{syms: b.base.syms, data: yoloString(buf)}
}

func marshalLabelsToSizedBuffer(t *SymbolTable, lbls []Label, data []byte) int {
	i := len(data)
	for index := len(lbls) - 1; index >= 0; index-- {
		size := marshalLabelToSizedBuffer(t, &lbls[index], data[:i])
		i -= size
	}
	return len(data) - i
}

func marshalLabelToSizedBuffer(t *SymbolTable, m *Label, data []byte) int {
	i := len(data)
	i = encodeVarint(data, i, t.ToNum(m.Value))
	i = encodeVarint(data, i, t.ToNum(m.Name))
	return len(data) - i
}

func sizeVarint(x uint64) (n int) {
	// Most common case first
	if x < 1<<7 {
		return 1
	}
	if x >= 1<<56 {
		return 9
	}
	if x >= 1<<28 {
		x >>= 28
		n = 4
	}
	if x >= 1<<14 {
		x >>= 14
		n += 2
	}
	if x >= 1<<7 {
		n++
	}
	return n + 1
}

func encodeVarintSlow(data []byte, offset int, v uint64) int {
	offset -= sizeVarint(v)
	base := offset
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return base
}

// Special code for the common case that a value is less than 128
func encodeVarint(data []byte, offset, v int) int {
	if v < 1<<7 {
		offset--
		data[offset] = uint8(v)
		return offset
	}
	return encodeVarintSlow(data, offset, uint64(v))
}

func labelsSize(t *SymbolTable, lbls []Label) (n int) {
	// we just encode name/value/name/value, without any extra tags or length bytes
	for _, e := range lbls {
		n += labelSize(t, &e)
	}
	return n
}

func labelSize(t *SymbolTable, m *Label) (n int) {
	// strings are encoded as a single varint, the index into the symbol table.
	i := t.ToNum(m.Name)
	n = sizeVarint(uint64(i))
	i = t.ToNum(m.Value)
	n += sizeVarint(uint64(i))
	return n
}

func appendLabelTo(t *SymbolTable, buf []byte, m *Label) []byte {
	size := labelSize(t, m)
	sizeRequired := len(buf) + size
	if cap(buf) >= sizeRequired {
		buf = buf[:sizeRequired]
	} else {
		bufSize := cap(buf)
		// Double size of buffer each time it needs to grow, to amortise copying cost.
		for bufSize < sizeRequired {
			bufSize = bufSize*2 + 1
		}
		newBuf := make([]byte, sizeRequired, bufSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	marshalLabelToSizedBuffer(t, m, buf)
	return buf
}

// ScratchBuilder allows efficient construction of a Labels from scratch.
type ScratchBuilder struct {
	syms            *SymbolTable
	add             []Label
	output          Labels
	overwriteBuffer []byte
}

// NewScratchBuilder creates a ScratchBuilder initialized for Labels with n entries.
func NewScratchBuilder(s *SymbolTable, n int) ScratchBuilder {
	return ScratchBuilder{syms: s, add: make([]Label, 0, n)}
}

func (b *ScratchBuilder) ResetSymbolTable(s *SymbolTable) {
	b.syms = s
	b.Reset()
}

func (b *ScratchBuilder) Reset() {
	b.add = b.add[:0]
	b.output = EmptyLabels()
}

// Add a name/value pair.
// Note if you Add the same name twice you will get a duplicate label, which is invalid.
func (b *ScratchBuilder) Add(name, value string) {
	b.add = append(b.add, Label{Name: name, Value: value})
}

// Add a name/value pair, using []byte instead of string to reduce memory allocations.
// The values must remain live until Labels() is called.
func (b *ScratchBuilder) UnsafeAddBytes(name, value []byte) {
	b.add = append(b.add, Label{Name: yoloString(name), Value: yoloString(value)})
}

// Sort the labels added so far by name.
func (b *ScratchBuilder) Sort() {
	slices.SortFunc(b.add, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
}

// Assign is for when you already have a Labels which you want this ScratchBuilder to return.
func (b *ScratchBuilder) Assign(l Labels) {
	b.output = l
}

// Labels returns the name/value pairs added as a Labels object. Calling Add() after Labels() has no effect.
// Note: if you want them sorted, call Sort() first.
func (b *ScratchBuilder) Labels() Labels {
	if b.output.IsEmpty() {
		size := labelsSize(b.syms, b.add)
		buf := make([]byte, size)
		marshalLabelsToSizedBuffer(b.syms, b.add, buf)
		b.output = Labels{syms: b.syms, data: yoloString(buf)}
	}
	return b.output
}

// Write the newly-built Labels out to ls, reusing an internal buffer.
// Callers must ensure that there are no other references to ls, or any strings fetched from it.
func (b *ScratchBuilder) Overwrite(ls *Labels) {
	size := labelsSize(b.syms, b.add)
	if size <= cap(b.overwriteBuffer) {
		b.overwriteBuffer = b.overwriteBuffer[:size]
	} else {
		b.overwriteBuffer = make([]byte, size)
	}
	marshalLabelsToSizedBuffer(b.syms, b.add, b.overwriteBuffer)
	ls.syms = b.syms
	ls.data = yoloString(b.overwriteBuffer)
}
