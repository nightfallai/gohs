//go:build go1.17
// +build go1.17

package handle

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// Represents a slot in the handles slice for concurrent access.
type slot struct {
	value any
}

// A mask used to clear the most significant byte of a uint64 value.
const mask = 0x00FFFFFFFFFFFFFF

// Gets the handle within a list by clearing the most significant byte from the global handle.
func getLocalHandle(globalHandle uint64) uint64 {
	return globalHandle & mask
}

// Creates an uint64 value whose most significant byte represents the index of the list in which the
// handle is stored. The remaining bytes represent a handle internal to that list.
func getGlobalHandle(index uint8, internalHandle uint64) uint64 {
	return uint64(index)<<56 | getLocalHandle(internalHandle)
}

// The maximum number of released handles to keep for re-use.
const maxReleased = 1000

// An independent list of handles that stores a disjoint set of handles. Needed to avoid contention
// on a single lock.
type HandleList struct {
	sync.RWMutex

	// The list of handles.
	handles []*atomic.Pointer[slot]

	// A pool of previously released handle indexes for re-use. A list of
	released []uint64

	// An atomic counter storing the length of the handles slice.
	lenHandles atomic.Uint64
}

// Initializes a new handle list.
func NewHandleList(index uint8) *HandleList {
	return &HandleList{
		handles:    make([]*atomic.Pointer[slot], 0),
		released:   make([]uint64, 0),
		lenHandles: atomic.Uint64{},
	}
}

// Creates a new handle internal to this list.
func (l *HandleList) NewHandle(v any) uint64 {
	// Start from the first handle and iterate until a free slot is found.
	s := &slot{value: v}
	var h uint64 = 1
	for {
		// Handles that have the most significant byte set to anything other than 0 are invalid.
		// In the rare case this handle is reached, reset it to 1.
		if h > mask {
			h = 1
		}

		// Re-use a previously released handle if available.
		var released uint64
		l.Lock()
		if len(l.released) > 0 {
			released = l.released[len(l.released)-1]
			l.released = l.released[:len(l.released)-1]
		}
		l.Unlock()
		if released != 0 {
			h = released
		}

		// Expand the list if necessary.
		if h > l.lenHandles.Load() {
			l.Lock()
			l.handles = append(l.handles, &atomic.Pointer[slot]{})
			l.lenHandles.Store(uint64(len(l.handles)))
			l.Unlock()
		}

		// Try to set the slot in the handle if it's either nil or nilSlot. Otherwise, increment the
		// handle and try again.
		l.RLock()
		if l.handles[h-1].CompareAndSwap(nilSlot, s) {
			l.RUnlock()
			return h
		} else if l.handles[h-1].CompareAndSwap(nil, s) {
			l.RUnlock()
			return h
		} else {
			h++
		}
		l.RUnlock()
	}
}

// Returns the value referenced by the handle. Panics if the handle is invalid.
func (l *HandleList) Value(h uint64) any {
	l.RLock()
	defer l.RUnlock()

	// Check if the handle is valid.
	if h > uint64(len(handles)) {
		panic("runtime/cgo: misuse of an invalid Handle")
	}
	v := l.handles[h-1].Load()
	if v == nil || v == nilSlot {
		panic("runtime/cgo: misuse of an released Handle")
	}

	return v.value
}

// Invalidates a handle. Panics if the handle is invalid.
func (l *HandleList) Delete(h uint64) {
	l.RLock()
	defer l.RUnlock()

	// Check if the handle is valid.
	if h == 0 {
		panic("runtime/cgo: misuse of an zero Handle")
	}
	if h > uint64(len(handles)) {
		panic("runtime/cgo: misuse of an invalid Handle")
	}
	if v := l.handles[h-1].Swap(nilSlot); v == nil || v == nilSlot {
		panic("runtime/cgo: misuse of an released Handle")
	}

	// Add it to the released list if it's not full.
	if len(l.released) < maxReleased {
		l.released = append(l.released, h)
	}
}

// The number of separate handle lists, must be less than 256 because 8 bits are used to indicate
// from which list the handle was created. Number can be changed if fewer lists are needed.
const numLists = 256

var (
	// A list of separate handle lists, each of which stores as disjoint set of handles.
	handles [numLists]*HandleList

	// A nil slot used to indicate that a handle has been released.
	nilSlot = &slot{}
)

// Initializes all the handle lists.
func init() {
	for i := range handles {
		handles[i] = NewHandleList(uint8(i))
	}
}

// Returns a pseudo-random uint64. Fastest random implementation that can be used concurrently and
// is lock-free. Effectively, it calls runtime.fastrand.
func Rand64() uint64 {
	return new(maphash.Hash).Sum64()
}

// Provides a way to pass values that contain Go pointers (pointers to memory allocated by Go)
// between Go and C without breaking the cgo pointer passing rules. A Handle is an integer value
// that can represent any Go value. A Handle can be passed through C and back to Go, and Go code can
// use the Handle to retrieve the original Go value.
//
// The underlying type of Handle is guaranteed to fit in an integer type that is large enough to
// hold the bit pattern of any pointer. The zero value of a Handle is not valid, and thus is safe to
// use as a sentinel in C APIs.
type Handle uintptr

// Returns a handle for a given value, storing it in a random list.
func NewHandle(v any) Handle {
	// Use a random value between 0 and 255 to select a list.
	var listIndex uint8
	if numLists == 256 {
		listIndex = uint8(Rand64() >> 56)
	} else {
		listIndex = uint8(Rand64() >> 56 % numLists)
	}
	return Handle(getGlobalHandle(listIndex, handles[listIndex].NewHandle(v)))
}

// Returns the associated Go value for a valid handle.
//
// The method panics if the handle is invalid.
func (h Handle) Value() any {
	index := uint8(h >> 56)
	localHandle := getLocalHandle(uint64(h))
	return handles[index].Value(localHandle)
}

// Invalidates a handle. This method should only be called once the program no longer needs to pass
// the handle to C and the C code no longer has a copy of the handle value.
//
// The method panics if the handle is invalid.
func (h Handle) Delete() {
	index := uint8(h >> 56)
	localHandle := getLocalHandle(uint64(h))
	handles[index].Delete(localHandle)
}

// Returns a handle for a given value.
var New = NewHandle
