//go:build go1.17
// +build go1.17

package handle

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestHandle(t *testing.T) {
	v := 42

	tests := []struct {
		v1 any
		v2 any
	}{
		{v1: v, v2: v},
		{v1: &v, v2: &v},
		{v1: nil, v2: nil},
	}

	for _, tt := range tests {
		h1 := NewHandle(tt.v1)
		h2 := NewHandle(tt.v2)

		if uintptr(h1) == 0 || uintptr(h2) == 0 {
			t.Fatalf("NewHandle returns zero")
		}

		if uintptr(h1) == uintptr(h2) {
			t.Fatalf("Duplicated Go values should have different handles, but got equal")
		}

		h1v := h1.Value()
		h2v := h2.Value()
		if !reflect.DeepEqual(h1v, h2v) || !reflect.DeepEqual(h1v, tt.v1) {
			t.Fatalf("Value of a Handle got wrong, got %+v %+v, want %+v", h1v, h2v, tt.v1)
		}

		h1.Delete()
		h2.Delete()
	}

	siz := 0
	for i := range handles {
		list := handles[i]
		for _, j := range list.handles {
			if j.Load() != nil && j.Load() != nilSlot {
				siz++
			}
		}
	}
	if siz != 0 {
		t.Fatalf("handles are not cleared, got %d, want %d", siz, 0)
	}
}

func TestInvalidHandle(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		h := Handle(0)

		defer func() {
			if r := recover(); r != nil {
				return
			}
			t.Fatalf("Delete of zero handle did not trigger a panic")
		}()

		h.Delete()
	})

	t.Run("invalid", func(t *testing.T) {
		h := NewHandle(42)

		defer func() {
			if r := recover(); r != nil {
				h.Delete()
				return
			}
			t.Fatalf("Invalid handle did not trigger a panic")
		}()

		Handle(h + 1).Delete()
	})
}
func formatNum(n int) string {
	// add commas to separate thousands
	s := fmt.Sprint(n)
	if n < 0 {
		return "-" + formatNum(-n)
	}
	if len(s) <= 3 {
		return s
	}
	return formatNum(n/1000) + "," + s[len(s)-3:]
}
func BenchmarkHandle(b *testing.B) {
	b.Run("non-concurrent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h := NewHandle(i)
			_ = h.Value()
			h.Delete()
		}
	})
	b.Run("concurrent", func(b *testing.B) {
		start := time.Now()
		b.RunParallel(func(pb *testing.PB) {
			var v int
			for pb.Next() {
				h := NewHandle(v)
				_ = h.Value()
				h.Delete()
			}
		})
		fmt.Printf("Time: %fs\tTime Each: %dns\tNumber of handles: %s\n", time.Since(start).Seconds(), time.Since(start).Nanoseconds()/int64(b.N), formatNum(b.N))
	})
}
