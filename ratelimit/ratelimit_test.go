package ratelimit

import (
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	b := New(10, 5, func() time.Time { return now })
	n := 0
	for b.Allow() {
		n++
	}
	if n != 5 {
		t.Fatalf("burst admitted %d, want 5", n)
	}
	now = now.Add(300 * time.Millisecond)
	for n = 0; b.Allow(); n++ {
	}
	if n != 3 {
		t.Fatalf("300ms at 10/s admitted %d, want 3", n)
	}
	now = now.Add(time.Hour)
	for n = 0; b.Allow(); n++ {
	}
	if n != 5 {
		t.Fatalf("refill capped at burst: admitted %d, want 5", n)
	}
}
