package leakcheck

import (
	"strings"
	"testing"
	"time"
)

func TestDetectsLeak(t *testing.T) {
	stop := make(chan struct{})
	go func() { <-stop }()
	time.Sleep(10 * time.Millisecond)
	if got := strings.Join(others(), "\n"); !strings.Contains(got, "TestDetectsLeak") {
		t.Fatalf("blocked goroutine not reported:\n%s", got)
	}
	close(stop)
	if leaked := wait(time.Second); leaked != "" {
		t.Fatalf("goroutine reported after it exited:\n%s", leaked)
	}
}
