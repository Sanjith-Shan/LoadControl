// Package leakcheck fails a test binary that leaves goroutines running
// after its tests finish: a limiter waiter, a timer or a stream that is
// never released shows up here.
package leakcheck

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Main runs the tests, then waits up to 5 s for every goroutine started by
// them to exit. If some are still running it prints their stacks and fails.
func Main(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if leaked := wait(5 * time.Second); leaked != "" {
			fmt.Fprintf(os.Stderr, "leakcheck: goroutines still running after the tests:\n%s\n", leaked)
			code = 1
		}
	}
	os.Exit(code)
}

// wait polls until only the main goroutine and runtime or testing
// goroutines are left, and returns the stacks of any others.
func wait(limit time.Duration) string {
	deadline := time.Now().Add(limit)
	for {
		leaked := others()
		if len(leaked) == 0 || time.Now().After(deadline) {
			return strings.Join(leaked, "\n\n")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func others() []string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		switch {
		case strings.Contains(g, "leakcheck.others"), // this goroutine
			strings.Contains(g, "testing.(*M).") || strings.Contains(g, "testing.tRunner"),
			strings.Contains(g, "runtime.goexit") && strings.Contains(g, "created by runtime"),
			strings.Contains(g, "os/signal.signal_recv"),
			strings.Contains(g, "runtime.ensureSigM"):
			continue
		}
		out = append(out, g)
	}
	return out
}
