// +build !race

package pool

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

type testLogger struct {
	buf bytes.Buffer
}

func (logger *testLogger) Printf(format string, a ...interface{}) {
	fmt.Fprintf(&logger.buf, format, a...)
}

// The test causes data races due to reading the log buffer and setting bp.time
func TestBP(t *testing.T) {
	// send log output to buffer
	var lf testLogger
	bp := NewBufferPool(&lf, mb)
	bp.timeout = 1 * time.Millisecond
	b := bp.Get()
	if cap(b) != int(mb) {
		t.Errorf("Expected buffer capacity: %d. Actual: %d", kb, cap(b))
	}
	bp.Put(b)
	if n := bp.AllocationCount(); n != 2 {
		t.Errorf("Expected makes: %d. Actual: %d", 2, n)
	}

	b = bp.Get()
	bp.Put(b)
	time.Sleep(2 * time.Millisecond)
	if n := bp.AllocationCount(); n != 3 {
		t.Errorf("Expected makes: %d. Actual: %d", 3, n)
	}
	bp.Close()
	expLog := "3 buffers of 1 MB allocated"
	time.Sleep(1 * time.Millisecond) // wait for log
	ls := lf.buf.String()
	if !strings.Contains(ls, expLog) {
		t.Errorf("BP debug logging on quit: \nExpected: %s\nActual: %s",
			expLog, ls)
	}
}
