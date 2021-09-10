package pool

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type testLogger struct {
	lock sync.Mutex
	buf  bytes.Buffer
}

func (logger *testLogger) Printf(format string, a ...interface{}) {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	fmt.Fprintf(&logger.buf, format, a...)
}

func (logger *testLogger) String() string {
	logger.lock.Lock()
	defer logger.lock.Unlock()

	return logger.buf.String()
}

func TestBP(t *testing.T) {
	// send log output to buffer
	var lf testLogger
	bp := NewBufferPool(&lf, mb)
	bp.SetTimeout(1 * time.Millisecond)
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
	ls := lf.String()
	if !strings.Contains(ls, expLog) {
		t.Errorf("BP debug logging on quit: \nExpected: %s\nActual: %s",
			expLog, ls)
	}
}
