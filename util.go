package s3gof3r

import "github.com/github/s3gof3r/internal/s3client"

// convenience multipliers
const (
	_        = iota
	kb int64 = 1 << (10 * iota)
	mb
	gb
	tb
	pb
	eb
)

// Min and Max functions
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

type bufferPoolLogger struct{}

func (l bufferPoolLogger) Printf(format string, a ...interface{}) {
	logger.debugPrintf(format, a...)
}

func StatusFromError(err error) (int, error) {
	if e, ok := err.(*s3client.RespError); ok {
		return e.StatusCode, nil
	} else {
		return 0, e
	}
}
