package s3gof3r

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
)

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

// RespError representbs an http error response
// http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
type RespError struct {
	Code       string
	Message    string
	Resource   string
	RequestID  string `xml:"RequestId"`
	StatusCode int
}

// newRespError returns an error whose contents are based on the
// contents of `r.Body`. It closes `r.Body`.
func newRespError(r *http.Response) *RespError {
	e := new(RespError)
	e.StatusCode = r.StatusCode
	_ = xml.NewDecoder(r.Body).Decode(e) // parse error from response
	_ = r.Body.Close()
	return e
}

func (e *RespError) Error() string {
	return fmt.Sprintf(
		"%d: %q",
		e.StatusCode,
		e.Message,
	)
}

func checkClose(c io.Closer, err error) {
	if c != nil {
		cerr := c.Close()
		if err == nil {
			err = cerr
		}
	}

}
