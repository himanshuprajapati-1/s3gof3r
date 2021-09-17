package s3client

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// RespError represents an http error response
// http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
type RespError struct {
	Code       string
	Message    string
	Resource   string
	RequestID  string `xml:"RequestId"`
	StatusCode int
}

// NewRespError returns an error whose contents are based on the
// contents of `r.Body`. It closes `r.Body`.
func NewRespError(r *http.Response) *RespError {
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
