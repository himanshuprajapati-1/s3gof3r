package s3gof3r

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// client is a client that encapsules low-level interactions with a
// specific, single blob.
type client struct {
	url        url.URL
	bucket     *Bucket
	httpClient *http.Client // http client to use for requests
	nTry       int
}

func newClient(
	url url.URL, bucket *Bucket, httpClient *http.Client, nTry int,
) *client {
	c := client{
		url:        url,
		bucket:     bucket,
		httpClient: httpClient,
		nTry:       nTry,
	}
	return &c
}

func (c *client) StartMultipartUpload(h http.Header) (string, error) {
	resp, err := c.retryRequest("POST", c.url.String()+"?uploads", nil, h)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", newRespError(resp)
	}

	var r struct {
		UploadID string `xml:"UploadId"`
	}

	err = xml.NewDecoder(resp.Body).Decode(&r)
	closeErr := resp.Body.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return r.UploadID, closeErr
	}
	return r.UploadID, nil
}

// UploadPart uploads a part of a multipart upload, checking the etag
// returned by S3 against the calculated value, including retries.
func (c *client) UploadPart(uploadID string, part *part) error {
	var err error
	for i := 0; i < c.nTry; i++ {
		err = c.uploadPartAttempt(uploadID, part)
		if err == nil {
			return nil
		}
		logger.debugPrintf(
			"Error on attempt %d: Retrying part: %d, Error: %s", i, part.partNumber, err,
		)
		// Exponential back-off:
		time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond)
	}
	return err
}

// uploadPartAttempt makes one attempt to upload a part of a multipart
// upload, checking the etag returned by S3 against the calculated
// value.
func (c *client) uploadPartAttempt(uploadID string, part *part) error {
	v := url.Values{}
	v.Set("partNumber", strconv.Itoa(part.partNumber))
	v.Set("uploadId", uploadID)
	req, err := http.NewRequest("PUT", c.url.String()+"?"+v.Encode(), bytes.NewReader(part.b))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(part.b))
	req.Header.Set(md5Header, part.md5)
	req.Header.Set(sha256Header, part.sha256)
	c.bucket.Sign(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return newRespError(resp)
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	s := resp.Header.Get("etag")
	if len(s) < 2 {
		return fmt.Errorf("Got Bad etag:%s", s)
	}
	s = s[1 : len(s)-1] // includes quote chars for some reason
	if part.eTag != s {
		return fmt.Errorf("Response etag does not match. Remote:%s Calculated:%s", s, part.eTag)
	}
	return nil
}

// completeMultipartUpload makes one attempt at completing a multiline
// upload, using `parts`, which have been uploaded already. Return:
//
// * `eTag, false, nil` on success;
// * `"", true, err` if there was a retryable error;
// * `"", false, err` if there was an unretryable error.
func (c *client) completeMultipartUpload(uploadID string, parts []*part) (string, bool, error) {
	type xmlPart struct {
		PartNumber int
		ETag       string
	}

	var xmlParts struct {
		XMLName string `xml:"CompleteMultipartUpload"`
		Part    []xmlPart
	}
	xmlParts.Part = make([]xmlPart, len(parts))
	for i, part := range parts {
		xmlParts.Part[i] = xmlPart{
			PartNumber: part.partNumber,
			ETag:       part.eTag,
		}
	}

	body, err := xml.Marshal(xmlParts)
	if err != nil {
		return "", false, err
	}

	b := bytes.NewReader(body)
	v := url.Values{}
	v.Set("uploadId", uploadID)

	resp, err := c.retryRequest("POST", c.url.String()+"?"+v.Encode(), b, nil)
	if err != nil {
		// If the connection got closed (firwall, proxy, etc.)
		// we should also retry, just like if we'd had a 500.
		if err == io.ErrUnexpectedEOF {
			return "", true, err
		}

		return "", false, err
	}
	if resp.StatusCode != 200 {
		return "", false, newRespError(resp)
	}

	// S3 will return an error under a 200 as well. Instead of the
	// CompleteMultipartUploadResult that we expect below, we might be
	// getting an Error, e.g. with InternalError under it. We should behave
	// in that case as though we received a 500 and try again.

	var r struct {
		ETag string
		Code string
	}

	err = xml.NewDecoder(resp.Body).Decode(&r)
	closeErr := resp.Body.Close()
	if err != nil {
		// The decoder unfortunately returns string error
		// instead of specific errors.
		if err.Error() == "unexpected EOF" {
			return "", true, err
		}

		return "", false, err
	}
	if closeErr != nil {
		return "", true, closeErr
	}

	// This is what S3 returns instead of a 500 when we should try
	// to complete the multipart upload again
	if r.Code == "InternalError" {
		return "", true, errors.New("S3 internal error")
	}
	// Some other generic error
	if r.Code != "" {
		return "", false, fmt.Errorf("CompleteMultipartUpload error: %s", r.Code)
	}

	return strings.Trim(r.ETag, "\""), false, nil
}

var err500 = errors.New("received 500 from server")

func (c *client) retryRequest(
	method, urlStr string, body io.ReadSeeker, h http.Header,
) (resp *http.Response, err error) {
	for i := 0; i < c.nTry; i++ {
		var req *http.Request
		req, err = http.NewRequest(method, urlStr, body)
		if err != nil {
			return
		}
		for k := range h {
			for _, v := range h[k] {
				req.Header.Add(k, v)
			}
		}

		if body != nil {
			req.Header.Set(sha256Header, shaReader(body))
		}

		c.bucket.Sign(req)
		resp, err = c.httpClient.Do(req)
		if err == nil && resp.StatusCode == 500 {
			err = err500
			time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond) // exponential back-off
		}
		if err == nil {
			return
		}
		logger.debugPrintln(err)
		if body != nil {
			if _, err = body.Seek(0, 0); err != nil {
				return
			}
		}
	}
	return
}
