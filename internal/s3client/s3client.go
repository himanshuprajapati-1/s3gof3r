package s3client

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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

const (
	MD5Header    = "content-md5"
	SHA256Header = "X-Amz-Content-Sha256"
)

// Client is a Client that encapsules low-level interactions with a
// specific, single blob.
type Client struct {
	url        url.URL
	signer     signer
	httpClient *http.Client // http client to use for requests
	nTry       int
	logger     logger
}

func New(
	url url.URL, signer signer, httpClient *http.Client, nTry int, logger logger,
) *Client {
	c := Client{
		url:        url,
		signer:     signer,
		httpClient: httpClient,
		nTry:       nTry,
		logger:     logger,
	}
	return &c
}

func (c *Client) StartMultipartUpload(h http.Header) (string, error) {
	resp, err := c.retryRequest("POST", c.url.String()+"?uploads", nil, h)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 200 {
		return "", NewRespError(resp)
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

type Part struct {
	Data []byte

	// Read by xml encoder
	PartNumber int
	ETag       string

	// Checksums
	MD5    string
	SHA256 string
}

// UploadPart uploads a part of a multipart upload, checking the etag
// returned by S3 against the calculated value, including retries.
func (c *Client) UploadPart(uploadID string, part *Part) error {
	var err error
	for i := 0; i < c.nTry; i++ {
		err = c.uploadPartAttempt(uploadID, part)
		if err == nil {
			return nil
		}
		c.logger.Printf(
			"Error on attempt %d: Retrying part: %d, Error: %s", i, part.PartNumber, err,
		)
		// Exponential back-off:
		time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond)
	}
	return err
}

// uploadPartAttempt makes one attempt to upload a part of a multipart
// upload, checking the etag returned by S3 against the calculated
// value.
func (c *Client) uploadPartAttempt(uploadID string, part *Part) error {
	v := url.Values{}
	v.Set("partNumber", strconv.Itoa(part.PartNumber))
	v.Set("uploadId", uploadID)
	req, err := http.NewRequest("PUT", c.url.String()+"?"+v.Encode(), bytes.NewReader(part.Data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(part.Data))
	req.Header.Set(MD5Header, part.MD5)
	req.Header.Set(SHA256Header, part.SHA256)
	c.signer.Sign(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return NewRespError(resp)
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	s := resp.Header.Get("etag")
	if len(s) < 2 {
		return fmt.Errorf("Got Bad etag:%s", s)
	}
	s = s[1 : len(s)-1] // includes quote chars for some reason
	if part.ETag != s {
		return fmt.Errorf("Response etag does not match. Remote:%s Calculated:%s", s, part.ETag)
	}
	return nil
}

// CompleteMultipartUpload completes a multiline upload, using
// `parts`, which have been uploaded already. Retry on errors. On
// success, return the etag that was returned by S3.
func (c *Client) CompleteMultipartUpload(uploadID string, parts []*Part) (string, error) {
	attemptsLeft := 5
	for {
		eTag, retryable, err := c.completeMultipartUpload(uploadID, parts)
		if err == nil {
			// Success!
			return eTag, nil
		}

		attemptsLeft--
		if !retryable || attemptsLeft == 0 {
			return "", err
		}
	}
}

// completeMultipartUpload makes one attempt at completing a multiline
// upload, using `parts`, which have been uploaded already. Return:
//
// * `eTag, false, nil` on success;
// * `"", true, err` if there was a retryable error;
// * `"", false, err` if there was an unretryable error.
func (c *Client) completeMultipartUpload(uploadID string, parts []*Part) (string, bool, error) {
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
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
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
		return "", false, NewRespError(resp)
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

// AbortMultipartUpload aborts a multipart upload, discarding any
// partly-uploaded contents.
func (c *Client) AbortMultipartUpload(uploadID string) error {
	v := url.Values{}
	v.Set("uploadId", uploadID)
	s := c.url.String() + "?" + v.Encode()
	resp, err := c.retryRequest("DELETE", s, nil, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != 204 {
		return NewRespError(resp)
	}
	_ = resp.Body.Close()

	return nil
}

// PutMD5 attempts to write an md5 file in a ".md5" subdirectory of
// the directory where the blob is stored, with retries. For example,
// the md5 for blob https://mybucket.s3.amazonaws.com/gof3r will be
// stored in https://mybucket.s3.amazonaws.com/.md5/gof3r.md5.
func (c *Client) PutMD5(url *url.URL, md5 string) error {
	var err error
	for i := 0; i < c.nTry; i++ {
		err = c.putMD5(url, md5)
		if err == nil {
			break
		}
	}
	return err
}

// putMD5 makes one attempt to write an md5 file in a ".md5"
// subdirectory of the directory where the blob is stored; e.g., the
// md5 for blob https://mybucket.s3.amazonaws.com/gof3r will be stored
// in https://mybucket.s3.amazonaws.com/.md5/gof3r.md5.
func (c *Client) putMD5(url *url.URL, md5 string) error {
	md5Reader := strings.NewReader(md5)

	r, err := http.NewRequest("PUT", url.String(), md5Reader)
	if err != nil {
		return err
	}
	c.signer.Sign(r)
	resp, err := c.httpClient.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return NewRespError(resp)
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	return nil
}

var err500 = errors.New("received 500 from server")

func (c *Client) retryRequest(
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
			req.Header.Set(SHA256Header, SHA256Reader(body))
		}

		c.signer.Sign(req)
		resp, err = c.httpClient.Do(req)
		if err == nil && resp.StatusCode == 500 {
			err = err500
			time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond) // exponential back-off
		}
		if err == nil {
			return
		}
		c.logger.Printf("%v", err)
		if body != nil {
			if _, err = body.Seek(0, 0); err != nil {
				return
			}
		}
	}
	return
}

type signer interface {
	Sign(*http.Request)
}

type logger interface {
	Printf(format string, a ...interface{})
}

// Return the SHA-256 checksum of the contents of `r` in hex format,
// then seek back to the original location in `r`.
func SHA256Reader(r io.ReadSeeker) string {
	hash := sha256.New()
	start, _ := r.Seek(0, 1)
	defer r.Seek(start, 0)

	io.Copy(hash, r)
	sum := hash.Sum(nil)
	return hex.EncodeToString(sum)
}
