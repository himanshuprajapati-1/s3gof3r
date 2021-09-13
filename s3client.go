package s3gof3r

import (
	"encoding/xml"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
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
