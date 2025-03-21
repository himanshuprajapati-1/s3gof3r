package s3gof3r

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io/ioutil"
	"net/http"

	"github.com/github/s3gof3r/internal/s3client"
)

type deleteObject struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

type deleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []deleteObject `xml:"Object"`
	Quiet   bool           `xml:"Quiet"`
}

type DeletedObject struct {
	Key                   string `xml:"Key"`
	VersionId             string `xml:"VersionId"`
	DeleteMarker          bool   `xml:"DeleteMarker"`
	DeleteMarkerVersionId string `xml:"DeleteMarkerVersionId"`
}

type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted"`
	Errors  []DeleteError   `xml:"Error"`
}

func deleteMultiple(c *Config, b *Bucket, quiet bool, keys []string) (DeleteResult, error) {
	if len(keys) == 0 {
		return DeleteResult{}, nil
	}

	u, err := b.url("", c)
	if err != nil {
		return DeleteResult{}, err
	}
	u.RawQuery = "delete"

	objects := make([]deleteObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, deleteObject{Key: key})
	}

	deleteRequest := deleteRequest{
		Objects: objects,
		Quiet:   quiet,
	}

	body, err := xml.Marshal(deleteRequest)
	if err != nil {
		return DeleteResult{}, err
	}

	md5sum := md5.Sum(body)
	r := http.Request{
		Method:        "POST",
		URL:           u,
		Body:          ioutil.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        make(http.Header),
	}
	r.Header.Set(s3client.MD5Header, base64.StdEncoding.EncodeToString(md5sum[:]))
	b.Sign(&r)

	resp, err := b.conf().Do(&r)
	if err != nil {
		return DeleteResult{}, err
	}
	if resp.StatusCode != 200 {
		return DeleteResult{}, s3client.NewRespError(resp)
	}

	var result DeleteResult
	decoder := xml.NewDecoder(resp.Body)
	if err := decoder.Decode(&result); err != nil {
		_ = resp.Body.Close()
		return DeleteResult{}, err
	}

	if err := resp.Body.Close(); err != nil {
		return DeleteResult{}, err
	}

	return result, nil
}
