package s3gof3r

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/github/s3gof3r/internal/pool"
	"github.com/github/s3gof3r/internal/s3client"
)

// defined by amazon
const (
	minPartSize = 5 * mb
	maxPartSize = 5 * gb
	maxObjSize  = 5 * tb
	maxNPart    = 10000
)

type s3Putter interface {
	StartMultipartUpload(h http.Header) (string, error)
	UploadPart(uploadID string, part *s3client.Part) error
	CompleteMultipartUpload(uploadID string, parts []*s3client.Part) (string, error)
	AbortMultipartUpload(uploadID string) error
	PutMD5(url *url.URL, md5 string) error
}

type putter struct {
	url    url.URL
	b      *Bucket
	c      *Config
	client s3Putter

	bufsz      int64
	buf        []byte
	bufbytes   int // bytes written to current buffer
	ch         chan *s3client.Part
	closed     bool
	err        error
	wg         sync.WaitGroup
	md5OfParts hash.Hash
	md5        hash.Hash
	eTag       string

	sp *pool.BufferPool

	uploadID string
	parts    []*s3client.Part
	putsz    int64
}

// Sends an S3 multipart upload initiation request.
// See http://docs.amazonwebservices.com/AmazonS3/latest/dev/mpuoverview.html.
// The initial request returns an UploadId that we use to identify
// subsequent PUT requests.
func newPutter(url url.URL, h http.Header, c *Config, b *Bucket) (*putter, error) {
	cCopy := *c
	cCopy.Concurrency = max(c.Concurrency, 1)
	cCopy.NTry = max(c.NTry, 1)
	bCopy := *b
	bufsz := max64(minPartSize, cCopy.PartSize)
	p := putter{
		url:        url,
		c:          &cCopy,
		b:          &bCopy,
		bufsz:      bufsz,
		ch:         make(chan *s3client.Part),
		md5OfParts: md5.New(),
		md5:        md5.New(),
	}

	p.client = s3client.New(p.url, p.b, p.c.Client, p.c.NTry, bufferPoolLogger{})

	var err error
	p.uploadID, err = p.client.StartMultipartUpload(h)
	if err != nil {
		return nil, err
	}

	p.sp = pool.NewBufferPool(bufferPoolLogger{}, bufsz)

	for i := 0; i < p.c.Concurrency; i++ {
		go p.worker()
	}

	return &p, nil
}

func (p *putter) Write(b []byte) (int, error) {
	if p.closed {
		p.abort()
		return 0, syscall.EINVAL
	}
	if p.err != nil {
		p.abort()
		return 0, p.err
	}
	nw := 0
	for nw < len(b) {
		if p.buf == nil {
			p.buf = p.sp.Get()
			if int64(cap(p.buf)) < p.bufsz {
				p.buf = make([]byte, p.bufsz)
				runtime.GC()
			}
		}
		n := copy(p.buf[p.bufbytes:], b[nw:])
		p.bufbytes += n
		nw += n

		if len(p.buf) == p.bufbytes {
			p.flush()
		}
	}
	return nw, nil
}

func (p *putter) flush() {
	p.wg.Add(1)
	part, err := p.addPart(p.buf[:p.bufbytes])
	if err != nil {
		p.err = err
	}
	p.buf, p.bufbytes = nil, 0

	p.ch <- part

	// if necessary, double buffer size every 2000 parts due to the 10000-part AWS limit
	// to reach the 5 Terabyte max object size, initial part size must be ~85 MB
	n := len(p.parts)
	if n%2000 == 0 && n < maxNPart && growPartSize(n, p.bufsz, p.putsz) {
		p.bufsz = min64(p.bufsz*2, maxPartSize)
		p.sp.SetBufferSize(p.bufsz) // update pool buffer size
		logger.debugPrintf("part size doubled to %d", p.bufsz)
	}
}

// newPart creates a new "multipart upload" part containing the bytes
// in `buf`, assigns it a part number, hashes its contents into
// `p.md5`, adds it to `p.xml.Part`, and returns it. It does not do
// anything to cause the part to get uploaded. FIXME: the part is
// returned even if there is an error hashing the data.
func (p *putter) addPart(buf []byte) (*s3client.Part, error) {
	p.putsz += int64(len(buf))
	part := &s3client.Part{
		Data:       buf,
		PartNumber: len(p.parts) + 1,
	}
	var err error
	part.MD5, part.SHA256, part.ETag, err = p.hashContent(part.Data)

	p.parts = append(p.parts, part)

	return part, err
}

func (p *putter) worker() {
	for part := range p.ch {
		err := p.client.UploadPart(p.uploadID, part)
		if err != nil {
			p.err = err
		}

		// Give the buffer back to the pool, first making sure
		// that its length is set to its full capacity:
		p.sp.Put(part.Data[:cap(part.Data)])
		part.Data = nil
		p.wg.Done()
	}
}

func (p *putter) Close() error {
	if p.closed {
		p.abort()
		return syscall.EINVAL
	}
	if p.err != nil {
		p.abort()
		return p.err
	}
	if p.bufbytes > 0 || // partial part
		len(p.parts) == 0 { // 0 length file
		p.flush()
	}
	p.wg.Wait()
	close(p.ch)
	p.closed = true
	p.sp.Close()

	// check p.err before completing
	if p.err != nil {
		p.abort()
		return p.err
	}

	eTag, err := p.client.CompleteMultipartUpload(p.uploadID, p.parts)
	if err != nil {
		p.abort()
		return err
	}
	p.eTag = eTag

	if err := p.checkMd5sOfParts(); err != nil {
		return err
	}

	if p.c.Md5Check {
		md5Path := fmt.Sprint(".md5", p.url.Path, ".md5")
		md5URL, err := p.b.url(md5Path, p.c)
		if err != nil {
			return err
		}
		sum := fmt.Sprintf("%x", p.md5.Sum(nil))
		logger.debugPrintln("md5: ", sum)
		logger.debugPrintln("md5Path: ", md5Path)
		// FIXME: should this error really be ignored?
		_ = p.client.PutMD5(md5URL, sum)
	}
	return nil
}

// checkMd5sOfParts checks the md5 hash of the concatenated part md5
// hashes against the returned ETag. More info:
// https://forums.aws.amazon.com/thread.jspa?messageID=456442&#456442
func (p *putter) checkMd5sOfParts() error {
	// Get the MD5 of the part checksums that we've been computing as
	// parts were added:
	calculatedMd5ofParts := fmt.Sprintf("%x", p.md5OfParts.Sum(nil))

	// Find the comparable hash in the ETag returned from S3:
	remoteMd5ofParts := p.eTag
	remoteMd5ofParts = strings.Split(remoteMd5ofParts, "-")[0]
	if len(remoteMd5ofParts) == 0 {
		return fmt.Errorf("Nil ETag")
	}

	if calculatedMd5ofParts != remoteMd5ofParts {
		return fmt.Errorf("MD5 hash of part hashes comparison failed. Hash from multipart complete header: %s."+
			" Calculated multipart hash: %s.", remoteMd5ofParts, calculatedMd5ofParts)
	}

	return nil
}

// Try to abort multipart upload. Do not error on failure.
func (p *putter) abort() {
	err := p.client.AbortMultipartUpload(p.uploadID)
	if err != nil {
		logger.Printf("Error aborting multipart upload: %v\n", err)
	}
}

// Md5 functions
func (p *putter) hashContent(buf []byte) (string, string, string, error) {
	m := md5.New()
	s := sha256.New()
	mw := io.MultiWriter(m, s, p.md5)
	if _, err := io.Copy(mw, bytes.NewReader(buf)); err != nil {
		return "", "", "", err
	}
	md5Sum := m.Sum(nil)
	shaSum := hex.EncodeToString(s.Sum(nil))
	etag := hex.EncodeToString(md5Sum)
	// add to checksum of all parts for verification on upload completion
	if _, err := p.md5OfParts.Write(md5Sum); err != nil {
		return "", "", "", err
	}
	return base64.StdEncoding.EncodeToString(md5Sum), shaSum, etag, nil
}

// returns true unless partSize is large enough
// to achieve maxObjSize with remaining parts
func growPartSize(partIndex int, partSize, putsz int64) bool {
	return (maxObjSize-putsz)/(maxNPart-int64(partIndex)) > partSize
}
