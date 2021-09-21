package s3gof3r

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
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
	"golang.org/x/sync/errgroup"
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

// putter is an `io.Writer` that uploads the data written to it to an
// S3 blob.
//
// Data flow for data written via `putter.Write()`:
//
//                                                                receive          putPart()
//                                                                      +----------+     +----+
//                                                                    > | worker() | --> | S3 |
//            p.pw.Write()      pr.Read()             send           /  +----------+     +----+
//     +--------+     +---------+     +--------------+     +------+ /   +----------+     +----+
//     | caller | --> | io.Pipe | --> | queueParts() | --> |  ch  | --> | worker() | --> | S3 |
//     +--------+     +---------+     +--------------+     +------+ \   +----------+     +----+
//                                          |                        \  +----------+     +----+
//                                          |                         > | worker() | --> | S3 |
//                                     hashContent()                    +----------+     +----+
//                                          |
//                                          v
//                                      +-------+                Close()                 +----+
//                                      | p.xml | -------------------------------------> | S3 |
//                                      +-------+                                        +----+
//
// The normal shutdown sequence:
//
// * The caller invokes `p.Close()`.
//
// * This closes `p.pr`, the write end of the pipe, which causes
//   `queueParts()` to read an EOF and return.
//
// * The `queueParts()` goroutine closes the read end of the pipe
//   (which makes any future calls to `p.Write()` fail) and closes
//   `p.ch`.
//
// * The closure of `p.ch` causes the `worker()` invocations to
//   return.
//
// * When all of the above goroutines finish, `p.eg.Wait()` returns,
//   allowing the `CompleteMultipartUpload` step to proceed.
//
// If an error occurs in one of the goroutines, the goroutine returns
// an error, which causes the `errgroup.Group` to cancel its context,
// causing most of the other goroutines to exit promptly. The only
// tricky one is `queueParts()`, which might be blocked reading from
// the read end of the pipe. So an extra goroutine waits on
// `ctx.Done()` and then closes the write end of the pipe if it hasn't
// already been closed by `p.Close()`.
type putter struct {
	cancel context.CancelFunc

	url    *url.URL
	b      *Bucket
	c      *Config
	client s3Putter

	pw *io.PipeWriter

	bufsz      int64
	closeOnce  sync.Once
	eg         *errgroup.Group
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
func newPutter(url *url.URL, h http.Header, c *Config, b *Bucket) (*putter, error) {
	ctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(ctx)

	cCopy := *c
	cCopy.Concurrency = max(c.Concurrency, 1)
	cCopy.NTry = max(c.NTry, 1)
	bCopy := *b
	bufsz := max64(minPartSize, cCopy.PartSize)
	p := putter{
		cancel:     cancel,
		url:        url,
		c:          &cCopy,
		b:          &bCopy,
		bufsz:      bufsz,
		eg:         eg,
		md5OfParts: md5.New(),
		md5:        md5.New(),
	}

	p.client = s3client.New(p.url, p.b, p.c.Client, p.c.NTry, bufferPoolLogger{})

	var err error
	p.uploadID, err = p.client.StartMultipartUpload(h)
	if err != nil {
		p.cancel()
		return nil, err
	}

	p.sp = pool.NewBufferPool(bufferPoolLogger{}, bufsz)
	pr, pw := io.Pipe()
	p.pw = pw

	ch := make(chan *s3client.Part)

	p.eg.Go(func() error {
		err = p.queueParts(ctx, pr, ch)
		p.closeOnce.Do(func() { pr.Close() })
		close(ch)
		return err
	})

	// Close `p.pw` to unblock `queueParts()` (which might be waiting
	// on `pr.Read()`) if the context is cancelled before `p.Close()`
	// is called. This also prevents any more successful calls to
	// `Write()`.
	go func() {
		<-ctx.Done()
		p.closeOnce.Do(func() {
			p.pw.CloseWithError(errors.New("upload aborted"))
		})
	}()

	for i := 0; i < p.c.Concurrency; i++ {
		p.eg.Go(func() error { return p.worker(ch) })
	}

	return &p, nil
}

func (p *putter) Write(b []byte) (int, error) {
	n, err := p.pw.Write(b)
	if err == io.ErrClosedPipe {
		// For backwards compatibility:
		err = syscall.EINVAL
	}
	return n, err
}

// queueParts reads from `r`, breaks the input into parts of size (at
// most) `p.bufsz`, adds the data to the hash, and passes each part to
// `p.ch` to be uploaded by the workers. It terminates when it has
// exausted the input or experiences a read error.
func (p *putter) queueParts(ctx context.Context, r io.Reader, ch chan<- *s3client.Part) error {
	for {
		buf := p.sp.Get()
		if int64(cap(buf)) != p.bufsz {
			buf = make([]byte, p.bufsz)
			runtime.GC()
		}
		n, err := io.ReadFull(r, buf)
		lastPart := false
		switch err {
		case nil:
			// No error. Send this part then continue looping.
		case io.EOF:
			if len(p.parts) > 0 {
				// There was an EOF immediately after the previous
				// part. This new part would be empty, so we don't
				// have to send it.
				return nil
			}
			// The file was zero length. In this case, we have to
			// upload the zero-length part, but then we're done:
			lastPart = true
		case io.ErrUnexpectedEOF:
			// The input was exhausted but only partly filled this
			// part. Send what we have, then we're done.
			lastPart = true
		default:
			// There was some other kind of error:
			return err
		}

		part, err := p.addPart(buf[:n])
		if err != nil {
			return err
		}

		select {
		case ch <- part:
		case <-ctx.Done():
			return ctx.Err()
		}

		if lastPart {
			return nil
		}

		// if necessary, double buffer size every 2000 parts due to the 10000-part AWS limit
		// to reach the 5 Terabyte max object size, initial part size must be ~85 MB
		count := len(p.parts)
		if count%2000 == 0 && count < maxNPart && growPartSize(count, p.bufsz, p.putsz) {
			p.bufsz = min64(p.bufsz*2, maxPartSize)
			p.sp.SetBufferSize(p.bufsz) // update pool buffer size
			logger.debugPrintf("part size doubled to %d", p.bufsz)
		}
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

// worker receives parts from `p.ch` that are ready to upload, and
// uploads them to S3 as file parts. Then it recycles the part's
// buffer back to the buffer pool.
func (p *putter) worker(ch <-chan *s3client.Part) error {
	for part := range ch {
		err := p.client.UploadPart(p.uploadID, part)
		if err != nil {
			return err
		}

		// Give the buffer back to the pool, first making sure
		// that its length is set to its full capacity:
		p.sp.Put(part.Data[:cap(part.Data)])
		part.Data = nil
	}
	return nil
}

func (p *putter) Close() error {
	defer p.cancel()
	defer p.sp.Close()

	cleanup := func() {
		p.cancel()
		p.eg.Wait()
		if p.uploadID != "" {
			err := p.client.AbortMultipartUpload(p.uploadID)
			if err != nil {
				logger.Printf("Error aborting multipart upload: %v\n", err)
			}
		}
	}

	// Closing `p.pw` prevents any future `Write()` calls from
	// succeeding and tells `queueParts()` that no more data is
	// coming:
	var err error
	p.closeOnce.Do(func() {
		err = p.pw.Close()
	})
	if err != nil {
		cleanup()
		return errors.New("unexpected error closing internal pipe")
	}

	err = p.eg.Wait()
	if err != nil {
		cleanup()
		return err
	}

	eTag, err := p.client.CompleteMultipartUpload(p.uploadID, p.parts)
	if err != nil {
		cleanup()
		return err
	}
	p.eTag = eTag

	if err := p.checkMd5sOfParts(); err != nil {
		cleanup()
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
