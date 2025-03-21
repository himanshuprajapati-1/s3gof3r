package s3gof3r

import (
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sync"
	"syscall"
	"time"

	"github.com/github/s3gof3r/internal/pool"
	"github.com/github/s3gof3r/internal/s3client"
)

const qWaitMax = 2

type getter struct {
	url   *url.URL
	b     *Bucket
	bufsz int64
	err   error

	chunkID    int
	rChunk     *chunk
	contentLen int64
	bytesRead  int64
	chunkTotal int

	readCh chan *chunk
	getCh  chan *chunk
	quit   chan struct{}

	workerAborted chan struct{}
	abortOnce     sync.Once

	qWait    map[int]*chunk
	qWaitLen uint
	cond     sync.Cond

	sp *pool.BufferPool

	closed bool
	c      *Config

	md5  hash.Hash
	cIdx int64
}

type chunk struct {
	id     int
	header http.Header
	start  int64
	size   int64
	b      []byte
}

func newGetter(getURL *url.URL, c *Config, b *Bucket) (io.ReadCloser, http.Header, error) {
	g := new(getter)
	g.url = getURL
	g.b = new(Bucket)
	*g.b = *b
	g.c = c.safeCopy(1)
	g.bufsz = g.c.PartSize

	g.getCh = make(chan *chunk)
	g.readCh = make(chan *chunk)
	g.quit = make(chan struct{})
	g.workerAborted = make(chan struct{})
	g.qWait = make(map[int]*chunk)
	g.b = b
	g.md5 = md5.New()
	g.cond = sync.Cond{L: &sync.Mutex{}}

	// use get instead of head for error messaging
	resp, err := g.retryRequest("GET", g.url.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, s3client.NewRespError(resp)
	}
	// Otherwise, we don't need the body:
	_ = resp.Body.Close()

	// Golang changes content-length to -1 when chunked transfer encoding / EOF close response detected
	if resp.ContentLength == -1 {
		return nil, nil, fmt.Errorf("Retrieving objects with undefined content-length " +
			" responses (chunked transfer encoding / EOF close) is not supported")
	}

	g.contentLen = resp.ContentLength
	g.chunkTotal = int((g.contentLen + g.bufsz - 1) / g.bufsz) // round up, integer division
	logger.debugPrintf("object size: %3.2g MB", float64(g.contentLen)/float64((1*mb)))

	g.sp = pool.NewBufferPool(bufferPoolLogger{}, g.bufsz)

	for i := 0; i < g.c.Concurrency; i++ {
		go g.worker()
	}
	go g.initChunks()
	return g, resp.Header, nil
}

func (g *getter) retryRequest(method, urlStr string, body io.ReadSeeker) (resp *http.Response, err error) {
	for i := 0; i < g.c.NTry; i++ {
		var req *http.Request
		req, err = http.NewRequest(method, urlStr, body)
		if err != nil {
			return
		}

		if body != nil {
			req.Header.Set(s3client.SHA256Header, s3client.SHA256Reader(body))
		}

		g.b.Sign(req)
		resp, err = g.c.Client.Do(req)
		if err == nil && resp.StatusCode >= 500 && resp.StatusCode <= 599 {
			time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond) // exponential back-off
			continue
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

func (g *getter) initChunks() {
	id := 0
	for i := int64(0); i < g.contentLen; {
		size := min64(g.bufsz, g.contentLen-i)
		c := &chunk{
			id: id,
			header: http.Header{
				"Range": {fmt.Sprintf("bytes=%d-%d",
					i, i+size-1)},
			},
			start: i,
			size:  size,
			b:     nil,
		}
		i += size
		id++
		g.getCh <- c
	}
	close(g.getCh)
}

func (g *getter) worker() {
	for c := range g.getCh {
		g.retryGetChunk(c)
		if g.err != nil {
			// tell Read() caller that 1 or more chunks can't be read; abort
			g.abortOnce.Do(func() { close(g.workerAborted) })
			break
		}
	}
}

func (g *getter) retryGetChunk(c *chunk) {
	var err error
	c.b = g.sp.Get()
	for i := 0; i < g.c.NTry; i++ {
		err = g.getChunk(c)
		if err == nil {
			return
		}
		logger.debugPrintf("error on attempt %d: retrying chunk: %v, error: %s", i, c.id, err)
		time.Sleep(time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond) // exponential back-off
	}
	select {
	case <-g.quit: // check for closed quit channel before setting error
		return
	default:
		g.err = err
	}
}

func (g *getter) getChunk(c *chunk) error {
	// ensure buffer is empty
	r, err := http.NewRequest("GET", g.url.String(), nil)
	if err != nil {
		return err
	}
	r.Header = c.header
	g.b.Sign(r)
	resp, err := g.c.Client.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != 206 && resp.StatusCode != 200 {
		return s3client.NewRespError(resp)
	}

	n, err := io.ReadAtLeast(resp.Body, c.b, int(c.size))
	if err != nil {
		_ = resp.Body.Close()
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}
	if int64(n) != c.size {
		return fmt.Errorf("chunk %d: Expected %d bytes, received %d",
			c.id, c.size, n)
	}
	g.readCh <- c

	// wait for qWait to drain before starting next chunk
	g.cond.L.Lock()
	for g.qWaitLen >= qWaitMax {
		if g.closed {
			return nil
		}
		g.cond.Wait()
	}
	g.cond.L.Unlock()
	return nil
}

func (g *getter) Read(p []byte) (int, error) {
	var err error
	if g.closed {
		return 0, syscall.EINVAL
	}
	if g.err != nil {
		return 0, g.err
	}
	nw := 0
	for nw < len(p) {
		if g.bytesRead == g.contentLen {
			return nw, io.EOF
		} else if g.bytesRead > g.contentLen {
			// Here for robustness / completeness
			// Should not occur as golang uses LimitedReader up to content-length
			return nw, fmt.Errorf("Expected %d bytes, received %d (too many bytes)",
				g.contentLen, g.bytesRead)
		}

		// If for some reason no more chunks to be read and bytes are off, error, incomplete result
		if g.chunkID >= g.chunkTotal {
			return nw, fmt.Errorf("Expected %d bytes, received %d and chunkID %d >= chunkTotal %d (no more chunks remaining)",
				g.contentLen, g.bytesRead, g.chunkID, g.chunkTotal)
		}

		if g.rChunk == nil {
			g.rChunk, err = g.nextChunk()
			if err != nil {
				return 0, err
			}
			g.cIdx = 0
		}

		n := copy(p[nw:], g.rChunk.b[g.cIdx:g.rChunk.size])
		g.cIdx += int64(n)
		nw += n
		g.bytesRead += int64(n)

		if g.cIdx >= g.rChunk.size { // chunk complete
			g.sp.Put(g.rChunk.b)
			g.chunkID++
			g.rChunk = nil
		}
	}
	return nw, nil
}

func (g *getter) nextChunk() (*chunk, error) {
	for {
		// first check qWait
		c := g.qWait[g.chunkID]
		if c != nil {
			delete(g.qWait, g.chunkID)
			g.cond.L.Lock()
			g.qWaitLen--
			g.cond.L.Unlock()
			g.cond.Signal() // wake up waiting worker goroutine
			if g.c.Md5Check {
				if _, err := g.md5.Write(c.b[:c.size]); err != nil {
					return nil, err
				}
			}
			return c, nil
		}
		// if next chunk not in qWait, read from channel
		select {
		case c := <-g.readCh:
			g.qWait[c.id] = c
			g.cond.L.Lock()
			g.qWaitLen++
			g.cond.L.Unlock()
		case <-g.workerAborted:
			return nil, g.err // worker aborted, quit
		case <-g.quit:
			return nil, g.err // fatal error, quit.
		}
	}
}

func (g *getter) Close() error {
	if g.closed {
		return syscall.EINVAL
	}
	g.closed = true
	g.sp.Close()
	close(g.quit)
	g.cond.Broadcast()
	if g.err != nil {
		return g.err
	}
	if g.bytesRead != g.contentLen {
		return fmt.Errorf("read error: %d bytes read. expected: %d", g.bytesRead, g.contentLen)
	}
	if g.c.Md5Check {
		if err := g.checkMd5(); err != nil {
			return err
		}
	}
	return nil
}

func (g *getter) checkMd5() error {
	calcMd5 := fmt.Sprintf("%x", g.md5.Sum(nil))
	md5Path := fmt.Sprint(".md5", g.url.Path, ".md5")
	md5Url, err := g.b.url(md5Path, g.c)
	if err != nil {
		return err
	}

	logger.debugPrintln("md5: ", calcMd5)
	logger.debugPrintln("md5Path: ", md5Path)
	resp, err := g.retryRequest("GET", md5Url.String(), nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf(
			"MD5 check failed: %s not found: %s", md5Url.String(), s3client.NewRespError(resp),
		)
	}

	givenMd5, err := ioutil.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if calcMd5 != string(givenMd5) {
		return fmt.Errorf("MD5 mismatch. given:%s calculated:%s", givenMd5, calcMd5)
	}
	return nil
}
