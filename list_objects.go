package s3gof3r

import (
	"context"
	"encoding/xml"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/github/s3gof3r/internal/s3client"
	"golang.org/x/sync/errgroup"
)

func newObjectLister(c *Config, b *Bucket, prefixes []string, maxKeys int) (*ObjectLister, error) {
	cCopy := *c
	cCopy.NTry = max(c.NTry, 1)
	cCopy.Concurrency = max(c.Concurrency, 1)

	bCopy := *b

	ctx, cancel := context.WithCancel(context.TODO())

	l := ObjectLister{
		cancel:   cancel,
		b:        &bCopy,
		c:        &cCopy,
		prefixCh: make(chan string, len(prefixes)),
		resultCh: make(chan []string, 1),
		maxKeys:  maxKeys,
	}

	// Enqueue all of the prefixes that we were given. This won't
	// block because we have initialized `prefixCh` to be long enough
	// to hold all of them. This has the added benefit that there is
	// no data race if the caller happens to modify the contents of
	// the slice after this call returns.
	for _, p := range prefixes {
		l.prefixCh <- p
	}
	close(l.prefixCh)

	eg, ctx := errgroup.WithContext(ctx)

	for i := 0; i < min(l.c.Concurrency, len(prefixes)); i++ {
		eg.Go(func() error {
			return l.worker(ctx)
		})
	}

	go func() {
		l.finalErr = eg.Wait()
		close(l.resultCh)
		l.cancel()
	}()

	return &l, nil
}

type ObjectLister struct {
	cancel context.CancelFunc

	b       *Bucket
	c       *Config
	maxKeys int

	prefixCh chan string
	resultCh chan []string

	// finalErr is set before closing `resultCh` if any of the workers
	// returned errors. Any subsequent calls to `Next()` report this
	// error.
	finalErr error

	// currentValue and currentErr are the "results" of the most
	// recent call to `Next()`.
	currentValue []string
	currentErr   error
}

func (l *ObjectLister) worker(ctx context.Context) error {
	for p := range l.prefixCh {
		var continuation string
	retries:
		for {
			res, err := l.retryListObjects(ctx, p, continuation)
			if err != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return err
				}
			}

			keys := make([]string, 0, len(res.Contents))
			for _, c := range res.Contents {
				keys = append(keys, c.Key)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case l.resultCh <- keys:
				continuation = res.NextContinuationToken
				if continuation != "" {
					continue
				}

				// Break from this prefix and grab the next one
				break retries
			}
		}
	}

	return nil
}

func (l *ObjectLister) retryListObjects(
	ctx context.Context, p, continuation string,
) (*listBucketResult, error) {
	var err error
	var res *listBucketResult
	var timer *time.Timer
	for i := 0; i < l.c.NTry; i++ {
		opts := listObjectsOptions{MaxKeys: l.maxKeys, Prefix: p, ContinuationToken: continuation}
		res, err = listObjects(l.c, l.b, opts)
		if err == nil {
			return res, nil
		}

		// Exponential back-off, reusing the timer if possible:
		duration := time.Duration(math.Exp2(float64(i))) * 100 * time.Millisecond
		if timer == nil {
			timer = time.NewTimer(duration)
		} else {
			// The only way to get here is if the timer was created
			// during an earlier iteration of the loop, in which case
			// the select below must have gone through the `<-timer.C`
			// branch, which drained the timer. So it is safe to call
			// `Reset()`:
			timer.Reset(duration)
		}

		select {
		case <-timer.C:
			// Timer has fired and been drained, so it is ready for reuse.
		case <-ctx.Done():
			// Stop the timer to prevent a resource leak:
			timer.Stop()
			return nil, ctx.Err()
		}
	}

	return nil, err
}

// Next moves the iterator to the next set of results. It returns true if there
// are more results, or false if there are no more results or there was an
// error.
func (l *ObjectLister) Next() bool {
	var ok bool
	l.currentValue, ok = <-l.resultCh
	if !ok {
		// If there has been an error, we now show it to the caller:
		l.currentErr = l.finalErr
		return false
	}

	return true
}

func (l *ObjectLister) Value() []string {
	return l.currentValue
}

func (l *ObjectLister) Error() error {
	return l.currentErr
}

func (l *ObjectLister) Close() {
	l.cancel()
}

// ListObjectsOptions specifies the options for a ListObjects operation on a S3
// bucket
type listObjectsOptions struct {
	// Maximum number of keys to return per request
	MaxKeys int
	// Only list those keys that start with the given prefix
	Prefix string
	// Continuation token from the previous request
	ContinuationToken string
}

type listBucketResult struct {
	Name                  string               `xml:"Name"`
	Prefix                string               `xml:"Prefix"`
	KeyCount              int                  `xml:"KeyCount"`
	MaxKeys               int                  `xml:"MaxKeys"`
	IsTruncated           bool                 `xml:"IsTrucated"`
	NextContinuationToken string               `xml:"NextContinuationToken"`
	Contents              []listBucketContents `xml:"Contents"`
}

type listBucketContents struct {
	Key            string         `xml:"Key"`
	LastModified   time.Time      `xml:"LastModified"`
	ETag           string         `xml:"ETag"`
	Size           int64          `xml:"Size"`
	StorageClass   string         `xml:"StorageClass"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type ListObjectsResult struct {
	result *listBucketResult
}

func listObjects(c *Config, b *Bucket, opts listObjectsOptions) (*listBucketResult, error) {
	result := new(listBucketResult)
	u, err := b.url("", c)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("list-type", "2")
	if opts.MaxKeys > 0 {
		q.Set("max-keys", strconv.Itoa(opts.MaxKeys))
	}
	if opts.Prefix != "" {
		q.Set("prefix", opts.Prefix)
	}
	if opts.ContinuationToken != "" {
		q.Set("continuation-token", opts.ContinuationToken)
	}
	u.RawQuery = q.Encode()

	r := http.Request{
		Method: "GET",
		URL:    u,
	}
	b.Sign(&r)

	resp, err := b.conf().Do(&r)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, s3client.NewRespError(resp)
	}

	err = xml.NewDecoder(resp.Body).Decode(result)
	closeErr := resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}

	return result, nil
}
