package pool

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
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

type qb struct {
	when time.Time
	s    []byte
}

type BufferPool struct {
	makes     uint64
	get       chan []byte
	give      chan []byte
	quit      chan struct{}
	timeout   time.Duration
	sizech    chan int64
	timeoutCh chan time.Duration
	wg        sync.WaitGroup
}

type logger interface {
	Printf(format string, a ...interface{})
}

func NewBufferPool(logger logger, bufsz int64) (sp *BufferPool) {
	sp = &BufferPool{
		get:       make(chan []byte),
		give:      make(chan []byte),
		quit:      make(chan struct{}),
		timeout:   time.Minute,
		sizech:    make(chan int64),
		timeoutCh: make(chan time.Duration),
	}

	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()

		q := new(list.List)
		for {
			if q.Len() == 0 {
				q.PushFront(qb{when: time.Now(), s: make([]byte, bufsz)})
				atomic.AddUint64(&sp.makes, 1)
			}

			e := q.Front()

			// Discard `e`, but not if it's the only item in `q`
			// (otherwise we'll just create it again the next time
			// through the loop):
			timeout := time.NewTimer(sp.timeout)
			var stale <-chan time.Time
			if q.Len() > 1 {
				stale = timeout.C
			}

			select {
			case b := <-sp.give:
				timeout.Stop()
				q.PushFront(qb{when: time.Now(), s: b})
			case sp.get <- e.Value.(qb).s:
				timeout.Stop()
				q.Remove(e)
			case <-stale:
				// free unused slices older than timeout
				e := q.Front()
				for e != nil {
					n := e.Next()
					if time.Since(e.Value.(qb).when) > sp.timeout {
						q.Remove(e)
						e.Value = nil
					}
					e = n
				}
			case sz := <-sp.sizech: // update buffer size, free buffers
				bufsz = sz
			case timeout := <-sp.timeoutCh:
				sp.timeout = timeout
			case <-sp.quit:
				logger.Printf("%d buffers of %d MB allocated", sp.makes, bufsz/(1*mb))
				return
			}
		}

	}()
	return sp
}

func (bp *BufferPool) Get() []byte {
	return <-bp.get
}

func (bp *BufferPool) Put(buf []byte) {
	bp.give <- buf
}

func (bp *BufferPool) Close() {
	close(bp.quit)
	bp.wg.Wait()
}

func (bp *BufferPool) SetBufferSize(bufsz int64) {
	bp.sizech <- bufsz
}

func (bp *BufferPool) SetTimeout(timeout time.Duration) {
	bp.timeoutCh <- timeout
}

func (bp *BufferPool) AllocationCount() uint64 {
	return atomic.LoadUint64(&bp.makes)
}
