package batcher

import (
	"container/list"
	"sync"
	"time"
)

// buffer is a buffer of lines that can be unlimited in size
type buffer struct {
	maxSize     int
	batchSize   int
	maxWaitTime time.Duration
	lock        sync.Mutex
	batchReady  *sync.Cond
	data        list.List
	closed      bool
}

// newBuffer creates a new buffer. maxSize is the maximum size of the
// buffer. maxSize <= 0 means buffer is of unlimited size. batchSize is
// the maximum batch size which must be positive. If maxSize is positive
// and batchSize > maxSize, maxSize becomes batchSize. maxWaitTime is the
// maximum amount of time Batch() will block in an attempt to get
// batchSize lines. maxWaitTime <= 0 means wait forever to get batchSize
// lines.
func newBuffer(maxSize, batchSize int, maxWaitTime time.Duration) *buffer {
	if batchSize <= 0 {
		panic("batchSize must be positive.")
	}
	if maxSize > 0 && batchSize > maxSize {
		maxSize = batchSize
	}
	result := &buffer{
		maxSize:     maxSize,
		batchSize:   batchSize,
		maxWaitTime: maxWaitTime,
	}
	result.batchReady = sync.NewCond(&result.lock)
	result.data.Init()

	// This goroutine signals the batchReady condition variable periodically
	// so that Batch can check if enough time has elapsed.
	if maxWaitTime > 0 {
		go result.signalBatchPeriodically()
	}

	return result
}

// Add adds a line to buffer. Add returns false if buffer is full.
// Add panics if buffer is closed.
func (b *buffer) Add(line string) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed {
		panic("Buffer already closed")
	}
	if b.maxSize > 0 && b.data.Len() >= b.maxSize {
		return false
	}
	b.data.PushBack(line)
	if b.data.Len() >= b.batchSize {
		// wake up goroutine blocking on Batch() when we have enough lines.
		b.batchReady.Signal()
	}
	return true
}

// Batch blocks until it can drain batchSize lines from the buffer or
// until maxWaitTime time passes. Batch returns the lines drained. If
// the buffer is closed, Batch does no blocking. scratch points to a slice
// that backs what is returned. If this buffer is both closed and empty,
// Batch returns nil immediately.
func (b *buffer) Batch(scratch *[]string) []string {
	// We reserve nil for a closed, empty buffer.
	if *scratch == nil {
		*scratch = make([]string, 0)
	}
	*scratch = (*scratch)[:0]
	ctime := time.Now()
	b.lock.Lock()
	defer b.lock.Unlock()
	for !b.closed && b.data.Len() < b.batchSize && (b.maxWaitTime <= 0 || time.Now().Sub(ctime) < b.maxWaitTime) {
		b.batchReady.Wait()
	}
	if b.closed && b.data.Len() == 0 {
		return nil
	}
	size := b.data.Len()
	if size > b.batchSize {
		size = b.batchSize
	}
	for i := 0; i < size; i++ {
		*scratch = append(*scratch, b.data.Remove(b.data.Front()).(string))
	}
	return *scratch
}

// All drains and returns all lines currently in the buffer without blocking.
// scratch points to a slice that backs what is returned. If this buffer is
// both closed and empty, All returns nil.
func (b *buffer) All(scratch *[]string) []string {
	// We reserve nil for a closed, empty buffer.
	if *scratch == nil {
		*scratch = make([]string, 0)
	}
	*scratch = (*scratch)[:0]
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed && b.data.Len() == 0 {
		return nil
	}
	size := b.data.Len()
	for i := 0; i < size; i++ {
		*scratch = append(*scratch, b.data.Remove(b.data.Front()).(string))
	}
	return *scratch
}

// Close closes this buffer so that it no longer accepts new lines.
func (b *buffer) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.closed = true

	// Wake up all goroutines waiting on Batch since the periodic signaling
	// is going away
	b.batchReady.Broadcast()
}

func (b *buffer) signalBatchPeriodically() {
	for {
		time.Sleep(b.maxWaitTime / 10)
		if !b.signalBatch() {
			return
		}
	}
}

func (b *buffer) signalBatch() bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed {
		return false
	}
	b.batchReady.Signal()
	return true
}
