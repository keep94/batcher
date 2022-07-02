package batcher

import (
	"time"
)

// Type Consumer consumes a batch of lines. It returns true if the same
// batch should be retried or false otherwise.
type Consumer func(strs []string) bool

// Option is used to configure a Batcher.
type Option interface {
	apply(cfg *config)
}

// BatchSize configures the maximum batch size. Default is 10,000
func BatchSize(n int) Option {
	return optionFunc(func(cfg *config) {
		cfg.batchSize = n
	})
}

type Batcher struct {
	consumer Consumer
	buffer   chan string
	batch    []string
	delay    time.Duration
	maxDelay time.Duration
	clck     clock
	done     chan struct{}
}

// New creates a new Batcher. consumer consumes the values added to this
// Batcher in order.
func New(consumer Consumer, options ...Option) *Batcher {
	return newForTesting(consumer, systemClock{}, options...)
}

func newForTesting(consumer Consumer, clck clock, options ...Option) *Batcher {
	cfg := config{
		delay:      time.Second,
		maxDelay:   time.Minute,
		bufferSize: 50000,
		batchSize:  10000,
	}
	for _, opt := range options {
		opt.apply(&cfg)
	}
	result := &Batcher{
		consumer: consumer,
		buffer:   make(chan string, cfg.bufferSize),
		batch:    make([]string, 0, cfg.batchSize),
		delay:    cfg.delay,
		maxDelay: cfg.maxDelay,
		clck:     clck,
		done:     make(chan struct{}),
	}
	go result.loop()
	return result
}

// Add adds a single line to this batcher.
// Add returns true if line was taken or false if buffer full.
func (b *Batcher) Add(s string) bool {
	select {
	case b.buffer <- s:
		return true
	default:
		return false
	}
}

// Close closes this Batcher. Close blocks until all in flight lines are
// consumed.
func (b *Batcher) Close() error {
	close(b.buffer)
	<-b.done
	return nil
}

func (b *Batcher) loop() {
	for {
		first, ok := <-b.buffer
		if !ok {
			close(b.done)
			return
		}
		b.batch = append(b.batch, first)
		b.clck.Sleep(b.delay)
		maybeReadChunk(b.buffer, &b.batch)
		delay := b.delay
		for b.consumer(b.batch) {
			delay *= 2
			if delay > b.maxDelay {
				delay = b.maxDelay
			}
			b.clck.Sleep(delay)
			maybeReadChunk(b.buffer, &b.batch)
		}
		b.batch = b.batch[:0]
	}
}

func maybeReadChunk(ch <-chan string, strs *[]string) {
	for len(*strs) < cap(*strs) {
		select {
		case s, ok := <-ch:
			if !ok {
				return
			}
			*strs = append(*strs, s)
		default:
			return
		}
	}
}

type optionFunc func(cfg *config)

func (o optionFunc) apply(cfg *config) {
	o(cfg)
}

type config struct {
	delay      time.Duration
	maxDelay   time.Duration
	bufferSize int
	batchSize  int
}

type clock interface {
	Sleep(dur time.Duration)
}

type systemClock struct {
}

func (s systemClock) Sleep(dur time.Duration) {
	time.Sleep(dur)
}
