package batcher

import (
	"time"
)

// Type Consumer consumes a batch of lines. A non-nil error and a retry of
// true means that the function call should be retried.
type Consumer func(strs []string) (err error, retry bool)

// Option is used to configure a Batcher.
type Option interface {
	apply(cfg *config)
}

// BatchSize configures the maximum batch size. Default is 10,000.
// n must be positive.
func BatchSize(n int) Option {
	if n <= 0 {
		panic("BatchSize must be positive")
	}
	return optionFunc(func(cfg *config) {
		cfg.batchSize = n
	})
}

// BufferSize configures the maximum buffer size. Default is 50,000.
// A buffer size of 0 means unlimited size. n must be non-negative.
func BufferSize(n int) Option {
	if n < 0 {
		panic("BufferSize must be non-negative")
	}
	return optionFunc(func(cfg *config) {
		cfg.bufferSize = n
	})
}

// InitialInterval configures the initial amount of time to wait before
// a retry. Default is 5 seconds. d must be positive.
func InitialInterval(d time.Duration) Option {
	if d <= 0 {
		panic("InitialInterval must be positive")
	}
	return optionFunc(func(cfg *config) {
		cfg.initialInterval = d
	})
}

// MaxInterval configures the maximum amount of time to wait before a retry.
// default is 1 minute. 0 means no maximum. d must be non-negative.
func MaxInterval(d time.Duration) Option {
	if d < 0 {
		panic("MaxInterval must be non-negative")
	}
	return optionFunc(func(cfg *config) {
		cfg.maxInterval = d
	})
}

// MaxDuration configures the total amount of waiting time before giving up
// on retries. Default is 5 minutes. 0 means retries are disabled. d
// must be non-negative
func MaxDuration(d time.Duration) Option {
	if d < 0 {
		panic("MaxDuration must be non-negative")
	}
	return optionFunc(func(cfg *config) {
		cfg.maxDuration = d
	})
}

// FlushInterval sets the maximum amount of time to wait between sending
// lines. If there is already a full batch of lines to send, no waiting is
// done. The default is 1 second. 0 means auto-flushing is turned off.
func FlushInterval(d time.Duration) Option {
	if d < 0 {
		panic("FlushInterval must be non-negative")
	}
	return optionFunc(func(cfg *config) {
		cfg.flushInterval = d
	})
}

// Batcher batches lines to be consumed by a Consumer
type Batcher struct {
	consumer        Consumer
	buf             *buffer
	batchSize       int
	initialInterval time.Duration
	maxInterval     time.Duration
	maxDuration     time.Duration
	clck            clock
	done            chan struct{}
}

// New creates a new Batcher. consumer consumes the values added to this
// Batcher in order.
func New(consumer Consumer, options ...Option) *Batcher {
	return newForTesting(consumer, systemClock{}, options...)
}

func newForTesting(consumer Consumer, clck clock, options ...Option) *Batcher {
	cfg := config{
		initialInterval: 5 * time.Second,
		maxInterval:     time.Minute,
		maxDuration:     5 * time.Minute,
		bufferSize:      50000,
		batchSize:       10000,
		flushInterval:   time.Second,
	}
	for _, opt := range options {
		opt.apply(&cfg)
	}
	cfg.fixUp()
	result := &Batcher{
		consumer:        consumer,
		buf:             newBuffer(cfg.bufferSize, cfg.batchSize, cfg.flushInterval),
		batchSize:       cfg.batchSize,
		initialInterval: cfg.initialInterval,
		maxInterval:     cfg.maxInterval,
		maxDuration:     cfg.maxDuration,
		clck:            clck,
		done:            make(chan struct{}),
	}
	if cfg.flushInterval > 0 {
		go result.loop()
	} else {
		close(result.done)
	}
	return result
}

// Add adds a single line to this batcher.
// Add returns true if line was taken or false if buffer full.
func (b *Batcher) Add(s string) bool {
	return b.buf.Add(s)
}

// Close closes this Batcher. Close blocks until all in flight lines are
// consumed.
func (b *Batcher) Close() error {
	b.buf.Close()
	<-b.done
	return nil
}

// Flush flushes this batcher. It is intended to be called only when
// auto flushing is turned off. Flush does no internal retrying on error.
func (b *Batcher) Flush() error {
	var scratch []string
	allLines := b.buf.All(&scratch)
	for length := len(allLines); length > 0; length = len(allLines) {
		if length > b.batchSize {
			length = b.batchSize
		}
		err, _ := b.consumer(allLines[0:length])
		if err != nil {
			return err
		}
		allLines = allLines[length:]
	}
	return nil
}

func (b *Batcher) loop() {
	scratch := make([]string, 0, b.batchSize)
	for {
		lines := b.buf.Batch(&scratch)
		if lines == nil {
			close(b.done)
			return
		}
		if len(lines) == 0 {
			continue
		}
		delay := b.initialInterval
		var totalDelay time.Duration
		for err, retry := b.consumer(lines); err != nil; err, retry = b.consumer(lines) {
			// Maybe log the error, but how?

			if !retry || totalDelay >= b.maxDuration {
				break
			}
			b.clck.Sleep(delay)
			totalDelay += delay
			delay *= 2
			if delay > b.maxInterval {
				delay = b.maxInterval
			}
		}
	}
}

type optionFunc func(cfg *config)

func (o optionFunc) apply(cfg *config) {
	o(cfg)
}

type config struct {
	initialInterval time.Duration
	maxInterval     time.Duration
	maxDuration     time.Duration
	bufferSize      int
	batchSize       int
	flushInterval   time.Duration
}

func (c *config) fixUp() {
	if c.maxInterval > 0 && c.initialInterval > c.maxInterval {
		c.maxInterval = c.initialInterval
	}
	if c.bufferSize > 0 && c.batchSize > c.bufferSize {
		c.bufferSize = c.batchSize
	}
}

type clock interface {
	Sleep(dur time.Duration)
}

type systemClock struct {
}

func (s systemClock) Sleep(dur time.Duration) {
	time.Sleep(dur)
}
