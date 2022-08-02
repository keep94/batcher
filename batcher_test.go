package batcher

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
)

var (
	errConsuming = errors.New("error Consuming")
)

func TestNormal(t *testing.T) {
	mockClock := newMockClock()
	consumer := newMockConsumer()
	batcher := New(consumer.Consume, BufferSize(0), BatchSize(5))
	batcher.Add("1")
	batcher.Add("2")
	batcher.Add("3")
	batcher.Add("4")
	batcher.Add("5")
	batcher.Add("6")
	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, consumer.Taken())
	batcher.Add("7")
	batcher.Add("8")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.NoError(t, batcher.Close())
		wg.Done()
	}()
	assert.Equal(t, []string{"6", "7", "8"}, consumer.Taken())
	wg.Wait()
	consumer.Close()
	mockClock.Close()
}

func TestBackoff(t *testing.T) {
	mockClock := newMockClock()
	consumer := newMockConsumer()
	batcher := newForTesting(
		consumer.Consume,
		mockClock,
		FlushInterval(100*time.Millisecond),
		BackOffStrategy(
			backoff.WithMaxRetries(
				&backoff.ConstantBackOff{Interval: 6 * time.Second},
				2,
			),
		))
	batcher.Add("1")
	batcher.Add("2")
	batcher.Add("3")
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 6*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 6*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.NoError(t, batcher.Close())
	consumer.Close()
	mockClock.Close()
}

func TestNoBackoff(t *testing.T) {
	mockClock := newMockClock()
	consumer := newMockConsumer()
	batcher := newForTesting(
		consumer.Consume,
		mockClock,
		FlushInterval(100*time.Millisecond))
	batcher.Add("1")
	batcher.Add("2")
	batcher.Add("3")
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.NoError(t, batcher.Close())
	consumer.Close()
	mockClock.Close()
}

func TestFlush(t *testing.T) {
	consumer := newMockConsumer()
	batcher := New(consumer.Consume, FlushInterval(0), BatchSize(3))
	batcher.Add("1")
	batcher.Add("2")
	batcher.Add("3")
	batcher.Add("4")

	// Make sure no autoflushing is going on
	time.Sleep(100 * time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assert.Nil(t, batcher.Flush())
		wg.Done()
	}()
	assert.Equal(t, []string{"1", "2", "3"}, consumer.Taken())
	assert.Equal(t, []string{"4"}, consumer.Taken())
	wg.Wait()
}

type mockClock struct {
	ch chan time.Duration
}

func newMockClock() *mockClock {
	return &mockClock{ch: make(chan time.Duration)}
}

func (m *mockClock) Sleep(dur time.Duration) {
	m.ch <- dur
}

func (m *mockClock) WaitAwhile() time.Duration {
	return <-m.ch
}

func (m *mockClock) Close() {
	close(m.ch)
}

type mockConsumer struct {
	resultCh chan bool
	strsCh   chan []string
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{
		resultCh: make(chan bool, 1),
		strsCh:   make(chan []string, 1),
	}
}

func (m *mockConsumer) Taken() []string {
	m.resultCh <- false
	return <-m.strsCh
}

func (m *mockConsumer) TakenRequestRetry() []string {
	m.resultCh <- true
	return <-m.strsCh
}

func (m *mockConsumer) Consume(strs []string) (error, bool) {
	strCopy := make([]string, len(strs))
	copy(strCopy, strs)
	m.strsCh <- strCopy
	if <-m.resultCh {
		return errConsuming, true
	}
	return nil, false
}

func (m *mockConsumer) Close() {
	close(m.strsCh)
	close(m.resultCh)
}
