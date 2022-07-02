package batcher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNormal(t *testing.T) {
	mockClock := newMockClock()
	consumer := newMockConsumer()
	batcher := newForTesting(consumer.Consume, mockClock, BatchSize(5))
	batcher.Add("1")
	batcher.Add("2")
	batcher.Add("3")
	batcher.Add("4")
	batcher.Add("5")
	batcher.Add("6")
	assert.Equal(t, time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, consumer.Taken())
	batcher.Add("7")
	batcher.Add("8")
	assert.Equal(t, time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"6", "7", "8"}, consumer.Taken())
	assert.NoError(t, batcher.Close())
	consumer.Close()
	mockClock.Close()
}

func TestExponentialBackoff(t *testing.T) {
	mockClock := newMockClock()
	consumer := newMockConsumer()
	batcher := newForTesting(consumer.Consume, mockClock)
	batcher.Add("1")
	batcher.Add("2")
	batcher.Add("3")
	assert.Equal(t, time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 2*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 4*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 8*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 16*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 32*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 60*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.TakenRequestRetry())
	assert.Equal(t, 60*time.Second, mockClock.WaitAwhile())
	assert.Equal(t, []string{"1", "2", "3"}, consumer.Taken())
	assert.NoError(t, batcher.Close())
	consumer.Close()
	mockClock.Close()

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

func (m *mockConsumer) Consume(strs []string) bool {
	strCopy := make([]string, len(strs))
	copy(strCopy, strs)
	m.strsCh <- strCopy
	return <-m.resultCh
}

func (m *mockConsumer) Close() {
	close(m.strsCh)
	close(m.resultCh)
}
