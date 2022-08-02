package batcher

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBufferFastData(t *testing.T) {
	b := newBuffer(6, 3, time.Second)
	assert.True(t, b.Add("1"))
	assert.True(t, b.Add("2"))
	assert.True(t, b.Add("3"))
	assert.True(t, b.Add("4"))
	assert.True(t, b.Add("5"))
	assert.True(t, b.Add("6"))
	assert.False(t, b.Add("7"))
	var scratch []string
	ctime := time.Now()
	assert.Equal(t, []string{"1", "2", "3"}, b.Batch(&scratch))
	b.Close()
	assert.Equal(t, []string{"4", "5", "6"}, b.Batch(&scratch))
	assert.Less(t, time.Now().Sub(ctime), 10*time.Millisecond)
	assert.Nil(t, b.Batch(&scratch))
}

func TestBufferNoData(t *testing.T) {
	b := newBuffer(0, 3, 50*time.Millisecond)
	var scratch []string
	ctime := time.Now()
	assert.Equal(t, []string{}, b.Batch(&scratch))
	d := time.Now().Sub(ctime)
	assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	assert.Less(t, d, 60*time.Millisecond)
	b.Close()
	assert.Nil(t, b.Batch(&scratch))
}

func TestBufferBigBatchSize(t *testing.T) {
	b := newBuffer(3, 5, 0)
	go func() {
		time.Sleep(10 * time.Millisecond)
		b.Add("1")
		b.Add("2")
		b.Add("3")
		b.Add("4")
		b.Add("5")
	}()
	var scratch []string
	ctime := time.Now()
	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, b.Batch(&scratch))
	assert.Less(t, time.Now().Sub(ctime), 20*time.Millisecond)
	b.Close()
}

func TestBufferClose(t *testing.T) {
	b := newBuffer(0, 3, time.Second)
	b.Close()
	var scratch []string
	ctime := time.Now()
	assert.Nil(t, b.Batch(&scratch))
	assert.Less(t, time.Now().Sub(ctime), 10*time.Millisecond)
	assert.Panics(t, func() { b.Add("1") })
}

func TestAll(t *testing.T) {
	b := newBuffer(0, 3, time.Second)
	b.Add("1")
	b.Add("2")
	b.Add("3")
	b.Add("4")
	b.Add("5")
	b.Add("6")
	b.Add("7")
	b.Add("8")
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8"}, b.All())
	b.Add("9")
	assert.Equal(t, []string{"9"}, b.All())
	assert.Equal(t, []string{}, b.All())
	b.Close()
	assert.Nil(t, b.All())
}
