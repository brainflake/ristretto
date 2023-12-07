package ristretto

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPolicy(t *testing.T) {
	defer func() {
		require.Nil(t, recover())
	}()
	newPolicy(100, 10)
}

func TestPolicyMetrics(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.CollectMetrics(newMetrics())
	require.NotNil(t, p.metrics)
	require.NotNil(t, p.evict.metrics)
}

func TestPolicyProcessItems(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.itemsCh <- []uint64{1, 2, 2}
	time.Sleep(wait)
	p.Lock()
	require.Equal(t, int64(2), p.admit.Estimate(2))
	require.Equal(t, int64(1), p.admit.Estimate(1))
	p.Unlock()

	p.stop <- struct{}{}
	p.itemsCh <- []uint64{3, 3, 3}
	time.Sleep(wait)
	p.Lock()
	require.Equal(t, int64(0), p.admit.Estimate(3))
	p.Unlock()
}

func TestPolicyPush(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	require.True(t, p.Push([]uint64{}))

	keepCount := 0
	for i := 0; i < 10; i++ {
		if p.Push([]uint64{1, 2, 3, 4, 5}) {
			keepCount++
		}
	}
	require.NotEqual(t, 0, keepCount)
}

func TestPolicyAdd(t *testing.T) {
	p := newDefaultPolicy(1000, 100)
	if victims, added := p.Add(1, 101); victims != nil || added {
		t.Fatal("can't add an item bigger than entire cache")
	}
	p.Lock()
	p.evict.add(1, 1)
	p.admit.Increment(1)
	p.admit.Increment(2)
	p.admit.Increment(3)
	p.Unlock()

	victims, added := p.Add(1, 1)
	require.Nil(t, victims)
	require.False(t, added)

	victims, added = p.Add(2, 20)
	require.Nil(t, victims)
	require.True(t, added)

	victims, added = p.Add(3, 90)
	require.NotNil(t, victims)
	require.True(t, added)

	victims, added = p.Add(4, 20)
	require.NotNil(t, victims)
	require.False(t, added)
}

func TestPolicySnapshot(t *testing.T) {
	p := newDefaultPolicy(1000, 100)
	if victims, added := p.Add(1, 101); victims != nil || added {
		t.Fatal("can't add an item bigger than entire cache")
	}
	p.Lock()
	p.evict.add(1, 1)
	p.admit.Increment(1)
	p.admit.Increment(2)
	p.admit.Increment(3)
	p.Unlock()

	victims, added := p.Add(1, 1)
	require.Nil(t, victims)
	require.False(t, added)

	victims, added = p.Add(2, 20)
	require.Nil(t, victims)
	require.True(t, added)

	victims, added = p.Add(3, 90)
	require.NotNil(t, victims)
	require.True(t, added)

	victims, added = p.Add(4, 20)
	require.NotNil(t, victims)
	require.False(t, added)

	// create admission/eviction writers
	var admissionWriter, evictionWriter bytes.Buffer
	err := p.admit.MarshalToBuffer(&admissionWriter)
	require.Nil(t, err)
	err = p.evict.MarshalToBuffer(&evictionWriter)
	require.Nil(t, err)

	unmarshaledAdmission, err := UnmarshalTinyLFU(admissionWriter.Bytes())
	require.Nil(t, err)

	unmarshaledEviction, err := UnmarshalSampledLFU(evictionWriter.Bytes())
	require.Nil(t, err)

	p2 := defaultPolicy{
		admit:   unmarshaledAdmission,
		evict:   unmarshaledEviction,
		itemsCh: make(chan []uint64, 3),
		stop:    make(chan struct{}),
	}

	require.NotNil(t, p2)
	require.Equal(t, p.admit, p2.admit)
	require.Equal(t, p.evict, p2.evict)
}

func TestPolicyHas(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Add(1, 1)
	require.True(t, p.Has(1))
	require.False(t, p.Has(2))
}

func TestPolicyDel(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Add(1, 1)
	p.Del(1)
	p.Del(2)
	require.False(t, p.Has(1))
	require.False(t, p.Has(2))
}

func TestPolicyCap(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Add(1, 1)
	require.Equal(t, int64(9), p.Cap())
}

func TestPolicyUpdate(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Add(1, 1)
	p.Update(1, 2)
	p.Lock()
	require.Equal(t, int64(2), p.evict.keyCosts[1])
	p.Unlock()
}

func TestPolicyCost(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Add(1, 2)
	require.Equal(t, int64(2), p.Cost(1))
	require.Equal(t, int64(-1), p.Cost(2))
}

func TestPolicyClear(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Add(1, 1)
	p.Add(2, 2)
	p.Add(3, 3)
	p.Clear()
	require.Equal(t, int64(10), p.Cap())
	require.False(t, p.Has(1))
	require.False(t, p.Has(2))
	require.False(t, p.Has(3))
}

func TestPolicyClose(t *testing.T) {
	defer func() {
		require.NotNil(t, recover())
	}()

	p := newDefaultPolicy(100, 10)
	p.Add(1, 1)
	p.Close()
	p.itemsCh <- []uint64{1}
}

func TestPushAfterClose(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Close()
	require.False(t, p.Push([]uint64{1, 2}))
}

func TestAddAfterClose(t *testing.T) {
	p := newDefaultPolicy(100, 10)
	p.Close()
	p.Add(1, 1)
}

func TestSampledLFUAdd(t *testing.T) {
	e := newSampledLFU(4)
	e.add(1, 1)
	e.add(2, 2)
	e.add(3, 1)
	require.Equal(t, int64(4), e.used)
	require.Equal(t, int64(2), e.keyCosts[2])
}

func TestSampledLFUDel(t *testing.T) {
	e := newSampledLFU(4)
	e.add(1, 1)
	e.add(2, 2)
	e.del(2)
	require.Equal(t, int64(1), e.used)
	_, ok := e.keyCosts[2]
	require.False(t, ok)
	e.del(4)
}

func TestSampledLFUUpdate(t *testing.T) {
	e := newSampledLFU(4)
	e.add(1, 1)
	require.True(t, e.updateIfHas(1, 2))
	require.Equal(t, int64(2), e.used)
	require.False(t, e.updateIfHas(2, 2))
}

func TestSampledLFUClear(t *testing.T) {
	e := newSampledLFU(4)
	e.add(1, 1)
	e.add(2, 2)
	e.add(3, 1)
	e.clear()
	require.Equal(t, 0, len(e.keyCosts))
	require.Equal(t, int64(0), e.used)
}

func TestSampledLFURoom(t *testing.T) {
	e := newSampledLFU(16)
	e.add(1, 1)
	e.add(2, 2)
	e.add(3, 3)
	require.Equal(t, int64(6), e.roomLeft(4))
}

func TestSampledLFUSample(t *testing.T) {
	e := newSampledLFU(16)
	e.add(4, 4)
	e.add(5, 5)
	sample := e.fillSample([]*policyPair{
		{1, 1},
		{2, 2},
		{3, 3},
	})
	k := sample[len(sample)-1].key
	require.Equal(t, 5, len(sample))
	require.NotEqual(t, 1, k)
	require.NotEqual(t, 2, k)
	require.NotEqual(t, 3, k)
	require.Equal(t, len(sample), len(e.fillSample(sample)))
	e.del(5)
	sample = e.fillSample(sample[:len(sample)-2])
	require.Equal(t, 4, len(sample))
}

func TestTinyLFUIncrement(t *testing.T) {
	a := newTinyLFU(4)
	a.Increment(1)
	a.Increment(1)
	a.Increment(1)
	require.True(t, a.door.Has(1))
	require.Equal(t, int64(2), a.freq.Estimate(1))

	a.Increment(1)
	require.False(t, a.door.Has(1))
	require.Equal(t, int64(1), a.freq.Estimate(1))
}

func TestTinyLFUEstimate(t *testing.T) {
	a := newTinyLFU(8)
	a.Increment(1)
	a.Increment(1)
	a.Increment(1)
	require.Equal(t, int64(3), a.Estimate(1))
	require.Equal(t, int64(0), a.Estimate(2))
}

func TestTinyLFUPush(t *testing.T) {
	a := newTinyLFU(16)
	a.Push([]uint64{1, 2, 2, 3, 3, 3})
	require.Equal(t, int64(1), a.Estimate(1))
	require.Equal(t, int64(2), a.Estimate(2))
	require.Equal(t, int64(3), a.Estimate(3))
	require.Equal(t, int64(6), a.incrs)
}

func TestTinyLFUClear(t *testing.T) {
	a := newTinyLFU(16)
	a.Push([]uint64{1, 3, 3, 3})
	a.clear()
	require.Equal(t, int64(0), a.incrs)
	require.Equal(t, int64(0), a.Estimate(3))
}

func TestMarshaling(t *testing.T) {
	t.Run("tinyLFU", func(t *testing.T) {
		a := newTinyLFU(16)
		a.Push([]uint64{1, 3, 3, 3})

		var buffer bytes.Buffer
		err := a.MarshalToBuffer(&buffer)
		require.Nil(t, err)

		unmarshaledA, err := UnmarshalTinyLFU(buffer.Bytes())

		require.Nil(t, err)
		require.Equal(t, a, unmarshaledA)
	})

	t.Run("tinyLFU- greenpack", func(t *testing.T) {
		a := newTinyLFU(16)
		a.Push([]uint64{1, 3, 3, 3})

		var buffer bytes.Buffer
		err := a.MarshalToBufferGreen(&buffer)
		require.Nil(t, err)

		unmarshaledA, err := UnmarshalTinyLFUGreen(buffer.Bytes())

		require.Nil(t, err)
		require.Equal(t, a, unmarshaledA)
	})

	//t.Run("sampledLFU", func(t *testing.T) {
	//	a := newSampledLFU(16)
	//
	//	a.add(1, 10)
	//	a.add(3, 4)
	//	a.updateIfHas(3, 40)
	//	a.updateIfHas(3, 50)
	//
	//	var buffer bytes.Buffer
	//	err := a.MarshalToBuffer(&buffer)
	//	require.Nil(t, err)
	//
	//	unmarshaledA, err := UnmarshalSampledLFU(buffer.Bytes())
	//
	//	require.Nil(t, err)
	//	require.Equal(t, a, unmarshaledA)
	//})
	//
	//t.Run("sampledLFU - greenpack", func(t *testing.T) {
	//	a := newSampledLFU(16)
	//
	//	a.add(1, 10)
	//	a.add(3, 4)
	//	a.updateIfHas(3, 40)
	//	a.updateIfHas(3, 50)
	//
	//	var buffer bytes.Buffer
	//	err := a.MarshalToBufferGreen(&buffer)
	//	require.Nil(t, err)
	//
	//	unmarshaledA, err := UnmarshalSampledLFUGreen(buffer.Bytes())
	//
	//	require.Nil(t, err)
	//	require.Equal(t, a, unmarshaledA)
	//})
}

func generateKeys(num int) []uint64 {
	keys := make([]uint64, num)

	for i := 0; i < num; i++ {
		keys[i] = rand.Uint64()
	}

	return keys
}

func BenchmarkMarshalSampledLFUGreen(b *testing.B) {
	keys := generateKeys(500000)

	lfu := newSampledLFU(171428570)

	for _, k := range keys {
		cost := rand.Uint64() % k
		lfu.add(k, int64(cost))
	}

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		err := lfu.MarshalToBufferGreen(&buffer)
		require.Nil(b, err)
	}
}

func BenchmarkMarshalSampledLFUMsgpack(b *testing.B) {
	keys := generateKeys(500000)

	lfu := newSampledLFU(171428570)

	for _, k := range keys {
		cost := rand.Uint64() % k
		lfu.add(k, int64(cost))
	}

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		err := lfu.MarshalToBuffer(&buffer)
		require.Nil(b, err)
	}
}

func BenchmarkMarshalTinyLFUGreen(b *testing.B) {
	keys := generateKeys(5000000)

	//lfu := newTinyLFU(171428570)
	lfu := newTinyLFU(200000000)
	lfu.Push(keys)

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		err := lfu.MarshalToBufferGreen(&buffer)
		require.Nil(b, err)
	}
}

func BenchmarkMarshalTinyLFUMsgpack(b *testing.B) {
	keys := generateKeys(5000000)

	//lfu := newTinyLFU(171428570)
	lfu := newTinyLFU(200000000)

	lfu.Push(keys)

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		err := lfu.MarshalToBuffer(&buffer)
		require.Nil(b, err)
	}
}
