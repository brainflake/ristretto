package ristretto

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"testing"
	"time"

	"github.com/brainflake/ristretto/z"
	"github.com/stretchr/testify/require"
)

func TestStoreSetGet(t *testing.T) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	s.Set(&i)
	val, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	i.Value = 3
	s.Set(&i)
	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 3, val.(int))

	key, conflict = z.KeyToHash(2)
	i = Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	s.Set(&i)
	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))
}

func TestMsgPackStoreItem(t *testing.T) {
	map1 := make(map[uint64]storeItem)

	item1 := storeItem{
		Key:        uint64(34),
		Conflict:   uint64(4),
		Value:      int8(3),
		Expiration: time.Now(),
	}

	map1[63] = item1

	map2 := make(map[uint64]storeItem)
	//var item2 storeItem

	marshaledMap1, err := msgpack.Marshal(&map1)
	require.Nil(t, err)

	msgpack.Unmarshal(marshaledMap1, &map2)

	require.Equal(t, len(map1), len(map2))
	require.Equal(t, map1[63].Key, map2[63].Key)
	require.Equal(t, map1[63].Conflict, map2[63].Conflict)
	require.Equal(t, map1[63].Value, map2[63].Value)
	require.True(t, map1[63].Expiration.Equal(map2[63].Expiration))
}

func TestStoreSnapshot(t *testing.T) {
	s := newShardedMap()
	key, conflict := z.KeyToHash(1)
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	s.Set(&i)
	val, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	i.Value = 3
	s.Set(&i)
	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 3, val.(int))

	key, conflict = z.KeyToHash(2)
	i = Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	s.Set(&i)
	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	writers := make([]io.Writer, numShards)
	for idx := range writers {
		buffer := &bytes.Buffer{}
		s.shards[idx].marshalToBuffer(buffer)
		writers[idx] = buffer

		//t.Log("storing", idx)
		//t.Log(s.shards[idx].data)
		//t.Log(buffer.Bytes())
	}
	//s.Snapshot(writers)
	//for _, w := range writers {
	//	require.NotNil(t, w)
	//	//require.NotEqual(t, 0, len(w))
	//}

	readers := make([]*bytes.Buffer, numShards)
	for idx := range readers {
		buffer, ok := writers[idx].(*bytes.Buffer)
		require.True(t, ok)
		readers[idx] = buffer
	}
	//s2 := newShardedMapFromSnapshot(readers)

	s2 := &shardedMap{
		shards:    make([]*lockedMap, int(numShards)),
		expiryMap: s.expiryMap,
	}
	for idx := range readers {
		//t.Log("Reading", idx)
		//var b []byte
		//readers[idx].Read(b)
		//t.Log(b)
		//
		//var lockedMapBuffer bytes.Buffer
		//_, err := lockedMapBuffer.ReadFrom(readers[idx])
		//require.Nil(t, err)
		//
		//t.Log("bytes", lockedMapBuffer.Bytes())
		//lockedMap := UnmarshalLockedMap(lockedMapBuffer.Bytes())
		//t.Log(lockedMap)

		s2.shards[idx] = newLockedMapFromSnapshot(s2.expiryMap, readers[idx].Bytes())
	}

	for idx, m := range s2.shards {
		//t.Log("Idx", idx)

		for key, item := range m.data {
			require.Equal(t, s.shards[idx].data[key].Value, int(item.Value.(int8)))
			require.True(t, s.shards[idx].data[key].Expiration.Equal(item.Expiration))
		}
		require.Equal(t, len(s.shards[idx].data), len(m.data))
		//require.Equal(t, s.shards[idx].data, m.data)
	}
}

func TestStoreDel(t *testing.T) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	s.Del(key, conflict)
	val, ok := s.Get(key, conflict)
	require.False(t, ok)
	require.Nil(t, val)

	s.Del(2, 0)
}

func TestStoreClear(t *testing.T) {
	s := newStore()
	for i := uint64(0); i < 1000; i++ {
		key, conflict := z.KeyToHash(i)
		it := Item{
			Key:      key,
			Conflict: conflict,
			Value:    i,
		}
		s.Set(&it)
	}
	s.Clear(nil)
	for i := uint64(0); i < 1000; i++ {
		key, conflict := z.KeyToHash(i)
		val, ok := s.Get(key, conflict)
		require.False(t, ok)
		require.Nil(t, val)
	}
}

func TestStoreUpdate(t *testing.T) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	i.Value = 2
	_, ok := s.Update(&i)
	require.True(t, ok)

	val, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.NotNil(t, val)

	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, val.(int))

	i.Value = 3
	_, ok = s.Update(&i)
	require.True(t, ok)

	val, ok = s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 3, val.(int))

	key, conflict = z.KeyToHash(2)
	i = Item{
		Key:      key,
		Conflict: conflict,
		Value:    2,
	}
	_, ok = s.Update(&i)
	require.False(t, ok)
	val, ok = s.Get(key, conflict)
	require.False(t, ok)
	require.Nil(t, val)
}

func TestStoreCollision(t *testing.T) {
	s := newShardedMap()
	s.shards[1].Lock()
	s.shards[1].data[1] = storeItem{
		Key:      1,
		Conflict: 0,
		Value:    1,
	}
	s.shards[1].Unlock()
	val, ok := s.Get(1, 1)
	require.False(t, ok)
	require.Nil(t, val)

	i := Item{
		Key:      1,
		Conflict: 1,
		Value:    2,
	}
	s.Set(&i)
	val, ok = s.Get(1, 0)
	require.True(t, ok)
	require.NotEqual(t, 2, val.(int))

	_, ok = s.Update(&i)
	require.False(t, ok)
	val, ok = s.Get(1, 0)
	require.True(t, ok)
	require.NotEqual(t, 2, val.(int))

	s.Del(1, 1)
	val, ok = s.Get(1, 0)
	require.True(t, ok)
	require.NotNil(t, val)
}

func TestStoreExpiration(t *testing.T) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	expiration := time.Now().Add(time.Second)
	i := Item{
		Key:        key,
		Conflict:   conflict,
		Value:      1,
		Expiration: expiration,
	}
	s.Set(&i)
	val, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 1, val.(int))

	ttl := s.Expiration(key)
	require.Equal(t, expiration, ttl)

	s.Del(key, conflict)

	_, ok = s.Get(key, conflict)
	require.False(t, ok)
	require.True(t, s.Expiration(key).IsZero())

	// missing item
	key, _ = z.KeyToHash(4340958203495)
	ttl = s.Expiration(key)
	require.True(t, ttl.IsZero())
}

func BenchmarkStoreGet(b *testing.B) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get(key, conflict)
		}
	})
}

func BenchmarkStoreSet(b *testing.B) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := Item{
				Key:      key,
				Conflict: conflict,
				Value:    1,
			}
			s.Set(&i)
		}
	})
}

func BenchmarkStoreUpdate(b *testing.B) {
	s := newStore()
	key, conflict := z.KeyToHash(1)
	i := Item{
		Key:      key,
		Conflict: conflict,
		Value:    1,
	}
	s.Set(&i)
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Update(&Item{
				Key:      key,
				Conflict: conflict,
				Value:    2,
			})
		}
	})
}
