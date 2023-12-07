/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ristretto

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"sync"
	"time"

	"github.com/glycerine/greenpack/msgp"
)

//go:generate greenpack -unexported

var (
	// TODO: find the optimal value or make it configurable.
	bucketDurationSecs = int64(5)
)

func storageBucket(t time.Time) int64 {
	return (t.Unix() / bucketDurationSecs) + 1
}

func cleanupBucket(t time.Time) int64 {
	// The bucket to cleanup is always behind the storage bucket by one so that
	// no elements in that bucket (which might not have expired yet) are deleted.
	return storageBucket(t) - 1
}

// bucket type is a map of key to conflict.
type bucket map[uint64]uint64

func DecodeMsgToBucket(dc *msgp.Reader) (bucket, error) {
	var mapHeader uint32
	var err error

	mapHeader, err = dc.ReadMapHeader()
	if err != nil {
		return nil, err
	}

	decodedBucket := make(map[uint64]uint64, mapHeader)

	for i := mapHeader; i > 0; i-- {
		key, err := dc.ReadUint64()
		if err != nil {
			return nil, err
		}
		val, err := dc.ReadUint64()
		if err != nil {
			return nil, err
		}

		decodedBucket[key] = val
	}

	return decodedBucket, nil
}

func (b bucket) DecodeMsg(dc *msgp.Reader) error {
	var mapHeader uint32
	var err error

	mapHeader, err = dc.ReadMapHeader()
	if err != nil {
		return err
	}

	decodedBucket := make(map[uint64]uint64, mapHeader)

	for i := mapHeader; i > 0; i-- {
		key, err := dc.ReadUint64()
		if err != nil {
			return err
		}
		val, err := dc.ReadUint64()
		if err != nil {
			return err
		}

		decodedBucket[key] = val
	}

	return nil
}

func (b bucket) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var nbs msgp.NilBitsStack
	nbs.Init(nil)
	if msgp.IsNil(bts) {
		bts = nbs.PushAlwaysNil(bts[1:])
	}

	var numEntries uint32

	numEntries, bts, err = nbs.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}

	unmarshaledBucket := make(map[uint64]uint64, numEntries)

	for i := numEntries; i > 0; i-- {
		var key, val uint64

		key, bts, err = nbs.ReadUint64Bytes(bts)
		if err != nil {
			return
		}
		val, bts, err = nbs.ReadUint64Bytes(bts)
		if err != nil {
			return
		}

		unmarshaledBucket[key] = val
	}

	o = bts

	return
}

func (b bucket) EncodeMsg(mw *msgp.Writer) error {
	var err error

	err = mw.WriteMapHeader(uint32(len(b)))
	if err != nil {
		return err
	}

	for key, val := range b {
		err = mw.WriteUint64(key)
		if err != nil {
			return err
		}
		err = mw.WriteUint64(val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b bucket) MarshalMsg(bs []byte) (o []byte, err error) {
	o = msgp.AppendMapHeader(o, uint32(len(b)))

	for key, val := range b {
		o = msgp.AppendUint64(o, key)
		o = msgp.AppendUint64(o, val)
	}

	return
}

func (b bucket) Msgsize() int {
	s := 1 + 18 + msgp.MapHeaderSize

	// for each entry in b, we have 2 uint64 sizes
	s += 2 * 8 * len(b)

	return s
}

// expirationMap is a map of bucket number to the corresponding bucket.
type expirationMap struct {
	sync.RWMutex `msg:"-"`
	buckets      map[int64]bucket `zid:"0"`
}

func (em *expirationMap) MarshalToBufferGreen(buffer io.Writer) error {
	return msgp.Encode(buffer, em)
}

func (em *expirationMap) MarshalToBuffer(buffer io.Writer) error {
	e := msgpack.NewEncoder(buffer)
	return e.Encode(em)
}

func UnmarshalExpirationMap(b []byte) (*expirationMap, error) {
	var em expirationMap
	var buffer = bytes.NewBuffer(b)

	err := msgp.Decode(buffer, &em)
	if err != nil {
		return nil, err
	}

	return &em, nil
}

//var _ msgpack.CustomDecoder = (*expirationMap)(nil)
//
//func (em *expirationMap) DecodeMsgpack(dec *msgpack.Decoder) error {
//	dec.SetMapDecoder(func(d *msgpack.Decoder) (interface{}, error) {
//		n, err := d.DecodeMapLen()
//		if err != nil {
//			return nil, err
//		}
//
//		m := make(map[int64]bucket)
//		for i := 0; i < n; i++ {
//			mk, err := d.DecodeInt64()
//			if err != nil {
//				return nil, err
//			}
//
//			mv, err := d.DecodeTypedMap()
//			if err != nil {
//				return nil, err
//			}
//
//			m[mk] = mv.(map[uint64]uint64)
//		}
//
//		return m, nil
//	})
//
//	out, err := dec.DecodeInterface()
//	if err != nil {
//		return err
//	}
//
//	em.buckets = out.(map[int64]bucket)
//
//	return err
//}

func newExpirationMap() *expirationMap {
	return &expirationMap{
		buckets: make(map[int64]bucket),
	}
}

func (m *expirationMap) add(key, conflict uint64, expiration time.Time) {
	if m == nil {
		return
	}

	// Items that don't expire don't need to be in the expiration map.
	if expiration.IsZero() {
		return
	}

	bucketNum := storageBucket(expiration)
	m.Lock()
	defer m.Unlock()

	b, ok := m.buckets[bucketNum]
	if !ok {
		b = make(bucket)
		m.buckets[bucketNum] = b
	}
	b[key] = conflict
}

func (m *expirationMap) update(key, conflict uint64, oldExpTime, newExpTime time.Time) {
	if m == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	oldBucketNum := storageBucket(oldExpTime)
	oldBucket, ok := m.buckets[oldBucketNum]
	if ok {
		delete(oldBucket, key)
	}

	newBucketNum := storageBucket(newExpTime)
	newBucket, ok := m.buckets[newBucketNum]
	if !ok {
		newBucket = make(bucket)
		m.buckets[newBucketNum] = newBucket
	}
	newBucket[key] = conflict
}

func (m *expirationMap) del(key uint64, expiration time.Time) {
	if m == nil {
		return
	}

	bucketNum := storageBucket(expiration)
	m.Lock()
	defer m.Unlock()
	_, ok := m.buckets[bucketNum]
	if !ok {
		return
	}
	delete(m.buckets[bucketNum], key)
}

// cleanup removes all the items in the bucket that was just completed. It deletes
// those items from the store, and calls the onEvict function on those items.
// This function is meant to be called periodically.
func (m *expirationMap) cleanup(store store, policy policy, onEvict itemCallback) {
	if m == nil {
		return
	}

	m.Lock()
	now := time.Now()
	bucketNum := cleanupBucket(now)
	keys := m.buckets[bucketNum]
	delete(m.buckets, bucketNum)
	m.Unlock()

	for key, conflict := range keys {
		// Sanity check. Verify that the store agrees that this key is expired.
		if store.Expiration(key).After(now) {
			continue
		}

		cost := policy.Cost(key)
		policy.Del(key)
		_, value := store.Del(key, conflict)

		if onEvict != nil {
			onEvict(&Item{Key: key,
				Conflict: conflict,
				Value:    value,
				Cost:     cost,
			})
		}
	}
}
