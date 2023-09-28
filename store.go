/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/brainflake/ristretto/z"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	shardFilenameTemplate = "shard_%d.msgpack"
	expirationMapFilename = "expirations.msgpack"
	metricsFilename       = "metrics.msgpack"
)

// TODO: Do we need this to be a separate struct from Item?
type storeItem struct {
	key        uint64
	conflict   uint64
	value      interface{}
	expiration time.Time
}

func init() {
	storeItemEncoder := func(enc *msgpack.Encoder, val reflect.Value) error {
		s := val.Interface().(storeItem)

		return enc.EncodeMulti(s.key, s.conflict, s.value, s.expiration)
	}

	// Note: if storing a non-primitive type this will be overridden
	// Also ensure your type satisfies the msgpack.CustomDecoder + msgpack.CustomEncoder
	// interfaces
	storeItemDecoder := func(dec *msgpack.Decoder, val reflect.Value) error {
		return dec.Decode(val)
	}

	msgpack.Register(storeItem{}, storeItemEncoder, storeItemDecoder)
}

// store is the interface fulfilled by all hash map implementations in this
// file. Some hash map implementations are better suited for certain data
// distributions than others, so this allows us to abstract that out for use
// in Ristretto.
//
// Every store is safe for concurrent usage.
type store interface {
	// Get returns the value associated with the key parameter.
	Get(uint64, uint64) (interface{}, bool)
	// Expiration returns the expiration time for this key.
	Expiration(uint64) time.Time
	// Set adds the key-value pair to the Map or updates the value if it's
	// already present. The key-value pair is passed as a pointer to an
	// item object.
	Set(*Item)
	// Del deletes the key-value pair from the Map.
	Del(uint64, uint64) (uint64, interface{})
	// Update attempts to update the key with a new value and returns true if
	// successful.
	Update(*Item) (interface{}, bool)
	// Cleanup removes items that have an expired TTL.
	Cleanup(policy policy, onEvict itemCallback)
	// Clear clears all contents of the store.
	Clear(onEvict itemCallback)
	// Snapshot will create a point-in-time snapshot of the cache
	Snapshot(path string) error
}

// newStore returns the default store implementation.
func newStore() store {
	return newShardedMap()
}

// newStoreFromSnapshot returns a new store from a stored snapshot
func newStoreFromSnapshot(dir string, itemType interface{}) (store, error) {
	return newShardedMapFromSnapshot(dir, itemType)
}

const numShards uint64 = 256

type shardedMap struct {
	shards    []*lockedMap
	expiryMap *expirationMap
}

func newShardedMap() *shardedMap {
	sm := &shardedMap{
		shards:    make([]*lockedMap, int(numShards)),
		expiryMap: newExpirationMap(),
	}
	for i := range sm.shards {
		sm.shards[i] = newLockedMap(sm.expiryMap)
	}
	return sm
}

func UnmarshalExpirationMap(b []byte) (*expirationMap, error) {
	var em expirationMap

	err := msgpack.Unmarshal(b, &em)
	if err != nil {
		return nil, err
	}

	return &em, nil
}

func newShardedMapFromSnapshot(path string, itemType interface{}) (*shardedMap, error) {
	sm := &shardedMap{
		shards: make([]*lockedMap, int(numShards)),
	}

	// TODO: process interim expirations that would have happened since the snapshot
	file, err := os.OpenFile(filepath.Join(path, expirationMapFilename), os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	buffer, err := z.NewReadBuffer(file, int(stat.Size()))
	if err != nil {
		return nil, err
	}

	sm.expiryMap, err = UnmarshalExpirationMap(buffer.Bytes())
	if err != nil {
		return nil, err
	}

	for i := range sm.shards {
		shardFile := fmt.Sprintf(shardFilenameTemplate, i)
		file, err := os.OpenFile(filepath.Join(path, shardFile), os.O_RDONLY, 0666)
		defer file.Close()

		if err != nil {
			return nil, err
		}

		stat, err := file.Stat()
		if err != nil {
			return nil, err
		}

		buffer, err := z.NewReadBuffer(file, int(stat.Size()))
		if err != nil {
			return nil, err
		}

		sm.shards[i], err = newLockedMapFromSnapshot(sm.expiryMap, buffer.Bytes(), itemType)
		if err != nil {
			return nil, err
		}
	}

	return sm, nil
}

func (sm *shardedMap) Get(key, conflict uint64) (interface{}, bool) {
	return sm.shards[key%numShards].get(key, conflict)
}

func (sm *shardedMap) Expiration(key uint64) time.Time {
	return sm.shards[key%numShards].Expiration(key)
}

func (sm *shardedMap) Set(i *Item) {
	if i == nil {
		// If item is nil make this Set a no-op.
		return
	}

	sm.shards[i.Key%numShards].Set(i)
}

func (sm *shardedMap) Del(key, conflict uint64) (uint64, interface{}) {
	return sm.shards[key%numShards].Del(key, conflict)
}

func (sm *shardedMap) Update(newItem *Item) (interface{}, bool) {
	return sm.shards[newItem.Key%numShards].Update(newItem)
}

func (sm *shardedMap) Cleanup(policy policy, onEvict itemCallback) {
	sm.expiryMap.cleanup(sm, policy, onEvict)
}

func (sm *shardedMap) Clear(onEvict itemCallback) {
	for i := uint64(0); i < numShards; i++ {
		sm.shards[i].Clear(onEvict)
	}
}

func (sm *shardedMap) Snapshot(dir string) error {
	var err error

	var expiryBuffer *z.Buffer
	expiryBuffer, err = z.NewBufferPersistent(filepath.Join(dir, expirationMapFilename), 0)
	if err != nil {
		return err
	}
	defer expiryBuffer.Release()

	e := msgpack.NewEncoder(expiryBuffer)

	err = e.Encode(sm.expiryMap)
	if err != nil {
		return err
	}

	// Note these are done serially here so as not to read lock all of the shards at once, although this may
	// not be a concern
	for idx, data := range sm.shards {
		var dataBuffer *z.Buffer

		dataBuffer, err = z.NewBufferPersistent(filepath.Join(dir, fmt.Sprintf(shardFilenameTemplate, idx)), 0)
		if err != nil {
			return err
		}

		err = data.marshalToBuffer(dataBuffer)
		dataBuffer.Release() // ensure we release the buffer regardless of error status
		if err != nil {
			// fail if there's a single error
			return err
		}
	}

	return nil
}

type lockedMap struct {
	sync.RWMutex
	data map[uint64]storeItem
	em   *expirationMap
}

func newLockedMap(em *expirationMap) *lockedMap {
	return &lockedMap{
		data: make(map[uint64]storeItem),
		em:   em,
	}
}

func newLockedMapFromSnapshot(em *expirationMap, buf []byte, itemType interface{}) (*lockedMap, error) {
	lockedMap, err := UnmarshalLockedMap(buf, itemType)
	if err != nil {
		return nil, err
	}

	lockedMap.em = em

	return lockedMap, nil
}

func (m *lockedMap) marshalToBuffer(buffer io.Writer) error {
	m.RLock()
	defer m.RUnlock()

	e := msgpack.NewEncoder(buffer)

	return e.Encode(m.data)
}

func UnmarshalLockedMap(b []byte, itemType interface{}) (*lockedMap, error) {
	lockedMap := &lockedMap{}

	data := make(map[uint64]storeItem)

	if len(b) > 1 {
		dec := msgpack.NewDecoder(bytes.NewBuffer(b))

		// If an itemType is passed in set up a decoder for it (used when storing structs as the item)
		if itemType != nil {
			storeItemEncoder := func(enc *msgpack.Encoder, val reflect.Value) error {
				s := val.Interface().(storeItem)

				return enc.EncodeMulti(s.key, s.conflict, s.value, s.expiration)
			}

			storeItemDecoder := func(dec *msgpack.Decoder, val reflect.Value) error {
				ptr := val.Addr().UnsafePointer()

				s := (*storeItem)(ptr)

				dec.DecodeMulti(&s.key, &s.conflict)

				ss := reflect.New(reflect.TypeOf(itemType))
				decodefn := ss.MethodByName("DecodeMsgpack")

				// TODO: check for return err here
				decodefn.Call([]reflect.Value{reflect.ValueOf(dec)})

				s.value = ss.Interface()

				return dec.Decode(&s.expiration)
			}

			msgpack.Register(storeItem{}, storeItemEncoder, storeItemDecoder)
		}

		err := dec.Decode(&data)
		if err != nil {
			return nil, err
		}
	}

	lockedMap.data = data

	return lockedMap, nil
}

func (m *lockedMap) get(key, conflict uint64) (interface{}, bool) {
	m.RLock()
	item, ok := m.data[key]
	m.RUnlock()
	if !ok {
		return nil, false
	}
	if conflict != 0 && (conflict != item.conflict) {
		return nil, false
	}

	// Handle expired items.
	if !item.expiration.IsZero() && time.Now().After(item.expiration) {
		return nil, false
	}
	return item.value, true
}

func (m *lockedMap) Expiration(key uint64) time.Time {
	m.RLock()
	defer m.RUnlock()
	return m.data[key].expiration
}

func (m *lockedMap) Set(i *Item) {
	if i == nil {
		// If the item is nil make this Set a no-op.
		return
	}

	m.Lock()
	defer m.Unlock()
	item, ok := m.data[i.Key]

	if ok {
		// The item existed already. We need to check the conflict key and reject the
		// update if they do not match. Only after that the expiration map is updated.
		if i.Conflict != 0 && (i.Conflict != item.conflict) {
			return
		}
		m.em.update(i.Key, i.Conflict, item.expiration, i.Expiration)
	} else {
		// The value is not in the map already. There's no need to return anything.
		// Simply add the expiration map.
		m.em.add(i.Key, i.Conflict, i.Expiration)
	}

	m.data[i.Key] = storeItem{
		key:        i.Key,
		conflict:   i.Conflict,
		value:      i.Value,
		expiration: i.Expiration,
	}
}

func (m *lockedMap) Del(key, conflict uint64) (uint64, interface{}) {
	m.Lock()
	item, ok := m.data[key]
	if !ok {
		m.Unlock()
		return 0, nil
	}
	if conflict != 0 && (conflict != item.conflict) {
		m.Unlock()
		return 0, nil
	}

	if !item.expiration.IsZero() {
		m.em.del(key, item.expiration)
	}

	delete(m.data, key)
	m.Unlock()
	return item.conflict, item.value
}

func (m *lockedMap) Update(newItem *Item) (interface{}, bool) {
	m.Lock()
	item, ok := m.data[newItem.Key]
	if !ok {
		m.Unlock()
		return nil, false
	}
	if newItem.Conflict != 0 && (newItem.Conflict != item.conflict) {
		m.Unlock()
		return nil, false
	}

	m.em.update(newItem.Key, newItem.Conflict, item.expiration, newItem.Expiration)
	m.data[newItem.Key] = storeItem{
		key:        newItem.Key,
		conflict:   newItem.Conflict,
		value:      newItem.Value,
		expiration: newItem.Expiration,
	}

	m.Unlock()
	return item.value, true
}

func (m *lockedMap) Clear(onEvict itemCallback) {
	m.Lock()
	i := &Item{}
	if onEvict != nil {
		for _, si := range m.data {
			i.Key = si.key
			i.Conflict = si.conflict
			i.Value = si.value
			onEvict(i)
		}
	}
	m.data = make(map[uint64]storeItem)
	m.Unlock()
}
