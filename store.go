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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/brainflake/ristretto/z"
	"github.com/golang/glog"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	shardFilenameTemplate = "shard_%d.map"
	expirationMapFilename = "expirations.map"
)

// TODO: Do we need this to be a separate struct from Item?
type storeItem struct {
	Key        uint64
	Conflict   uint64
	Value      interface{}
	Expiration time.Time
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
func newStoreFromSnapshot(dir string) (store, error) {
	return newShardedMapFromSnapshot(dir)
}

const numShards uint64 = 256

type shardedMap struct {
	shards    []*lockedMap
	expiryMap *expirationMap
}

type sharedMapSnapshot struct {
	offsets []uint64
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

func UnmarshalExpirationMap(b []byte) *expirationMap {
	var em expirationMap

	err := msgpack.Unmarshal(b, &em)
	if err != nil {
		glog.Fatal("msgpack.Unmarshal failed: ", err)
	}

	return &em
}

func newShardedMapFromSnapshot(path string) (*shardedMap, error) {
	sm := &shardedMap{
		shards: make([]*lockedMap, int(numShards)),
	}

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

	sm.expiryMap = UnmarshalExpirationMap(buffer.Bytes())

	for i := range sm.shards {
		file, err := os.OpenFile(filepath.Join(path, strconv.Itoa(i)), os.O_RDONLY, 0666)
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

		sm.shards[i] = newLockedMapFromSnapshot(sm.expiryMap, buffer.Bytes())
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

type exportedLockedMap struct {
	Data map[uint64]storeItem
	Em   *expirationMap
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

func newLockedMapFromSnapshot(em *expirationMap, buf []byte) *lockedMap {
	lockedMap := UnmarshalLockedMap(buf)
	lockedMap.em = em

	return lockedMap
}

func (m *lockedMap) marshalToBuffer(buffer io.Writer) error {
	m.RLock()
	defer m.RUnlock()

	e := msgpack.NewEncoder(buffer)

	//exportedMap := &exportedLockedMap{
	//	Data: m.data,
	//	Em:   m.em,
	//}
	err := e.Encode(m.data)
	if err != nil {
		return err
	}

	return nil
}

func UnmarshalLockedMap(b []byte) *lockedMap {
	lockedMap := &lockedMap{}

	data := make(map[uint64]storeItem)

	if len(b) > 1 {
		err := msgpack.Unmarshal(b, &data)
		if err != nil {
			glog.Fatal("msgpack.Unmarshal failed: ", err)
		}
	}

	lockedMap.data = data

	return lockedMap
}

func (m *lockedMap) get(key, conflict uint64) (interface{}, bool) {
	m.RLock()
	item, ok := m.data[key]
	m.RUnlock()
	if !ok {
		return nil, false
	}
	if conflict != 0 && (conflict != item.Conflict) {
		return nil, false
	}

	// Handle expired items.
	if !item.Expiration.IsZero() && time.Now().After(item.Expiration) {
		return nil, false
	}
	return item.Value, true
}

func (m *lockedMap) Expiration(key uint64) time.Time {
	m.RLock()
	defer m.RUnlock()
	return m.data[key].Expiration
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
		if i.Conflict != 0 && (i.Conflict != item.Conflict) {
			return
		}
		m.em.update(i.Key, i.Conflict, item.Expiration, i.Expiration)
	} else {
		// The value is not in the map already. There's no need to return anything.
		// Simply add the expiration map.
		m.em.add(i.Key, i.Conflict, i.Expiration)
	}

	m.data[i.Key] = storeItem{
		Key:        i.Key,
		Conflict:   i.Conflict,
		Value:      i.Value,
		Expiration: i.Expiration,
	}
}

func (m *lockedMap) Del(key, conflict uint64) (uint64, interface{}) {
	m.Lock()
	item, ok := m.data[key]
	if !ok {
		m.Unlock()
		return 0, nil
	}
	if conflict != 0 && (conflict != item.Conflict) {
		m.Unlock()
		return 0, nil
	}

	if !item.Expiration.IsZero() {
		m.em.del(key, item.Expiration)
	}

	delete(m.data, key)
	m.Unlock()
	return item.Conflict, item.Value
}

func (m *lockedMap) Update(newItem *Item) (interface{}, bool) {
	m.Lock()
	item, ok := m.data[newItem.Key]
	if !ok {
		m.Unlock()
		return nil, false
	}
	if newItem.Conflict != 0 && (newItem.Conflict != item.Conflict) {
		m.Unlock()
		return nil, false
	}

	m.em.update(newItem.Key, newItem.Conflict, item.Expiration, newItem.Expiration)
	m.data[newItem.Key] = storeItem{
		Key:        newItem.Key,
		Conflict:   newItem.Conflict,
		Value:      newItem.Value,
		Expiration: newItem.Expiration,
	}

	m.Unlock()
	return item.Value, true
}

func (m *lockedMap) Clear(onEvict itemCallback) {
	m.Lock()
	i := &Item{}
	if onEvict != nil {
		for _, si := range m.data {
			i.Key = si.Key
			i.Conflict = si.Conflict
			i.Value = si.Value
			onEvict(i)
		}
	}
	m.data = make(map[uint64]storeItem)
	m.Unlock()
}
