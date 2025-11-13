// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package txn

import (
	"context"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func TestBufferBatchGetter(t *testing.T) {
	snap := newMockStore()
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	require.NoError(t, snap.Set(ka, ka))
	require.NoError(t, snap.Set(kb, kb))
	require.NoError(t, snap.Set(kc, kc))
	require.NoError(t, snap.Set(kd, kd))

	// middle value is the same as snap
	middle := newMockStore()
	require.NoError(t, middle.Set(ka, []byte("a1")))
	require.NoError(t, middle.Set(kc, []byte("c1")))

	buffer := newMockStore()
	require.NoError(t, buffer.Set(ka, []byte("a2")))
	require.NoError(t, buffer.Delete(kb))

	batchGetter := NewBufferBatchGetter(&mockBufferBatchGetterStore{buffer}, middle, snap)
	result, err := batchGetter.BatchGet(context.Background(), []kv.Key{ka, kb, kc, kd})
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, "a2", string(result[string(ka)].Value))
	require.Equal(t, "c1", string(result[string(kc)].Value))
	require.Equal(t, "d", string(result[string(kd)].Value))
}

type mockBatchGetterStore struct {
	index []kv.Key
	value [][]byte
}

func newMockStore() *mockBatchGetterStore {
	return &mockBatchGetterStore{
		index: make([]kv.Key, 0),
		value: make([][]byte, 0),
	}
}

func (s *mockBatchGetterStore) Len() int {
	return len(s.index)
}
func (s *mockBatchGetterStore) Get(_ context.Context, k kv.Key, _ ...kv.GetOption) (kv.ValueEntry, error) {
	for i, key := range s.index {
		if key.Cmp(k) == 0 {
			return kv.NewValueEntry(s.value[i], 0), nil
		}
	}
	return kv.ValueEntry{}, kv.ErrNotExist
}

func (s *mockBatchGetterStore) BatchGet(ctx context.Context, keys []kv.Key, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	m := make(map[string]kv.ValueEntry)
	getOptions := kv.BatchGetToGetOptions(options)
	for _, k := range keys {
		v, err := s.Get(ctx, k, getOptions...)
		if err == nil {
			m[string(k)] = v
			continue
		}
		if kv.IsErrNotFound(err) {
			continue
		}
		return m, err
	}
	return m, nil
}

func (s *mockBatchGetterStore) Set(k kv.Key, v []byte) error {
	for i, key := range s.index {
		if key.Cmp(k) == 0 {
			s.value[i] = v
			return nil
		}
	}
	s.index = append(s.index, k)
	s.value = append(s.value, v)
	return nil
}

func (s *mockBatchGetterStore) Delete(k kv.Key) error {
	return s.Set(k, []byte{})
}

type mockBufferBatchGetterStore struct {
	*mockBatchGetterStore
}

func (s *mockBufferBatchGetterStore) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	kvKeys := *(*[]kv.Key)(unsafe.Pointer(&keys))
	return s.mockBatchGetterStore.BatchGet(ctx, kvKeys)
}
