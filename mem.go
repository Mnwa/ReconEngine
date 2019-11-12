package reconEngine

import (
	"bytes"
	"errors"
	"strings"
	"sync"
)

//Base mem interface, you can implement own realisation
type MemStorage interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte)
	Del(key string) error
	Scan(subStr string, cb func(key string, value []byte) bool)
	Sync() error
	Len() int
	SsTable() SsTableStorage
}

// Memory table
type mem struct {
	storage map[string][]byte
	ssTable SsTableStorage
}

func (m *mem) Scan(subStr string, cb func(key string, value []byte) bool) {
	needNextIteration := true
	for key, value := range m.storage {
		if strings.Contains(key, subStr) {
			if !cb(key, value) {
				return
			}
		}
	}
	m.ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		partitionStorage.Range(func(key string, value []byte) bool {
			if _, ok := m.storage[key]; !ok && strings.Contains(key, subStr) {
				m.storage[key] = value
				if !cb(key, value) {
					needNextIteration = false
				}
			}
			return needNextIteration
		})
		return needNextIteration
	})
}

// Key not found error
var KeyNotFoundErr = errors.New("can't found value by that key")

// Removed key error
var KeyRemovedErr = errors.New("that key was removed")

func (m *mem) Get(key string) ([]byte, error) {
	val, ok := m.storage[key]
	if ok && bytes.Equal(val, []byte{removed}) {
		return nil, KeyNotFoundErr
	}
	if !ok {
		val, err := m.ssTable.Get(key)
		if err == nil {
			m.storage[key] = val
		}
		return val, err
	}
	return val, nil
}

func (m *mem) Set(key string, value []byte) {
	m.storage[key] = value
}

func (m *mem) Del(key string) error {
	m.storage[key] = []byte{removed}
	return nil
}

func (m *mem) Sync() error {
	var mx sync.Mutex
	mx.Lock()
	defer mx.Unlock()
	ssp := m.ssTable.CreatePartition()
	for k, v := range m.storage {
		err := ssp.Set(k, v)
		if err != nil {
			return err
		} else {
			delete(m.storage, k)
		}
	}
	return nil
}

func (m *mem) Len() int {
	return len(m.storage)
}

func (m *mem) SsTable() SsTableStorage {
	return m.ssTable
}

// Mem constructor, create structure realised MemStorage interface
// ssTable argument may be a nil
func NewMem(ssTable SsTableStorage) MemStorage {
	if ssTable == nil {
		ssTable = NewSsTable()
	}
	return &mem{
		storage: make(map[string][]byte),
		ssTable: ssTable,
	}
}
