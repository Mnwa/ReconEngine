package reconEngine

import (
	"bytes"
	"errors"
	"sync"
)

type MemStorage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte)
	Del(key []byte) error
	Sync() error
	Len() int
	SsTable() SsTableStorage
}

// Memory table
type mem struct {
	storage map[string][]byte
	ssTable SsTableStorage
}

// Key not found error
var KeyNotFoundErr = errors.New("can't found value by that key")

// Removed key error
var KeyRemovedErr = errors.New("that key was removed")

func (m *mem) Get(key []byte) ([]byte, error) {
	val, ok := m.storage[string(key)]
	if ok && bytes.Equal(val, []byte{removed}) {
		return nil, KeyNotFoundErr
	}
	if !ok {
		return m.ssTable.Get(key)
	}
	return val, nil
}

func (m *mem) Set(key []byte, value []byte) {
	m.storage[string(key)] = value
}

func (m *mem) Del(key []byte) error {
	_, ok := m.storage[string(key)]
	if !ok {
		return KeyNotFoundErr
	}
	m.storage[string(key)] = []byte{removed}
	return nil
}

func (m *mem) Sync() error {
	var mx sync.Mutex
	mx.Lock()
	defer mx.Unlock()
	ssp := m.ssTable.CreatePartition()
	for k, v := range m.storage {
		err := ssp.Set([]byte(k), v)
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

// Create mem table
// Argument may be nil
func NewMem(ssTable SsTableStorage) MemStorage {
	if ssTable == nil {
		ssTable = NewSsTable()
	}
	return &mem{
		storage: make(map[string][]byte),
		ssTable: ssTable,
	}
}
