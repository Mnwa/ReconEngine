package reconEngine

import (
	"bytes"
	"errors"
	"sync"
)

// Memory table
type Mem struct {
	storage map[string][]byte
	ssTable *SsTable
}

// Key not found error
var KeyNotFoundErr = errors.New("can't found value by that key")

// Removed key error
var KeyRemovedErr = errors.New("that key was removed")

func (m *Mem) Get(key []byte) ([]byte, error) {
	val, ok := m.storage[string(key)]
	if ok && bytes.Equal(val, []byte{removed}) {
		return nil, KeyNotFoundErr
	}
	if !ok {
		return m.ssTable.Get(key)
	}
	return val, nil
}

func (m *Mem) Set(key []byte, value []byte) {
	m.storage[string(key)] = value
}

func (m *Mem) Del(key []byte) error {
	_, ok := m.storage[string(key)]
	if !ok {
		return KeyNotFoundErr
	}
	m.storage[string(key)] = []byte{removed}
	return nil
}

func (m *Mem) Sync() error {
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
	return m.ssTable.ClosePartition(ssp)
}

func (m *Mem) Len() int {
	return len(m.storage)
}

// Create mem table
func NewMem(ssTable *SsTable) *Mem {
	return &Mem{
		storage: make(map[string][]byte),
		ssTable: ssTable,
	}
}
