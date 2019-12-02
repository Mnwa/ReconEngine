package reconEngine

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"path"
	"strconv"
)

const removed = 0x04

//Base ss table partition interface, you can implement own realisation
type SsTablePartitionStorage interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Del(key string) error
	Range(cb func(key string, value []byte) bool)
	Key() int64
	Close() error
}

type dataPosition struct {
	Offset int64
	Length int32
}

// It's a part of ss table, loaded parts can be a readOnly
type ssTablePartition struct {
	createdAt int64
	index     map[string]dataPosition
	fd        *os.File
	dir       *string
}

func (ssp *ssTablePartition) Range(cb func(key string, value []byte) bool) {
	for k := range ssp.index {
		v, err := ssp.Get(k)
		if err != nil && err != KeyRemovedErr {
			continue
		}
		if !cb(k, v) {
			break
		}
	}
}

func (ssp *ssTablePartition) Get(key string) ([]byte, error) {
	index, ok := ssp.index[key]
	if !ok {
		return nil, KeyNotFoundErr
	}

	_, err := ssp.fd.Seek(index.Offset, 0)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(ssp.fd)

	val, err := reader.Peek(int(index.Length))
	if err != nil {
		return nil, err
	}

	if bytes.Equal(val, []byte{removed}) {
		return val, KeyRemovedErr
	}
	return val, nil
}

func (ssp *ssTablePartition) Set(key string, value []byte) error {
	n, err := ssp.fd.Write(value)
	if err != nil {
		return err
	}
	fi, err := ssp.fd.Stat()
	if err != nil {
		return err
	}
	ssp.index[key] = dataPosition{
		Offset: fi.Size() - int64(n),
		Length: int32(len(value)),
	}
	return ssp.saveIndex()
}

func (ssp *ssTablePartition) Del(key string) error {
	return ssp.Set(key, []byte{removed})
}

func (ssp *ssTablePartition) Key() int64 {
	return ssp.createdAt
}

func (ssp *ssTablePartition) Close() error {
	err := ssp.fd.Sync()
	if err != nil {
		return err
	}

	err = ssp.saveIndex()
	if err != nil {
		return err
	}
	err = ssp.fd.Close()
	if err != nil {
		return err
	}
	return nil
}

func (ssp *ssTablePartition) createIndex() (err error) {
	fd, err := os.OpenFile(makePath(*ssp.dir, "index", ssp.createdAt), os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	fi, err := fd.Stat()
	if err != nil {
		return
	}

	if fi.Size() != 0 {
		err = gob.NewDecoder(fd).Decode(&ssp.index)
		return
	} else {
		ssp.index = make(map[string]dataPosition)
		return
	}
}

func (ssp *ssTablePartition) saveIndex() error {
	fd, err := os.OpenFile(makePath(*ssp.dir, "index", ssp.createdAt), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	err = gob.NewEncoder(fd).Encode(ssp.index)
	if err != nil {
		return err
	}

	return fd.Close()
}

func makePath(dir, prefix string, createdAt int64) string {
	return path.Join(dir, strconv.FormatInt(createdAt, 10)+"-"+prefix+".bin")
}

// SsTable partition constructor, create structure realised SsTablePartitionStorage interface
func NewSStablePartition(createdAt int64, dir *string) SsTablePartitionStorage {
	fd, err := os.OpenFile(makePath(*dir, "partition", createdAt), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Panic(err)
	}
	ssp := &ssTablePartition{
		createdAt: createdAt,
		fd:        fd,
		index:     nil,
		dir:       dir,
	}
	err = ssp.createIndex()
	if err != nil && err != io.EOF {
		log.Panic(err)
	}

	return ssp
}
