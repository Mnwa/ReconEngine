package reconEngine

import (
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SsTableStorage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	Del(key []byte) error
	CreatePartition() SsTablePartitionStorage
	ClosePartition(partition SsTablePartitionStorage) error
	OpenPartition(createdAt int64) SsTablePartitionStorage
	GetOpenedPartitions() []SsTablePartitionStorage
	GetAvailablePartitions() []int64
	Range(cb func(createdAt int64, partitionStorage SsTablePartitionStorage) bool)
	CloseAll() error
	MergeSort() error
}

type ssTablePartitions []SsTablePartitionStorage

func (ssp ssTablePartitions) Len() int           { return len(ssp) }
func (ssp ssTablePartitions) Swap(i, j int)      { ssp[i], ssp[j] = ssp[j], ssp[i] }
func (ssp ssTablePartitions) Less(i, j int) bool { return ssp[i].CreatedAt() > ssp[j].CreatedAt() }

type ssTablePartitionKeys []int64

func (sspK ssTablePartitionKeys) Len() int           { return len(sspK) }
func (sspK ssTablePartitionKeys) Swap(i, j int)      { sspK[i], sspK[j] = sspK[j], sspK[i] }
func (sspK ssTablePartitionKeys) Less(i, j int) bool { return sspK[i] > sspK[j] }

// Ss table realisation
type ssTable struct {
	OpenedPartitions    ssTablePartitions
	AvailablePartitions ssTablePartitionKeys
}

func (ssTable ssTable) Range(cb func(createdAt int64, partitionStorage SsTablePartitionStorage) bool) {
	for _, createdAt := range ssTable.AvailablePartitions {
		ssTable.OpenPartition(createdAt)
	}
	for _, p := range ssTable.OpenedPartitions {
		if !cb(p.CreatedAt(), p) {
			break
		}
	}
}

func (ssTable *ssTable) GetOpenedPartitions() []SsTablePartitionStorage {
	return ssTable.OpenedPartitions
}

func (ssTable *ssTable) GetAvailablePartitions() []int64 {
	return ssTable.AvailablePartitions
}

func (ssTable *ssTable) CreatePartition() SsTablePartitionStorage {
	ssp := ssTable.OpenPartition(time.Now().UnixNano())
	return ssp
}

func (ssTable *ssTable) ClosePartition(partition SsTablePartitionStorage) error {
	for i, p := range ssTable.OpenedPartitions {
		if p.CreatedAt() == partition.CreatedAt() {
			err := p.Close()
			if err != nil {
				return err
			}
			if len(ssTable.OpenedPartitions) > 1 {
				ssTable.OpenedPartitions = append(ssTable.OpenedPartitions[:i], ssTable.OpenedPartitions[i+1:]...)
			} else {
				ssTable.OpenedPartitions = make(ssTablePartitions, 0)
			}
			ssTable.AvailablePartitions = append(ssTable.AvailablePartitions, p.CreatedAt())
			sort.Sort(ssTable.AvailablePartitions)
			break
		}
	}
	return nil
}

func (ssTable *ssTable) OpenPartition(createdAt int64) SsTablePartitionStorage {
	partition := NewSStablePartition(createdAt)
	ssTable.OpenedPartitions = append(ssTable.OpenedPartitions, partition)
	sort.Sort(ssTable.OpenedPartitions)
	return partition
}

func (ssTable *ssTable) Get(key []byte) (val []byte, err error) {
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		val, err = partitionStorage.Get(key)
		if err == KeyNotFoundErr {
			return true
		}
		return false
	})
	return
}

func (ssTable *ssTable) Set(key []byte, val []byte) (err error) {
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		err = partitionStorage.Set(key, val)
		return false
	})
	return
}

func (ssTable *ssTable) Del(key []byte) (err error) {
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		err = partitionStorage.Del(key)
		return false
	})
	return
}

func (ssTable *ssTable) CloseAll() error {
	for _, o := range ssTable.OpenedPartitions {
		err := ssTable.ClosePartition(o)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewSsTable() SsTableStorage {
	var SsTable = &ssTable{}
	fileInfos, err := ioutil.ReadDir(BinDir)
	if err != nil {
		log.Panic(err)
	}
	for _, fileInfo := range fileInfos {
		if strings.Contains(fileInfo.Name(), "-partition") {
			timestampEnc := strings.ReplaceAll(fileInfo.Name(), "-partition.bin", "")
			timestamp, err := strconv.ParseInt(timestampEnc, 10, 64)
			if err != nil {
				log.Panic(err)
			}
			SsTable.AvailablePartitions = append(SsTable.AvailablePartitions, timestamp)
			sort.Sort(SsTable.AvailablePartitions)
		}
	}
	return SsTable
}
