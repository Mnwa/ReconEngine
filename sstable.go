package reconEngine

import (
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

//Base SsTable interface, you can implement own realisation
type SsTableStorage interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Del(key string) error
	CreatePartition() SsTablePartitionStorage
	ClosePartition(partition SsTablePartitionStorage) error
	OpenPartition(createdAt int64) SsTablePartitionStorage
	Range(cb func(createdAt int64, partitionStorage SsTablePartitionStorage) bool)
	Len() int
	CloseAll() error
	MergeSort() error
}

type ssTablePartitions []SsTablePartitionStorage

func (ssp ssTablePartitions) Len() int           { return len(ssp) }
func (ssp ssTablePartitions) Swap(i, j int)      { ssp[i], ssp[j] = ssp[j], ssp[i] }
func (ssp ssTablePartitions) Less(i, j int) bool { return ssp[i].Key() > ssp[j].Key() }

type ssTablePartitionKeys []int64

func (sspK ssTablePartitionKeys) Len() int           { return len(sspK) }
func (sspK ssTablePartitionKeys) Swap(i, j int)      { sspK[i], sspK[j] = sspK[j], sspK[i] }
func (sspK ssTablePartitionKeys) Less(i, j int) bool { return sspK[i] > sspK[j] }

// Ss table realisation
type ssTable struct {
	openedPartitions    ssTablePartitions
	availablePartitions ssTablePartitionKeys
}

func (ssTable *ssTable) Len() int {
	return len(ssTable.availablePartitions) + len(ssTable.openedPartitions)
}

func (ssTable ssTable) Range(cb func(createdAt int64, partitionStorage SsTablePartitionStorage) bool) {
	for _, createdAt := range ssTable.availablePartitions {
		ssTable.OpenPartition(createdAt)
	}
	for _, p := range ssTable.openedPartitions {
		if !cb(p.Key(), p) {
			break
		}
	}
}

func (ssTable *ssTable) CreatePartition() SsTablePartitionStorage {
	ssp := ssTable.OpenPartition(time.Now().UnixNano())
	return ssp
}

func (ssTable *ssTable) ClosePartition(partition SsTablePartitionStorage) error {
	for i, p := range ssTable.openedPartitions {
		if p.Key() == partition.Key() {
			err := p.Close()
			if err != nil {
				return err
			}
			if len(ssTable.openedPartitions) > 1 {
				ssTable.openedPartitions = append(ssTable.openedPartitions[:i], ssTable.openedPartitions[i+1:]...)
			} else {
				ssTable.openedPartitions = make(ssTablePartitions, 0)
			}
			ssTable.availablePartitions = append(ssTable.availablePartitions, p.Key())
			sort.Sort(ssTable.availablePartitions)
			break
		}
	}
	return nil
}

func (ssTable *ssTable) OpenPartition(createdAt int64) SsTablePartitionStorage {
	partition := NewSStablePartition(createdAt)
	ssTable.openedPartitions = append(ssTable.openedPartitions, partition)
	sort.Sort(ssTable.openedPartitions)
	return partition
}

func (ssTable *ssTable) Get(key string) (val []byte, err error) {
	if ssTable.Len() == 0 {
		ssTable.CreatePartition()
	}
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		val, err = partitionStorage.Get(key)
		if err == KeyNotFoundErr {
			return true
		}
		return false
	})
	return
}

func (ssTable *ssTable) Set(key string, val []byte) (err error) {
	if ssTable.Len() == 0 {
		ssTable.CreatePartition()
	}
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		err = partitionStorage.Set(key, val)
		return false
	})
	return
}

func (ssTable *ssTable) Del(key string) (err error) {
	if ssTable.Len() == 0 {
		ssTable.CreatePartition()
	}
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		err = partitionStorage.Del(key)
		return false
	})
	return
}

func (ssTable *ssTable) CloseAll() error {
	for _, o := range ssTable.openedPartitions {
		err := ssTable.ClosePartition(o)
		if err != nil {
			return err
		}
	}
	return nil
}

// SsTable constructor, create structure realised SsTableStorage interface
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
			SsTable.availablePartitions = append(SsTable.availablePartitions, timestamp)
			sort.Sort(SsTable.availablePartitions)
		}
	}
	return SsTable
}
