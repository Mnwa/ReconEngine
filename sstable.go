package reconEngine

import (
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SsTablePartitions []*SStablePartition

func (ssp SsTablePartitions) Len() int           { return len(ssp) }
func (ssp SsTablePartitions) Swap(i, j int)      { ssp[i], ssp[j] = ssp[j], ssp[i] }
func (ssp SsTablePartitions) Less(i, j int) bool { return ssp[i].createdAt > ssp[j].createdAt }

type SsTablePartitionKeys []int64

func (sspK SsTablePartitionKeys) Len() int           { return len(sspK) }
func (sspK SsTablePartitionKeys) Swap(i, j int)      { sspK[i], sspK[j] = sspK[j], sspK[i] }
func (sspK SsTablePartitionKeys) Less(i, j int) bool { return sspK[i] > sspK[j] }

// Ss table realisation
type SsTable struct {
	OpenedPartitions         SsTablePartitions
	PossibleToOpenPartitions SsTablePartitionKeys
}

func (ss *SsTable) CreatePartition() *SStablePartition {
	ssp := ss.OpenPartition(time.Now().Unix())
	return ssp
}

func (ss *SsTable) ClosePartition(partition *SStablePartition) error {
	for i, p := range ss.OpenedPartitions {
		if p.createdAt == partition.createdAt {
			err := p.Close()
			if err != nil {
				return err
			}
			if len(ss.OpenedPartitions) > 1 {
				ss.OpenedPartitions = append(ss.OpenedPartitions[:i], ss.OpenedPartitions[i+1:]...)
			} else {
				ss.OpenedPartitions = make(SsTablePartitions, 0)
			}
			ss.PossibleToOpenPartitions = append(ss.PossibleToOpenPartitions, p.createdAt)
			sort.Sort(ss.PossibleToOpenPartitions)
			break
		}
	}
	return nil
}

func (ss *SsTable) OpenPartition(createdAt int64) *SStablePartition {
	partition := NewSStablePartition(createdAt)
	ss.OpenedPartitions = append(ss.OpenedPartitions, partition)
	sort.Sort(ss.OpenedPartitions)
	return partition
}

func (ss *SsTable) Get(key []byte) ([]byte, error) {
	for _, o := range ss.OpenedPartitions {
		val, err := o.Get(key)
		if err == KeyNotFoundErr {
			continue
		}
		return val, err
	}
	for i, c := range ss.PossibleToOpenPartitions {
		o := ss.OpenPartition(c)
		if len(ss.PossibleToOpenPartitions) > 1 {
			ss.PossibleToOpenPartitions = append(ss.PossibleToOpenPartitions[:i], ss.PossibleToOpenPartitions[i+1:]...)
		} else {
			ss.PossibleToOpenPartitions = make(SsTablePartitionKeys, 0)
		}
		val, err := o.Get(key)
		if err == KeyNotFoundErr {
			continue
		}
		return val, err
	}
	return nil, KeyNotFoundErr
}

func (ss *SsTable) Set(key []byte, val []byte) error {
	var o *SStablePartition
	if len(ss.OpenedPartitions) != 0 && !ss.OpenedPartitions[0].isLoaded {
		if len(ss.PossibleToOpenPartitions) != 0 {
			if ss.OpenedPartitions[0].createdAt > ss.PossibleToOpenPartitions[0] {
				o = ss.OpenedPartitions[len(ss.OpenedPartitions)-1]
			} else {
				o = ss.CreatePartition()
			}
		} else {
			o = ss.OpenedPartitions[len(ss.OpenedPartitions)-1]
		}
	} else {
		o = ss.CreatePartition()
	}
	return o.Set(key, val)
}

func (ss *SsTable) Del(key []byte) error {
	for _, o := range ss.OpenedPartitions {
		err := o.Del(key)
		if err == KeyNotFoundErr {
			continue
		}
		return err
	}
	for i, c := range ss.PossibleToOpenPartitions {
		o := ss.OpenPartition(c)
		if len(ss.PossibleToOpenPartitions) > 1 {
			ss.PossibleToOpenPartitions = append(ss.PossibleToOpenPartitions[:i], ss.PossibleToOpenPartitions[i+1:]...)
		} else {
			ss.PossibleToOpenPartitions = make(SsTablePartitionKeys, 0)
		}
		err := o.Del(key)
		if err == KeyNotFoundErr {
			continue
		}
		return err
	}
	return KeyNotFoundErr
}

func (ss *SsTable) CloseAll() error {
	for _, o := range ss.OpenedPartitions {
		err := ss.ClosePartition(o)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewSsTable() *SsTable {
	var SsTable = &SsTable{}
	fileInfos, err := ioutil.ReadDir("./bin")
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
			SsTable.PossibleToOpenPartitions = append(SsTable.PossibleToOpenPartitions, timestamp)
			sort.Sort(SsTable.PossibleToOpenPartitions)
		}
	}
	return SsTable
}
