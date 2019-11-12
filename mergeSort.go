package reconEngine

import (
	"bytes"
	"os"
	"sync"
)

// Merge sort algoritm (merge old partitions in bigger one)
func (ssTable *ssTable) MergeSort() error {
	var mx sync.Mutex
	mx.Lock()
	defer mx.Unlock()
	if ssTable.Len() <= 1 {
		return nil
	}
	values := make(map[string][]byte)
	ssTable.Range(func(createdAt int64, partitionStorage SsTablePartitionStorage) bool {
		partitionStorage.Range(func(k string, v []byte) bool {
			if _, ok := values[k]; !ok {
				values[k] = v
			}
			return true
		})
		return true
	})
	ssp := ssTable.CreatePartition()
	for k, v := range values {
		if !bytes.Equal(v, []byte{removed}) {
			err := ssp.Set(k, v)
			if err != nil {
				return err
			}
		}
	}

	err := ssTable.CloseAll()
	if err != nil {
		return err
	} else {
		for _, c := range ssTable.availablePartitions {
			if c == ssp.Key() {
				continue
			}
			err := os.Remove(makePath("partition", c))
			if err != nil {
				return err
			}
			err = os.Remove(makePath("index", c))
			if err != nil {
				return err
			}
		}
	}
	ssTable.availablePartitions = make(ssTablePartitionKeys, 0)
	ssTable.openedPartitions = ssTablePartitions{ssp}

	ssTable.CreatePartition()
	return nil
}
