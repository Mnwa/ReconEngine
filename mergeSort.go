package reconEngine

import (
	"bytes"
	"log"
	"os"
	"sync"
)

// Merge sort algoritm (merge old partitions in bigger one)
func MergeSort(ssTable *SsTable) error {
	var mx sync.Mutex
	mx.Lock()
	defer mx.Unlock()
	err := ssTable.CloseAll()
	if err != nil {
		log.Fatal(err)
	}
	if len(ssTable.PossibleToOpenPartitions) <= 1 {
		return nil
	}
	firstC := ssTable.PossibleToOpenPartitions[0]
	values := make(map[string][]byte)
	for _, c := range ssTable.PossibleToOpenPartitions {
		p := ssTable.OpenPartition(c)
		for k := range p.Index {
			if _, ok := values[k]; !ok {
				v, err := p.Get([]byte(k))
				if err != nil && err != KeyRemovedErr {
					return err
				} else {
					values[k] = v
				}
			}
		}
		err = ssTable.ClosePartition(p)
		if err != nil {
			return err
		} else {
			err := os.Remove(makePath("partition", p.createdAt))
			if err != nil {
				return err
			}
			err = os.Remove(makePath("index", p.createdAt))
			if err != nil {
				return err
			}
		}
	}
	ssp := NewSStablePartition(firstC)
	for k, v := range values {
		if !bytes.Equal(v, []byte{removed}) {
			err := ssp.Set([]byte(k), v)
			if err != nil {
				return err
			}
		}
	}
	ssTable.PossibleToOpenPartitions = make(SsTablePartitionKeys, 0)
	ssTable.OpenedPartitions = SsTablePartitions{ssp}
	return nil
}
