package reconEngine

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"sync"
)

// Merge sort algoritm (merge old partitions in bigger one)
func MergeSort(ssTable *SsTable) {
	var mx sync.Mutex
	mx.Lock()
	err := ssTable.CloseAll()
	if err != nil {
		log.Fatal(err)
	}
	if len(ssTable.PossibleToOpenPartitions) <= 1 {
		return
	}
	firstC := ssTable.PossibleToOpenPartitions[0]
	values := make(map[string][]byte)
	for _, c := range ssTable.PossibleToOpenPartitions {
		p := ssTable.OpenPartition(c)
		for k := range p.Index {
			if _, ok := values[k]; !ok {
				v, err := p.Get([]byte(k))
				if err != nil && err != KeyRemovedErr {
					log.Fatal(err)
				} else {
					values[k] = v
				}
			}
		}
		err = ssTable.ClosePartition(p)
		if err != nil {
			log.Fatal(err)
		} else {
			err := os.Remove("bin/" + strconv.FormatInt(p.createdAt, 10) + "-partition.bin")
			if err != nil {
				log.Fatal(err)
			}
			err = os.Remove("bin/" + strconv.FormatInt(p.createdAt, 10) + "-index.bin")
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	ssp := NewSStablePartition(firstC)
	for k, v := range values {
		if !bytes.Equal(v, []byte{removed}) {
			err := ssp.Set([]byte(k), v)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	ssTable.PossibleToOpenPartitions = make(SsTablePartitionKeys, 0)
	ssTable.OpenedPartitions = SsTablePartitions{ssp}
	mx.Unlock()
}
