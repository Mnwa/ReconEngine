package reconEngine

import (
	"testing"
)

func TestMergeSort(t *testing.T) {
	var ss = NewSsTable(&tmp)
	sspO := ss.CreatePartition()
	sspT := ss.CreatePartition()
	err := sspO.Set("test", []byte("mega test"))
	if err != nil {
		t.Error(err)
	}
	err = sspT.Set("test1", []byte("mega test1"))
	if err != nil {
		t.Error(err)
	}
	err = ss.ClosePartition(sspO)
	if err != nil {
		t.Error(err)
	}
	err = ss.ClosePartition(sspT)
	if err != nil {
		t.Error(err)
	}
	err = ss.MergeSort()
	if err != nil {
		t.Error(err)
	}
	if ss.Len() != 2 {
		t.Errorf("Possible partitions more then one: %d", ss.Len())
	}
}
