package reconEngine

import (
	"os"
	"testing"
)

func TestMergeSort(t *testing.T) {
	BinDir = os.TempDir()
	var ss = NewSsTable()
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
	if ss.Len() != 1 {
		t.Errorf("Possible partitions more then one: %d", ss.Len())
	}
}
