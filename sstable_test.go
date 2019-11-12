package reconEngine

import (
	"bytes"
	"os"
	"testing"
)

func TestSsTable_Get(t *testing.T) {
	BinDir = os.TempDir()
	var ss = NewSsTable()
	err := ss.Set("test", []byte("mega test"))
	if err != nil {
		t.Error(err)
	}
	v, err := ss.Get("test")
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(v, []byte("mega test")) {
		t.Error("Waiting 'mega test', received: " + string(v))
	}
}
func TestSsTable_Del(t *testing.T) {
	BinDir = os.TempDir()
	var ss = NewSsTable()
	err := ss.Set("test", []byte("mega test"))
	if err != nil {
		t.Error(err)
	}
	err = ss.Del("test")
	if err != nil {
		t.Error(err)
	}
	_, err = ss.Get("test")
	if err != KeyRemovedErr {
		t.Error("Key exists")
	}
}

func TestSsTable_CreatePartition(t *testing.T) {
	BinDir = os.TempDir()
	var ss = NewSsTable()
	sp := ss.CreatePartition()
	err := ss.ClosePartition(sp)
	if err != nil {
		t.Error(err)
	}
}
