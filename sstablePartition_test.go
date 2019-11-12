package reconEngine

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestSStablePartition_Get(t *testing.T) {
	BinDir = os.TempDir()
	var ssp = NewSStablePartition(time.Now().UnixNano())
	err := ssp.Set("test", []byte("mega test"))
	if err != nil {
		t.Error(err)
	}
	v, err := ssp.Get("test")
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(v, []byte("mega test")) {
		t.Error("Waiting 'mega test', received: " + string(v))
	}
}
func TestSStablePartition_Del(t *testing.T) {
	BinDir = os.TempDir()
	var ssp = NewSStablePartition(time.Now().UnixNano())
	err := ssp.Set("test", []byte("mega test"))
	if err != nil {
		t.Error(err)
	}
	err = ssp.Del("test")
	if err != nil {
		t.Error(err)
	}
	_, err = ssp.Get("test")
	if err != KeyRemovedErr {
		t.Error("Key exists")
	}
}
