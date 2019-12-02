package reconEngine

import (
	"bytes"
	"testing"
	"time"
)

func TestSStablePartition_Get(t *testing.T) {
	var ssp = NewSStablePartition(time.Now().UnixNano(), &tmp)
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
	var ssp = NewSStablePartition(time.Now().UnixNano(), &tmp)
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
