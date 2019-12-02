package reconEngine

import (
	"bytes"
	"os"
	"testing"
)

var tmp = os.TempDir()

func TestMem_Get(t *testing.T) {
	var mem = NewMem(nil, &tmp)
	mem.Set("test", []byte("mega test"))
	v, err := mem.Get("test")
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(v, []byte("mega test")) {
		t.Error("Waiting 'mega test', received: " + string(v))
	}
}

func TestMem_Del(t *testing.T) {
	var mem = NewMem(nil, &tmp)
	mem.Set("test", []byte("mega test"))
	err := mem.Del("test")
	if err != nil {
		t.Error(err)
	}
	_, err = mem.Get("test")
	if err != KeyNotFoundErr {
		t.Error("Key exists")
	}
}

func TestMem_Sync(t *testing.T) {
	var mem = NewMem(nil, &tmp)
	mem.Set("test", []byte("mega test"))
	mem.Set("test1", []byte("mega test1"))
	mem.Set("test2", []byte("mega test2"))
	prevLen := mem.SsTable().Len()
	err := mem.Sync()
	if err != nil {
		t.Error(err)
	}
	if mem.Len() != 0 {
		t.Error("Synced is not all data")
	}
	if mem.SsTable().Len()-prevLen != 1 {
		t.Error("ssTable do not synced")
	}
}
