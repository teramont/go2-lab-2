package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{"key", "value"}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadEntry(t *testing.T) {
	e := entry{"key", "test-value"}
	data := e.Encode()
	entr, err := readEntry(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if entr.key != e.key {
		t.Errorf("Got bat key [%s]", entr.key)
	}
	if entr.value != e.value {
		t.Errorf("Got bat value [%s]", entr.value)
	}
}

func TestFailSum(t *testing.T) {
	e := entry{"key", "test-value"}
	data := e.Encode()
	data[10] = ^data[10] // let's flip some bits
	_, err := readEntry(bufio.NewReader(bytes.NewReader(data)))
	if err != ErrHashSumDontMatch {
		t.Fatalf("Expected error that signatures don't match, but got %s", err)
	}
}
