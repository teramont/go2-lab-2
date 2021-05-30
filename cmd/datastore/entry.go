package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
)

type entry struct {
	key, value string
}

const sha1Len = 20

// 4 bytes + 4 bytes + hash
const headerLen = 8 + sha1Len

var ErrHashSumDontMatch = fmt.Errorf("hashsums don't match")

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + headerLen
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res[0:4], uint32(kl))
	binary.LittleEndian.PutUint32(res[4:8], uint32(vl))
	copy(res[8:], e.key)
	copy(res[8+kl:], e.value)
	hashIndex := size - sha1Len
	hash := sha1.Sum(res[:hashIndex])
	copy(res[hashIndex:], hash[:])
	return res
}

func (e *entry) Decode(input []byte) error {
	kl := binary.LittleEndian.Uint32(input[0:4])
	vl := binary.LittleEndian.Uint32(input[4:8])

	keyStart := uint32(8)
	valueStart := keyStart + kl
	hashStart := valueStart + vl

	keyBuf := make([]byte, kl)
	valBuf := make([]byte, vl)

	copy(keyBuf, input[keyStart:valueStart])
	e.key = string(keyBuf)

	copy(valBuf, input[valueStart:hashStart])
	e.value = string(valBuf)

	hash := input[hashStart:]
	expectedHash := sha1.Sum(input[:hashStart])

	if bytes.Equal(hash, expectedHash[:]) {
		return nil
	} else {
		return ErrHashSumDontMatch
	}
}

func readEntry(in *bufio.Reader) (*entry, error) {
	var header [8]byte
	_, err := io.ReadFull(in, header[:])
	if err != nil {
		return nil, err
	}

	keySize := int(binary.LittleEndian.Uint32(header[0:4]))
	valueSize := int(binary.LittleEndian.Uint32(header[4:8]))
	key := make([]byte, keySize)
	value := make([]byte, valueSize)

	_, err = io.ReadFull(in, key)
	if err != nil {
		return nil, err
	}
	_, err = io.ReadFull(in, value)

	if err != nil {
		return nil, err
	}

	var hash [sha1Len]byte
	_, err = io.ReadFull(in, hash[:])

	if err != nil {
		return nil, err
	}

	hasher := sha1.New()
	// hashing can't fall, so we don't need to check for errors
	hasher.Write(header[:])
	hasher.Write(key)
	hasher.Write(value)
	expectedHash := hasher.Sum(nil)

	if !bytes.Equal(hash[:], expectedHash) {
		return nil, ErrHashSumDontMatch
	}

	entr := entry{
		key:   string(key),
		value: string(value),
	}
	return &entr, nil
}

func (e *entry) serializedSize() int64 {
	return headerLen + int64(len(e.key)) + int64(len(e.value))
}
