package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type entry struct {
	key, value string
}

// 4 bytes + 4 bytes + 4 bytes
const entryHeader = 12

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	size := kl + vl + 12
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	vl := binary.LittleEndian.Uint32(input[kl+8:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	e.value = string(valBuf)
}

func readValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	return string(data), nil
}

func readEntry(in *bufio.Reader) (*entry, error) {
	var header [8]byte
	_, err := io.ReadFull(in, header[:])
	if err != nil {
		return nil, err
	}

	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	key := make([]byte, keySize)

	_, err = io.ReadFull(in, key)
	if err != nil {
		return nil, err
	}

	var valueHeader [4]byte
	_, err = io.ReadFull(in, valueHeader[:])

	if err != nil {
		return nil, err
	}
	valueSize := int(binary.LittleEndian.Uint32(valueHeader[:]))
	value := make([]byte, valueSize)
	_, err = io.ReadFull(in, value)

	if err != nil {
		return nil, err
	}
	entr := entry{
		key:   string(key),
		value: string(value),
	}
	return &entr, nil
}

func (e *entry) serializedSize() int64 {
	return entryHeader + int64(len(e.key)) + int64(len(e.value))
}
