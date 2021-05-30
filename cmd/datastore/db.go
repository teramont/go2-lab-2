package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

const outFileName = "current-data"
const KB = 1024
const MB = 1024 * KB
const defaultSegmentSize = 10 * MB

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndexEntry struct {
	segmentName *string // pointer into db.segments array
	offset      int64
}

type hashIndex map[string]hashIndexEntry

type Db struct {
	dir     string
	out     *os.File
	offset  int64
	maxSize int64

	segments []string

	index hashIndex
}

func NewDb(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	out, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		dir:      dir,
		out:      out,
		offset:   0,
		maxSize:  defaultSegmentSize,
		segments: []string{outputPath},
		index:    make(hashIndex),
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

// Sets max size of the segment. Returns *db for the chaining
func (db *Db) MaxSize(max int64) *Db {
	db.maxSize = max
	return db
}

const bufSize = 8192

func (db *Db) recover() error {
	files, err := ioutil.ReadDir(db.dir)
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime().Before(files[j].ModTime())
	})

	for _, file := range files {
		path := filepath.Join(db.dir, file.Name())

		err = db.recoverSegment(path)
		if err != nil {
			return err
		}
	}

	return nil
}

// Recovers segment, by reading the file and updating the index.
func (db *Db) recoverSegment(path string) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()

	db.segments = append(db.segments, path)
	currentPath := &db.segments[len(db.segments)-1]

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	var offset int64 = 0
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			indexEntry := hashIndexEntry{
				segmentName: currentPath,
				offset:      offset,
			}
			db.index[e.key] = indexEntry
			offset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(*position.segmentName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position.offset, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		entry := hashIndexEntry{
			segmentName: &db.segments[len(db.segments)-1],
			offset:      db.offset,
		}
		db.index[key] = entry
		db.offset += int64(n)
	}
	return err
}
