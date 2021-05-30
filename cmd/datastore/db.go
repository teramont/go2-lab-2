package datastore

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const KB = 1024
const MB = 1024 * KB
const defaultSegmentSize = 10 * MB

// value for db.started flag
const STARTED = 0xbeef
const CLOSED = 0xdead

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndexEntry struct {
	segmentIdx int // index into db.segments array
	offset     int64
}

type hashIndex map[string]hashIndexEntry

type Db struct {
	dir     string
	out     *os.File
	offset  int64
	maxSize int64

	segments []string

	indexMutex sync.Mutex
	index      hashIndex

	started   uint32 // flag whether the writing thread has started
	closed    uint32 // flag whether the db is closed to prevent double closing of the channel
	writeChan chan writeRequest
}

type writeRequest struct {
	key, value string
	callback   chan error
}

func NewDb(dir string) (*Db, error) {
	db := &Db{
		dir:        dir,
		out:        nil,
		offset:     0,
		maxSize:    defaultSegmentSize,
		segments:   []string{},
		index:      make(hashIndex),
		indexMutex: sync.Mutex{},
		started:    0,
		writeChan:  make(chan writeRequest),
	}
	err := db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	err = db.pushNewSegment()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Sets max size of the segment. Returns *db for the chaining
func (db *Db) SegmentSize(max int64) *Db {
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
	currentIdx := len(db.segments) - 1

	in := bufio.NewReaderSize(input, bufSize)
	var offset int64 = 0
	for {

		e, err := readEntry(in)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		indexEntry := hashIndexEntry{
			segmentIdx: currentIdx,
			offset:     offset,
		}
		db.index[e.key] = indexEntry
		offset += e.serializedSize()

	}
}

func (db *Db) Close() error {
	if !atomic.CompareAndSwapUint32(&db.closed, 0, CLOSED) {
		return nil
	}
	close(db.writeChan)
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.indexMutex.Lock()
	position, ok := db.index[key]
	db.indexMutex.Unlock()
	if !ok {
		return "", ErrNotFound
	}

	segName := &db.segments[position.segmentIdx]
	file, err := os.Open(*segName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position.offset, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	e, err := readEntry(reader)
	if err != nil {
		return "", err
	}
	return e.value, nil
}

func (db *Db) pushNewSegment() error {
	if db.out != nil {
		if err := db.out.Close(); err != nil {
			return err
		}
	}
	filepath, file, err := db.openNewSegment()
	if err != nil {
		return err
	}
	db.segments = append(db.segments, filepath)
	db.out = file
	db.offset = 0

	if len(db.segments) >= 3 {
		return db.mergeSegments()
	}
	return nil
}

func (db *Db) openNewSegment() (string, *os.File, error) {
	filename := nextSegName()
	filepath := filepath.Join(db.dir, filename)
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	return filepath, file, err
}

func (db *Db) putUnsafe(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		entry := hashIndexEntry{
			segmentIdx: len(db.segments) - 1,
			offset:     db.offset,
		}
		db.index[key] = entry
		db.offset += int64(n)

		if db.offset >= db.maxSize {
			return db.pushNewSegment()
		}
	}
	return err
}

func randStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	rand.Seed(time.Now().UnixNano())

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func nextSegName() string {
	return fmt.Sprintf("segment-%s", randStringBytes(10))
}

func (db *Db) mergeSegments() error {
	values := make(map[string]string)
	oldsegments := db.segments[:len(db.segments)-1]
	for _, filename := range oldsegments {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		reader := bufio.NewReader(file)
		defer file.Close()
		for {
			entr, err := readEntry(reader)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			values[entr.key] = entr.value
		}
	}

	// Create in default temp directory, so if error happens, file will be cleaned
	file, err := os.CreateTemp("", "segment-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	// we will rename the file to this name
	filename := nextSegName()
	filepth := filepath.Join(db.dir, filename)

	index := make(hashIndex)
	segments := []string{filepth, db.segments[len(db.segments)-1]}

	var offset int64 = 0
	if err != nil {
		return err
	}
	for key, value := range values {
		entr := entry{
			key:   key,
			value: value,
		}
		_, err := file.Write(entr.Encode())
		if err != nil {
			return err
		}
		indexEntr := hashIndexEntry{
			segmentIdx: 0, // resulted segment is the first in the array
			offset:     offset,
		}
		index[key] = indexEntr
	}

	err = file.Close()
	if err != nil {
		return err
	}
	err = os.Rename(file.Name(), filepth)
	if err != nil {
		return err
	}

	db.indexMutex.Lock()
	db.segments = segments
	db.indexMutex.Unlock()

	for _, seg := range oldsegments {
		err = os.Remove(seg)
		if err != nil {
			return err
		}
	}
	return nil
}

// Start write thread. Without it, db will not work
func (db *Db) Start() {
	// only one thread should be started
	if !atomic.CompareAndSwapUint32(&db.started, 0, STARTED) {
		return
	}
	go func() {
		for {
			req, ok := <-db.writeChan
			if !ok {
				return
			}
			err := db.putUnsafe(req.key, req.value)
			req.callback <- err
		}
	}()
}

func (db *Db) Put(key, value string) error {
	callback := make(chan error)
	req := writeRequest{
		key:      key,
		value:    value,
		callback: callback,
	}
	db.writeChan <- req
	return <-callback
}
