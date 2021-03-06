package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Start()
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	filename := db.segments[len(db.segments)-1]
	outFile, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}
		db.Start()

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

}

func TestDb_Merge(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Start()
	defer db.Close()

	db.SegmentSize(16)

	if len(db.segments) != 1 {
		t.Fatalf("Expected 1 segment, but got %d", len(db.segments))
	}

	err = db.Put("aa", "aa")
	if err != nil {
		t.Fatal(err)
	}

	if len(db.segments) != 2 {
		t.Fatalf("Expected 2 segments, but got %d", len(db.segments))
	}

	err = db.Put("bb", "bb")
	if err != nil {
		t.Fatal(err)
	}

	// we are expecting 2 instead of 3 segments, because of merge
	if len(db.segments) != 2 {
		t.Fatalf("Expected 2 segments, but got %d", len(db.segments))
	}

	err = db.Put("cc", "cc")
	if err != nil {
		t.Fatal(err)
	}

	if len(db.segments) != 2 {
		t.Fatalf("Expected 2 segments, but got %d", len(db.segments))
	}

	err = db.Put("dd", "dd")
	if err != nil {
		t.Fatal(err)
	}

	if len(db.segments) != 2 {
		t.Fatalf("Expected 2 segments, but got %d", len(db.segments))
	}
}

func TestDbPar(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Start()
	defer db.Close()

	pairs := [][]string{}
	for i := 0; i < 255; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		pairs = append(pairs, []string{key, value})
	}

	callback := make(chan struct{}, len(pairs))

	t.Run("put", func(t *testing.T) {
		for _, pair := range pairs {
			pair := pair
			go func() {
				err := db.Put(pair[0], pair[1])
				if err != nil {
					t.Errorf("Cannot put %s: %s", pairs[0], err)
				}
				callback <- struct{}{}
			}()
		}
	})

	// wait for all threads
	for i := 0; i < len(pairs); i++ {
		<-callback
	}
	callback = make(chan struct{}, len(pairs))

	t.Run("get", func(t *testing.T) {
		for _, pair := range pairs {
			pair := pair
			go func() {
				val, err := db.Get(pair[0])
				if err != nil {
					t.Errorf("Cannot get %s: %s", pairs[0], err)
				}
				if val != pair[1] {
					t.Errorf("Wrong value. Expected '%s', but got '%s'", pairs[1], val)
				}
				callback <- struct{}{}
			}()
		}

		for i := 0; i < len(pairs); i++ {
			<-callback
		}
	})
}
