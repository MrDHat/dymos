package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageIntegration(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := NewStorage(dir, StorageOptions{maxFileSizeInBytes: 1024 * 1024})

	// Test Put and Get
	key := "foo"
	value := []byte("bar")
	if err := store.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(value) {
		t.Errorf("Get returned wrong value: got %q, want %q", got, value)
	}

	// Test overwrite
	newValue := []byte("baz")
	if err := store.Put(key, newValue); err != nil {
		t.Fatalf("Put (overwrite) failed: %v", err)
	}
	got, err = store.Get(key)
	if err != nil {
		t.Fatalf("Get after overwrite failed: %v", err)
	}
	if string(got) != string(newValue) {
		t.Errorf("Get after overwrite returned wrong value: got %q, want %q", got, newValue)
	}

	// Test non-existent key
	nonExistent, err := store.Get("doesnotexist")
	if err != nil {
		t.Fatalf("Get non-existent key failed: %v", err)
	}
	if nonExistent != nil {
		t.Errorf("Get non-existent key returned non-nil: %v", nonExistent)
	}

}

func TestStorageFileRollover(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage_rollover_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Use a small file size to force rollover
	store := NewStorage(dir, StorageOptions{maxFileSizeInBytes: 128})
	numRecords := 10
	values := make(map[string][]byte)
	for i := range numRecords {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		values[key] = value
		if err := store.Put(key, value); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// All records should be retrievable
	for key, want := range values {
		got, err := store.Get(key)
		if err != nil {
			t.Errorf("Get failed for %s: %v", key, err)
			continue
		}
		if string(got) != string(want) {
			t.Errorf("Get returned wrong value for %s: got %q, want %q", key, got, want)
		}
	}

	// There should be more than one data file in the directory
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	dataFileCount := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".data" {
			dataFileCount++
		}
	}
	if dataFileCount < 2 {
		t.Errorf("expected at least 2 data files, got %d", dataFileCount)
	}
}
