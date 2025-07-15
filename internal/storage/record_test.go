package storage

import (
	"strings"
	"testing"
)

func TestRecord_validate(t *testing.T) {
	t.Run("valid record", func(t *testing.T) {
		r := &Record{Key: "foo", Value: []byte("bar")}
		if err := r.validate(); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("key size exceeds max", func(t *testing.T) {
		bigKey := strings.Repeat("a", MaxKeySize+1)
		r := &Record{Key: bigKey, Value: []byte("bar")}
		if err := r.validate(); err == nil {
			t.Error("expected error for key size, got nil")
		}
	})

	t.Run("value size exceeds max", func(t *testing.T) {
		bigValue := make([]byte, MaxValueSize+1)
		r := &Record{Key: "foo", Value: bigValue}
		if err := r.validate(); err == nil {
			t.Error("expected error for value size, got nil")
		}
	})
}

func TestRecord_GenerateCaskRecord(t *testing.T) {
	t.Run("valid record", func(t *testing.T) {
		r := &Record{Key: "foo", Value: []byte("bar")}
		record, valueSize, err := r.GenerateCaskRecord()
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if valueSize != len(r.Value) {
			t.Errorf("expected valueSize %d, got %d", len(r.Value), valueSize)
		}
		if len(record) != HeaderSize+len(r.Key)+len(r.Value) {
			t.Errorf("unexpected record length: got %d, want %d", len(record), HeaderSize+len(r.Key)+len(r.Value))
		}
	})

	t.Run("invalid key size", func(t *testing.T) {
		bigKey := strings.Repeat("a", MaxKeySize+1)
		r := &Record{Key: bigKey, Value: []byte("bar")}
		_, _, err := r.GenerateCaskRecord()
		if err == nil {
			t.Error("expected error for key size, got nil")
		}
	})

	t.Run("invalid value size", func(t *testing.T) {
		bigValue := make([]byte, MaxValueSize+1)
		r := &Record{Key: "foo", Value: bigValue}
		_, _, err := r.GenerateCaskRecord()
		if err == nil {
			t.Error("expected error for value size, got nil")
		}
	})
}
