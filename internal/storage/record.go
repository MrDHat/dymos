package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	HeaderSize   = 4 + 8 + 4 + 4 // crc32 + timestamp + key size + value size
	MaxKeySize   = 64 * 1024     // 64KB max key size
	MaxValueSize = 1024 * 1024   // 1MB max value size
)

type Record struct {
	Key   string
	Value []byte
}

func (r *Record) validate() error {
	if len(r.Key) > MaxKeySize {
		return fmt.Errorf("key size exceeds max size")
	}
	if len(r.Value) > MaxValueSize {
		return fmt.Errorf("value size exceeds max size")
	}

	return nil
}

// Layout: [CRC32][Timestamp][KeySize][ValueSize][Key][Value]
//
//	4 bytes + 8 bytes + 4 bytes + 4 bytes + KeySize + ValueSize
func (r *Record) GenerateCaskRecord() ([]byte, int, error) {
	// validate the record
	if err := r.validate(); err != nil {
		return nil, 0, err
	}

	keySize := len(r.Key)
	valueSize := len(r.Value)
	recordSize := HeaderSize + keySize + valueSize

	record := make([]byte, recordSize)

	offset := 4 // reserved for crc32

	binary.BigEndian.PutUint64(record[offset:], uint64(time.Now().UnixNano()))
	offset += 8

	binary.BigEndian.PutUint32(record[offset:], uint32(keySize))
	offset += 4

	binary.BigEndian.PutUint32(record[offset:], uint32(valueSize))
	offset += 4

	copy(record[offset:], r.Key)
	offset += keySize

	copy(record[offset:], r.Value)
	offset += valueSize

	// calculate the crc32
	crc32 := crc32.ChecksumIEEE(record[4:])
	binary.BigEndian.PutUint32(record[0:4], crc32)

	return record, valueSize, nil
}
