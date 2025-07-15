package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

/*
TODO: write to file atomically, so that if the process is killed, the data is not lost
*/

type Storage interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
}

type StorageOptions struct {
	maxFileSizeInBytes int64
}

type storage struct {
	dir                    string
	currentFile            *os.File
	currentFileSizeInBytes int64
	maxFileSizeInBytes     int64
	keydir                 map[string]struct {
		File      string
		ValueSize int
		ValuePos  int64
		Timestamp int64
	}
	dataFiles map[string]*os.File
}

func NewStorage(dir string, opts StorageOptions) Storage {
	// create dir if not exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}

	s := &storage{
		dir:                dir,
		maxFileSizeInBytes: opts.maxFileSizeInBytes,
		keydir: make(map[string]struct {
			File      string
			ValueSize int
			ValuePos  int64
			Timestamp int64
		}),
		dataFiles: make(map[string]*os.File),
	}
	currentFile, err := s.createDataFile()
	if err != nil {
		panic(err)
	}
	s.currentFile = currentFile
	s.dataFiles[currentFile.Name()] = currentFile

	// TODO: read all data files in read only mode
	// This will also need keydir rebuild

	return s
}

func (s *storage) Put(key string, value []byte) error {
	// prepare the data to be written
	record := Record{Key: key, Value: value}
	data, valueSize, err := record.GenerateCaskRecord()
	if err != nil {
		return err
	}

	// check if current file is full
	if s.currentFileSizeInBytes+int64(len(data)) > s.maxFileSizeInBytes {
		oldFileName := s.currentFile.Name()
		err = s.currentFile.Close()
		if err != nil {
			return err
		}

		// Reopen the closed file in read-only mode for future reads
		readOnlyFile, err := os.Open(oldFileName)
		if err != nil {
			return err
		}
		s.dataFiles[oldFileName] = readOnlyFile

		// create a new file
		currentFile, err := s.createDataFile()
		if err != nil {
			return err
		}
		s.currentFile = currentFile
		s.currentFileSizeInBytes = 0
		s.dataFiles[currentFile.Name()] = currentFile
	}

	// write to file
	s.currentFile.Write(data)
	// flush to disk
	err = s.currentFile.Sync()
	if err != nil {
		return err
	}

	s.currentFileSizeInBytes += int64(len(data))

	// update keydir
	s.keydir[key] = struct {
		File      string
		ValueSize int
		ValuePos  int64
		Timestamp int64
	}{
		File:      s.currentFile.Name(),
		ValueSize: valueSize,
		ValuePos:  s.currentFileSizeInBytes - int64(valueSize),
		Timestamp: time.Now().UnixNano(),
	}

	return nil
}

func (s *storage) Get(key string) ([]byte, error) {
	entry, ok := s.keydir[key]
	if !ok {
		return nil, nil
	}

	// read from file
	file, ok := s.dataFiles[entry.File]
	if !ok {
		return nil, fmt.Errorf("file not found")
	}

	// read from file
	file.Seek(entry.ValuePos, io.SeekStart)
	buffer := make([]byte, entry.ValueSize)
	_, err := file.Read(buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (s *storage) createDataFile() (*os.File, error) {
	// create the data file
	timestamp := time.Now().UnixNano()
	dataFile := filepath.Join(s.dir, fmt.Sprintf("%d.data", timestamp))
	file, err := os.Create(dataFile)
	if err != nil {
		return nil, err
	}

	return file, nil
}
