package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Index struct {
	file           *os.File
	path           string
	absoluteOffset int64
	mu             sync.RWMutex
}

func NewIndex(port int16, offset int64, logSize int64, topic string) *Index {
	filePath := fmt.Sprintf("data/%d/log/%s/%020d.index", port, topic, offset)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Sprintf("index failed: %s", err))
	}

	return &Index{
		file:           file,
		path:           filePath,
		absoluteOffset: logSize,
	}
}

func (idx *Index) IndexWrite(relOffset, size int64) {
	buf := make([]byte, 8+8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(relOffset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(idx.absoluteOffset))

	idx.mu.Lock()
	idx.absoluteOffset += size
	_, err := idx.file.Write(buf)
	idx.mu.Unlock()
	if err != nil {
		// whatever, not gonna error anyway trust
	}
}

func (idx *Index) EntryCount() (int64, error) {
	stat, err := idx.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size() / 16, nil
}

func (idx *Index) offsetLookup(indexFile *os.File, offsetTarget int64) (uint64, error) {
	fileInfo, err := indexFile.Stat()
	if err != nil {
		return 0, err
	}
	size := fileInfo.Size()
	if size == 0 {
		return 0, fmt.Errorf("index file is empty")
	}

	entries := size / 16
	var low int64 = 0
	high := entries - 1
	buf := make([]byte, 16)

	for low <= high {
		mid := low + (high-low)/2
		_, err = indexFile.ReadAt(buf, mid*16)
		if err != nil {
			return 0, fmt.Errorf("failed to read index at entry %d: %w", mid, err)
		}
		midOffset := binary.BigEndian.Uint64(buf[0:8])
		midPosition := binary.BigEndian.Uint64(buf[8:16])

		if midOffset == uint64(offsetTarget) {
			return midPosition, nil
		} else if midOffset < uint64(offsetTarget) {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return 0, fmt.Errorf("offset not found: %d", offsetTarget)
}
