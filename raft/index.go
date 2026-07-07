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

func NewIndex(port int16, offset int64, logSize int64) *Index {
	filePath := fmt.Sprintf("logs/%d/%020d", port, offset)

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
	// 8 byte relative offset & 8 byte absolute offset (physical byte offset)
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
