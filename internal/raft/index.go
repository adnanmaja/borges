package raft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

type Index struct {
	file       *os.File
	path       string
	byteOffset int64
	entryCount int64
	mu         sync.RWMutex
	writer     *bufio.Writer
}

type indexEntry struct {
	relOffset int64
	size      int64
}

func NewIndex(port int16, offset int64, logSize int64, topic string, partitionId int32) *Index {
	filePath := fmt.Sprintf("data/%d/log/%s/%d/%020d.index", port, topic, partitionId, offset)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Sprintf("index failed: %s", err))
	}

	var entryCount int64
	stat, err := file.Stat()
	if err == nil {
		entryCount = stat.Size() / 16
	}

	return &Index{
		file:       file,
		path:       filePath,
		byteOffset: logSize,
		entryCount: entryCount,
		writer:     bufio.NewWriterSize(file, 4096),
	}
}

func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.writer.Flush(); err != nil {
		return err
	}
	return idx.file.Close()
}

func (idx *Index) WriteIndex(entries []indexEntry) {
	buf := make([]byte, len(entries)*16)
	off := 0
	currentOffset := idx.byteOffset
	for _, e := range entries {
		binary.BigEndian.PutUint64(buf[off:], uint64(e.relOffset))
		binary.BigEndian.PutUint64(buf[off+8:], uint64(currentOffset))
		currentOffset += e.size
		off += 16
	}
	idx.mu.Lock()
	idx.byteOffset = currentOffset
	idx.entryCount += int64(len(entries))
	idx.writer.Write(buf)
	idx.mu.Unlock()
}

func (idx *Index) EntryCount() (int64, error) {
	return idx.entryCount, nil
}

func (idx *Index) lookupOffset(indexFile *os.File, offsetTarget int64, fileSize int64) (uint64, error) {
	if fileSize == 0 {
		return 0, fmt.Errorf("index file is empty")
	}

	entries := fileSize / 16
	var low int64 = 0
	high := entries - 1
	buf := make([]byte, 16)

	for low <= high {
		mid := low + (high-low)/2
		_, err := indexFile.ReadAt(buf, mid*16)
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
