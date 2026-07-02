package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Index struct {
	file           *os.File
	path           string
	absoluteOffset int64
}

func newIndex(topic string, offset int64, logSize int64) *Index {
	filePath := fmt.Sprintf("logs/%s/%020d.index", topic, offset)

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

func (idx *Index) indexWrite(relOffset, size int64) {
	// 8 byte relative offset & 8 byte absolute offset (physical byte offset)
	buf := make([]byte, 8+8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(relOffset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(idx.absoluteOffset))

	idx.absoluteOffset += size

	_, err := idx.file.Write(buf)
	if err != nil {
		panic(fmt.Sprintf("indexWrite problem: %s", err))
	}
}

func (l *Log) offsetLookup(indexPath string, offsetTarget int64) (uint64, error) {
	fmt.Println("[DEBUG] Opening index file: ", indexPath)

	file, err := os.Open(indexPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	fileInfo, err := os.Stat(indexPath)
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

		_, err = file.ReadAt(buf, mid*16)
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

	return 0, fmt.Errorf("this aint here: %d", offsetTarget)
}
