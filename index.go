package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Index struct {
	file           *os.File
	path           string
	absoluteOffset int64
}

func newIndex(topic string, offset int64) *Index {
	filePath := fmt.Sprintf("logs/%s/%020d.index", topic, offset)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("index failed index fialed index failed: %s", err))
	}

	return &Index{
		file:           file,
		path:           filePath,
		absoluteOffset: 0,
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

	buf := make([]byte, 16)

	for {
		_, err := io.ReadFull(file, buf)
		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, err
		}

		relOffset := binary.BigEndian.Uint64(buf[0:8])

		if relOffset == uint64(offsetTarget) {
			return binary.BigEndian.Uint64((buf[8:16])), nil
		}
	}

	return 0, fmt.Errorf("this aint here: %d", offsetTarget)
}
