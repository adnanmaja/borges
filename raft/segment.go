package main

import (
	"fmt"
	"os"
	"time"
)

type Segment struct {
	file        *os.File
	path        string
	currentSize int64
	maxSize     int64
	firstOffset int64
	timestamp   int64
}

func NewSegment(port int16, offset int64) *Segment {
	filePath := fmt.Sprintf("logs/%d/%020d.log", port, offset)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("log failed log fialed log failed: %s", err))
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		panic(fmt.Sprintf("stat failed stat fialed stat failed: %s", err))
	}

	return &Segment{
		file:        file,
		path:        filePath,
		currentSize: int64(stat.Size()),
		maxSize:     1 * 1024 * 1024, //1mb
		firstOffset: offset,
		timestamp:   time.Now().UnixMilli(),
	}
}
