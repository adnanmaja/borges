package raft

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

type Segment struct {
	file        *os.File
	path        string
	currentSize int64
	maxSize     int64
	firstOffset int64
	timestamp   int64
	writer      *bufio.Writer
	mu          sync.Mutex
}

func (s *Segment) Close() error {
	s.mu.Lock()
	s.writer.Flush()
	defer s.mu.Unlock()
	return s.file.Close()
}

func NewSegment(port int16, offset int64, topic string, partitionId int32) *Segment {
	filePath := fmt.Sprintf("data/%d/log/%s/%d/%020d.log", port, topic, partitionId, offset)

	err := os.MkdirAll(fmt.Sprintf("data/%d/log/%s/%d", port, topic, partitionId), 0755)
	if err != nil {
		panic(fmt.Sprintf("log failed: %s", err))
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("log failed: %s", err))
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		panic(fmt.Sprintf("stat failed: %s", err))
	}

	return &Segment{
		file:        file,
		path:        filePath,
		currentSize: int64(stat.Size()),
		maxSize:     1 * 1024 * 1024,
		firstOffset: offset,
		timestamp:   time.Now().UnixMilli(),
		writer:      bufio.NewWriterSize(file, 65536),
	}
}
