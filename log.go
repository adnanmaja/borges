package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Record struct {
	Timestamp int64
	Payload   []byte
}

type Log struct {
	segments       []*Segment
	mu             sync.RWMutex
	activeSegment  *Segment
	segmentOffsets []int64
	activeIndex    *Index
	nextOffset     int64
	topic          string
}

func NewLog(topic string) *Log {
	var savedOffsets []int64

	l := &Log{
		topic: topic,
	}

	err := spawnInitFile(l.topic)
	if err != nil {
		panic(fmt.Sprintf("error spawning file: %s", err))
	}

	files, _ := os.ReadDir(fmt.Sprintf("logs/%s", l.topic))

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			fileName := strings.TrimSuffix(file.Name(), ".log")
			baseOffset, _ := strconv.ParseInt(fileName, 10, 64)
			savedOffsets = append(savedOffsets, baseOffset)
		}
	}

	l.segmentOffsets = savedOffsets

	latestOffset := l.segmentOffsets[len(l.segmentOffsets)-1]

	latestFile, err := os.Stat(fmt.Sprintf("logs/%s/%020d.index", l.topic, latestOffset))
	if err != nil {
		panic(fmt.Sprintf("Error at newLog(): %s", err))
	}

	fmt.Println("[DEBUG] Latest file size:", latestFile.Size())
	l.nextOffset = latestOffset + latestFile.Size()/16 // latest offset + entries inside the latest offset's .index file
	fmt.Println("[DEBUG] l.nextOffset:", l.nextOffset)

	l.activeSegment = newSegment(l.topic, latestOffset)
	l.activeIndex = newIndex(l.topic, latestOffset)
	return l
}

func spawnInitFile(topic string) error {
	os.MkdirAll(fmt.Sprintf("logs/%s", topic), 0755)
	fmt.Printf("[DEBUG] Created directory: logs/%s\n", topic)

	logPath := fmt.Sprintf("logs/%s/%020d.log", topic, 0)
	indexPath := fmt.Sprintf("logs/%s/%020d.index", topic, 0)

	_, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0644)
	_, err = os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY, 0644)

	return err
}

func (l *Log) prepareSegment(size int64) (*Segment, int64) {

	if (l.activeSegment.currentSize + size) > l.activeSegment.maxSize {
		l.activeSegment.file.Close()
		l.segments = append(l.segments, l.activeSegment)

		l.segmentOffsets = append(l.segmentOffsets, l.nextOffset)
		l.activeSegment = newSegment(l.topic, l.segmentOffsets[len(l.segmentOffsets)-1])
		l.activeIndex = newIndex(l.topic, l.segmentOffsets[len(l.segmentOffsets)-1])
	}

	writeOffset := l.activeSegment.currentSize
	l.activeSegment.currentSize += size

	fmt.Println("[DEBUG] l.nextOffset: ", l.nextOffset)
	return l.activeSegment, writeOffset
}

func (l *Log) findIndexFile(relativeOffset int64) string {
	if len(l.segmentOffsets) == 0 {
		return ""
	}

	latestSegmentOffset := l.segmentOffsets[len(l.segmentOffsets)-1]
	if relativeOffset >= latestSegmentOffset {
		return fmt.Sprintf("logs/%s/%020d.index", l.topic, latestSegmentOffset)
	}

	for i := 0; i < len(l.segmentOffsets)-1; i++ {
		if relativeOffset >= l.segmentOffsets[i] && relativeOffset < l.segmentOffsets[i+1] {
			return fmt.Sprintf("logs/%s/%020d.index", l.topic, l.segmentOffsets[i])
		}
	}
	return ""
}

func (l *Log) Write(payload []byte) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// [4 bytes for the payload length][8 bytes timestamp][payload]
	buf := make([]byte, 4+8+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(buf[4:12], uint64(time.Now().UnixMilli()))
	copy(buf[12:], payload)

	segment, _ := l.prepareSegment(int64(len(buf)))
	l.activeIndex.indexWrite(l.nextOffset, int64(len(buf)))

	_, err := segment.file.Write(buf)
	if err != nil {
		return "", err
	}

	fmt.Printf("[DEBUG] Relative offset: %d, At file: %s, Size: %d B \n", l.nextOffset, l.activeSegment.path, len(buf))
	fmt.Printf("[DEBUG] Current segment size: %d B, first offset: %d \n", l.activeSegment.currentSize, l.segmentOffsets[0])

	l.nextOffset++

	return segment.path, nil
}

func (l *Log) Read(relOffset int64) (Record, error) {
	indexPath := l.findIndexFile(relOffset)

	absOffset, err := l.offsetLookup(indexPath, relOffset)
	if err != nil {
		return Record{}, err
	}

	logPath := strings.TrimSuffix(indexPath, filepath.Ext(indexPath)) + ".log"

	fmt.Printf("[DEBUG] Reading log file: %s/%s, with absOffset: %d \n", l.topic, logPath, absOffset)

	file, err := os.Open(logPath)
	if err != nil {
		return Record{}, err
	}

	_, err = file.Seek(int64(absOffset), io.SeekStart)
	if err != nil {
		return Record{}, err
	}

	headerBuf := make([]byte, 12)
	_, err = io.ReadFull(file, headerBuf)
	if err != nil {
		return Record{}, err
	}

	payloadLength := binary.BigEndian.Uint32(headerBuf[0:4])
	timestamp := binary.BigEndian.Uint64(headerBuf[4:12])

	payloadBuf := make([]byte, payloadLength)

	_, err = io.ReadFull(file, payloadBuf)

	var r Record
	r.Timestamp = int64(timestamp)
	r.Payload = payloadBuf

	defer file.Close()

	return r, nil
}
