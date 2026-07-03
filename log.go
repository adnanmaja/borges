package main

import (
	"encoding/binary"
	"fmt"
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

	readCache map[string]*os.File
}

var writeBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4+8+MaxPayloadSize)
		return &b
	},
}

func NewLog(topic string) *Log {
	var savedOffsets []int64

	l := &Log{
		topic:     topic,
		readCache: make(map[string]*os.File),
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

	if debug {
		fmt.Println("[DEBUG] Latest file size:", latestFile.Size())
	}
	l.nextOffset = latestOffset + latestFile.Size()/16 // latest offset + entries inside the latest offset's .index file

	if debug {
		fmt.Println("[DEBUG] l.nextOffset:", l.nextOffset)
	}

	l.activeSegment = newSegment(l.topic, latestOffset)
	l.activeIndex = newIndex(l.topic, latestOffset, l.activeSegment.currentSize)

	go l.cleanOldFiles()

	return l
}

func spawnInitFile(topic string) error {
	os.MkdirAll(fmt.Sprintf("logs/%s", topic), 0755)
	if debug {
		fmt.Printf("[DEBUG] Created directory: logs/%s\n", topic)
	}

	logPath := fmt.Sprintf("logs/%s/%020d.log", topic, 0)
	indexPath := fmt.Sprintf("logs/%s/%020d.index", topic, 0)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	f, err = os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	return nil
}

func (l *Log) prepareSegment(size int64) (*Segment, int64) {

	if (l.activeSegment.currentSize + size) > l.activeSegment.maxSize {
		l.activeSegment.file.Close()
		if l.activeIndex != nil {
			l.activeIndex.writer.Flush()
			l.activeIndex.file.Close()
		}
		l.segments = append(l.segments, l.activeSegment)

		l.segmentOffsets = append(l.segmentOffsets, l.nextOffset)
		l.activeSegment = newSegment(l.topic, l.segmentOffsets[len(l.segmentOffsets)-1])
		l.activeIndex = newIndex(l.topic, l.segmentOffsets[len(l.segmentOffsets)-1], 0)
	}

	writeOffset := l.activeSegment.currentSize
	l.activeSegment.currentSize += size

	if debug {
		fmt.Println("[DEBUG] l.nextOffset: ", l.nextOffset)
	}
	return l.activeSegment, writeOffset
}

func (l *Log) findIndexFile(relativeOffset int64) string {
	if len(l.segmentOffsets) == 0 {
		return ""
	}

	// Binary search for the segment whose base offset <= relativeOffset.
	// segmentOffsets is sorted ascending (segments appended in order).
	lo, hi := 0, len(l.segmentOffsets)-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if l.segmentOffsets[mid] <= relativeOffset {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	if l.segmentOffsets[lo] > relativeOffset {
		return ""
	}

	return fmt.Sprintf("logs/%s/%020d.index", l.topic, l.segmentOffsets[lo])
}

func (l *Log) Write(payload []byte) (string, error) {
	// [4 bytes for the payload length][8 bytes timestamp][payload]
	bufPtr := writeBufferPool.Get().(*[]byte)
	buf := *bufPtr

	totalSize := 4 + 8 + len(payload)
	writeBuf := buf[:totalSize]

	binary.BigEndian.PutUint32(writeBuf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(writeBuf[4:12], uint64(time.Now().UnixMilli()))
	copy(writeBuf[12:], payload)

	l.mu.Lock()
	segment, _ := l.prepareSegment(int64(totalSize))

	segment.mu.Lock()
	_, err := segment.file.Write(writeBuf)
	segment.mu.Unlock()
	if err != nil {
		l.mu.Unlock()
		writeBufferPool.Put(bufPtr)
		return "", err
	}

	l.activeIndex.indexWrite(l.nextOffset, int64(totalSize))

	if debug {
		fmt.Printf("[DEBUG] Relative offset: %d, At file: %s, Size: %d B \n", l.nextOffset, l.activeSegment.path, len(buf))
		fmt.Printf("[DEBUG] Current segment size: %d B, first offset: %d \n", l.activeSegment.currentSize, l.segmentOffsets[0])
	}

	l.nextOffset++
	l.mu.Unlock()

	writeBufferPool.Put(bufPtr)

	return segment.path, nil
}

func (l *Log) Read(relOffset int64) (Record, error) {
	l.mu.Lock()
	indexPath := l.findIndexFile(relOffset)
	absOffset, err := l.offsetLookup(indexPath, relOffset)

	if err != nil {
		l.mu.Unlock()
		return Record{}, err
	}

	logPath := strings.TrimSuffix(indexPath, filepath.Ext(indexPath)) + ".log"

	file, exists := l.readCache[logPath]
	if !exists {
		var err error
		file, err = os.Open(logPath)
		if err != nil {
			l.mu.Unlock()
			return Record{}, err
		}
		l.readCache[logPath] = file
	}

	l.mu.Unlock()

	if debug {
		fmt.Printf("[DEBUG] Reading log file: %s/%s, with absOffset: %d \n", l.topic, logPath, absOffset)
	}

	headerBuf := make([]byte, 12)
	_, err = file.ReadAt(headerBuf, int64(absOffset))
	if err != nil {
		return Record{}, err
	}

	payloadLength := binary.BigEndian.Uint32(headerBuf[0:4])
	timestamp := binary.BigEndian.Uint64(headerBuf[4:12])

	if payloadLength > MaxPayloadSize {
		return Record{}, fmt.Errorf("payload length %d exceeds max %d", payloadLength, MaxPayloadSize)
	}

	payloadBuf := make([]byte, payloadLength)

	_, err = file.ReadAt(payloadBuf, int64(absOffset)+12)
	if err != nil {
		return Record{}, err
	}

	return Record{
		Timestamp: int64(timestamp),
		Payload:   payloadBuf,
	}, nil
}

func (l *Log) cleanOldFiles() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Printf("\n[DEBUG] File cleaner woken up\n")
		currentTime := time.Now().UnixMilli()

		l.mu.Lock()

		var activeSegments []*Segment
		var activeOffsets []int64
		var expiredSegments []*Segment

		for i, segment := range l.segments {
			if currentTime-segment.timestamp >= int64(10*60*1000) {
				expiredSegments = append(expiredSegments, segment)
			} else {
				activeSegments = append(activeSegments, segment)
				if i < len(l.segmentOffsets) {
					activeOffsets = append(activeOffsets, l.segmentOffsets[i])
				}
			}
		}

		l.segments = activeSegments
		l.segmentOffsets = activeOffsets

		l.mu.Unlock()

		for _, segment := range expiredSegments {
			segment.file.Close()

			l.mu.Lock()
			delete(l.readCache, segment.path)
			l.mu.Unlock()

			os.Remove(segment.path)
			os.Remove(strings.TrimSuffix(segment.path, ".log") + ".index")
		}
	}
}
