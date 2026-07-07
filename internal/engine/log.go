package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
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
	stopCh         chan struct{}

	fdCache map[string]*os.File
}

var writeBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4+8+MaxPayloadSize)
		return &b
	},
}

func NewLog(topic string, stopCh chan struct{}) *Log {
	var savedOffsets []int64

	l := &Log{
		topic:   topic,
		fdCache: make(map[string]*os.File),
		stopCh:  stopCh,
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

	sort.Slice(savedOffsets, func(i, j int) bool { return savedOffsets[i] < savedOffsets[j] })
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

	l.activeSegment = NewSegment(l.topic, latestOffset)
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
		l.activeSegment = NewSegment(l.topic, l.segmentOffsets[len(l.segmentOffsets)-1])
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
	// [4 bytes checksum][4 bytes for the payload length][8 bytes timestamp][payload]
	bufPtr := writeBufferPool.Get().(*[]byte)
	buf := *bufPtr

	totalSize := 4 + 4 + 8 + len(payload)
	writeBuf := buf[:totalSize]

	checksum := crc32.ChecksumIEEE(payload)

	binary.BigEndian.PutUint32(writeBuf[0:4], checksum)
	binary.BigEndian.PutUint32(writeBuf[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint64(writeBuf[8:16], uint64(time.Now().UnixMilli()))
	copy(writeBuf[16:], payload)

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

	l.activeIndex.IndexWrite(l.nextOffset, int64(totalSize))

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

	logPath := strings.TrimSuffix(indexPath, filepath.Ext(indexPath)) + ".log"

	indexFile, exists := l.fdCache[indexPath]
	if !exists {
		var err error
		indexFile, err = os.Open(indexPath)
		if err != nil {
			l.mu.Unlock()
			return Record{}, err
		}
		l.fdCache[indexPath] = indexFile
	}

	logFile, exists := l.fdCache[logPath]
	if !exists {
		var err error
		logFile, err = os.Open(logPath)
		if err != nil {
			l.mu.Unlock()
			return Record{}, err
		}
		l.fdCache[logPath] = logFile
	}

	absOffset, err := l.activeIndex.offsetLookup(indexFile, relOffset)
	if err != nil {
		l.mu.Unlock()
		return Record{}, err
	}

	l.mu.Unlock()

	if debug {
		fmt.Printf("[DEBUG] Reading log file: %s/%s, with absOffset: %d \n", l.topic, logPath, absOffset)
	}

	headerBuf := make([]byte, 16)
	_, err = logFile.ReadAt(headerBuf, int64(absOffset))
	if err != nil {
		return Record{}, err
	}

	savedChecksum := binary.BigEndian.Uint32(headerBuf[0:4])
	payloadLength := binary.BigEndian.Uint32(headerBuf[4:8])
	timestamp := binary.BigEndian.Uint64(headerBuf[8:16])

	if payloadLength > MaxPayloadSize {
		return Record{}, fmt.Errorf("payload length %d exceeds max %d", payloadLength, MaxPayloadSize)
	}

	payloadBuf := make([]byte, payloadLength)

	_, err = logFile.ReadAt(payloadBuf, int64(absOffset)+16)
	if err != nil {
		return Record{}, err
	}

	actualChecksum := crc32.ChecksumIEEE(payloadBuf)
	if savedChecksum != actualChecksum {
		return Record{}, fmt.Errorf("Data corrupt, checksum mismatch")
	}

	return Record{
		Timestamp: int64(timestamp),
		Payload:   payloadBuf,
	}, nil
}

func (l *Log) cleanOldFiles() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
				indexPath := strings.TrimSuffix(segment.path, ".log") + ".index"

				l.mu.Lock()
				delete(l.fdCache, segment.path)
				delete(l.fdCache, indexPath)
				l.mu.Unlock()

				os.Remove(segment.path)
				os.Remove(indexPath)
			}

		case <-l.stopCh:
			return
		}
	}
}
