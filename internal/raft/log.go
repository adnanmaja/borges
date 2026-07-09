package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const MaxPayloadSize int = 64 * 1024

type Log struct {
	segments       []*Segment
	mu             sync.Mutex
	activeSegment  *Segment
	activeIndex    *Index
	segmentOffsets []int64
	nextOffset     int64
	topic          string

	fdCache map[string]*os.File
}

type appendLogMsg struct {
	leaderTerm   int32
	leaderPort   int16
	prevLogIndex int32
	prevLogTerm  int32
	commitIndex  int32
	entries      []Entry
	topic        string
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var errs []error
	if l.activeIndex != nil {
		if err := l.activeIndex.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if l.activeSegment != nil {
		if err := l.activeSegment.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for path, f := range l.fdCache {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
		delete(l.fdCache, path)
	}

	if len(errs) > 0 {
		return fmt.Errorf("log close errors: %v", errs)
	}
	return nil
}

var writeBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4+8+MaxPayloadSize)
		return &b
	},
}

func NewLog(port int16, topic string) *Log {
	l := &Log{
		topic:   topic,
		fdCache: make(map[string]*os.File),
	}

	var savedOffsets []int64

	files, _ := os.ReadDir(fmt.Sprintf("data/%d/log/%s/", port, topic))
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			fileName := strings.TrimSuffix(file.Name(), ".log")
			baseOffset, _ := strconv.ParseInt(fileName, 10, 64)
			savedOffsets = append(savedOffsets, baseOffset)
		}
	}

	if len(savedOffsets) == 0 {
		createInitFile(port)
		savedOffsets = append(savedOffsets, 0)
	}

	sort.Slice(savedOffsets, func(i, j int) bool { return savedOffsets[i] < savedOffsets[j] })
	l.segmentOffsets = savedOffsets

	latestOffset := l.segmentOffsets[len(l.segmentOffsets)-1]

	l.activeSegment = NewSegment(port, latestOffset, topic)
	l.activeIndex = NewIndex(port, latestOffset, l.activeSegment.currentSize, topic)

	entryCount, err := l.activeIndex.EntryCount()
	if err == nil {
		l.nextOffset = entryCount
	}

	return l
}

func createInitFile(port int16) {
	path := fmt.Sprintf("data/%d/log/%020d.log", port, 0)
	f, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		f.Close()
	}
	indexPath := fmt.Sprintf("data/%d/log/%020d.index", port, 0)
	f2, _ := os.OpenFile(indexPath, os.O_CREATE|os.O_WRONLY, 0644)
	if f2 != nil {
		f2.Close()
	}
}

func (l *Log) prepareSegment(size int64, port int16) (*Segment, int64) {
	if (l.activeSegment.currentSize + size) > l.activeSegment.maxSize {
		l.activeSegment.file.Close()
		if l.activeIndex != nil {
			l.activeIndex.writer.Flush()
			l.activeIndex.file.Close()
		}
		l.segments = append(l.segments, l.activeSegment)

		l.segmentOffsets = append(l.segmentOffsets, l.nextOffset)
		l.activeSegment = NewSegment(port, l.segmentOffsets[len(l.segmentOffsets)-1], l.topic)
		l.activeIndex = NewIndex(port, l.segmentOffsets[len(l.segmentOffsets)-1], 0, l.topic)
	}

	writeOffset := l.activeSegment.currentSize
	l.activeSegment.currentSize += size

	return l.activeSegment, writeOffset
}

func (l *Log) Write(entry []byte, node *Node) bool {
	node.entries = append(node.entries, Entry{timestamp: time.Now().UnixMilli(), payload: string(entry), term: node.currentTerm})
	l.writeToDisk(node.entries[len(node.entries)-1], node)

	var successCount int32 = 1
	var wg sync.WaitGroup

	fmt.Println("[LOG] Appending the log entry:", string(entry))

	if node.role == "Leader" {
		for _, port := range node.peers {
			if port == node.port {
				continue
			}
			wg.Add(1)
			go func(p int16) {
				defer wg.Done()

				logMsg := appendLogMsg{
					leaderTerm:   node.currentTerm,
					leaderPort:   node.port,
					prevLogIndex: node.nextIndex[p] - 1,
					prevLogTerm:  node.entries[node.nextIndex[p]-1].term,
					commitIndex:  node.commitIndex,
					entries:      node.entries[node.nextIndex[p]:],
					topic:        l.topic,
				}

				conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", p), 5*time.Second)
				if err != nil {
					fmt.Printf("[LOG] cannot reach %d: %s\n", p, err)
					return
				}
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				fmt.Println("[LOG] Sending append log entries")
				ok, success, err := sendAppendEntries(conn, logMsg)
				conn.Close()
				if err != nil {
					fmt.Printf("[LOG] cannot reach %d: %s\n", p, err)
					return
				}

				if ok && success {
					atomic.AddInt32(&successCount, 1)

					node.mu.Lock()
					node.matchIndex[p] = logMsg.prevLogIndex + int32(len(logMsg.entries))
					node.nextIndex[p] = node.matchIndex[p] + 1
					node.mu.Unlock()

				} else if ok && !success {
					node.mu.Lock()
					node.nextIndex[p]--
					node.mu.Unlock()

					if node.nextIndex[p] < 1 {
						node.nextIndex[p] = 1
					}
				}
			}(port)

		}
		wg.Wait()
		if successCount > int32(len(node.peers)/2) {
			node.commitIndex = int32(len(node.entries) - 1)
			return true
		}
	}
	return false
}

func (l *Log) Append(leaderTerm, prevLogIdx, prevLogTerm, leaderCommit int32, entries []Entry, node *Node) bool {
	if leaderTerm < node.currentTerm {
		return false
	}

	if leaderTerm > node.currentTerm {
		node.currentTerm = leaderTerm
		node.votedFor = 0
		node.saveStates()
	}

	if prevLogIdx >= int32(len(node.entries)) {
		return false
	}

	if node.entries[prevLogIdx].term != prevLogTerm {
		return false
	}

	node.entries = node.entries[:prevLogIdx+1]

	for _, entry := range entries {
		node.entries = append(node.entries, entry)
		l.writeToDisk(entry, node)
		fmt.Println("[LOG] Appending the log entry:", string(entry.payload))
	}

	if leaderCommit > node.commitIndex {
		node.commitIndex = min(leaderCommit, int32(len(node.entries)-1))
	}

	return true
}

func sendAppendEntries(conn net.Conn, logMsg appendLogMsg) (bool, bool, error) {
	frameSize := 2 + 2 + 4 + 4 + 4 + 4 + 4 + len(logMsg.topic) + 2
	frame := make([]byte, frameSize)
	off := 0

	binary.BigEndian.PutUint16(frame[off:], 0x0007)
	off += 2
	binary.BigEndian.PutUint16(frame[off:], uint16(logMsg.leaderPort))
	off += 2
	binary.BigEndian.PutUint32(frame[off:], uint32(logMsg.leaderTerm))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(logMsg.prevLogIndex))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(logMsg.prevLogTerm))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(logMsg.commitIndex))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(len(logMsg.topic)))
	off += 4

	copy(frame[off:], []byte(logMsg.topic))
	off += len(logMsg.topic)

	entriesCount := len(logMsg.entries)
	binary.BigEndian.PutUint16(frame[off:], uint16(entriesCount))

	_, err := conn.Write(frame)
	if err != nil {
		return false, false, err
	}

	for i := 0; i < entriesCount; i++ {
		timestampFrame := make([]byte, 8)
		binary.BigEndian.PutUint64(timestampFrame, uint64(logMsg.entries[i].timestamp))
		conn.Write(timestampFrame)
		entryLen := make([]byte, 4)
		binary.BigEndian.PutUint32(entryLen, uint32(len(logMsg.entries[i].payload)))
		conn.Write(entryLen)
		payload := logMsg.entries[i].payload
		conn.Write([]byte(payload))
	}

	responseFrame := make([]byte, 2)
	_, err = conn.Read(responseFrame)
	if err != nil {
		return false, false, err
	}

	responseCode := binary.BigEndian.Uint16(responseFrame)

	switch responseCode {
	case 0x0001:
		return true, true, nil
	case 0x0002:
		return true, false, nil
	}
	return false, false, nil
}

func (l *Log) writeToDisk(entry Entry, node *Node) {
	bufPtr := writeBufferPool.Get().(*[]byte)
	buf := *bufPtr

	totalSize := 4 + 8 + 4 + len(entry.payload)
	writeBuf := buf[:totalSize]
	off := 0

	checksum := crc32.ChecksumIEEE([]byte(entry.payload))
	binary.BigEndian.PutUint32(writeBuf[off:], checksum)
	off += 4
	binary.BigEndian.PutUint64(writeBuf[off:], uint64(entry.timestamp))
	off += 8
	binary.BigEndian.PutUint32(writeBuf[off:], uint32(len(entry.payload)))
	off += 4

	copy(writeBuf[off:], []byte(entry.payload))

	l.mu.Lock()
	segment, _ := l.prepareSegment(int64(totalSize), node.port)

	segment.mu.Lock()
	_, err := segment.file.Write(writeBuf)
	segment.mu.Unlock()
	if err != nil {
		l.mu.Unlock()
		writeBufferPool.Put(bufPtr)
		panic("writeToDisk failed")
	}

	l.activeIndex.WriteIndex(l.nextOffset, int64(totalSize))
	l.nextOffset++
	l.mu.Unlock()

	writeBufferPool.Put(bufPtr)

}

func (l *Log) findIndexFile(relativeOffset int64, port int16) string {
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

	return fmt.Sprintf("data/%d/log/%s/%020d.index", port, l.topic, l.segmentOffsets[lo])
}

func (l *Log) Read(startOffset int64, node *Node, sizeLimit int32) ([]Entry, error) {
	l.mu.Lock()
	indexPath := l.findIndexFile(startOffset, node.port)

	logPath := strings.TrimSuffix(indexPath, filepath.Ext(indexPath)) + ".log"
	l.mu.Unlock()

	indexFile, exists := l.fdCache[indexPath]
	if !exists {
		var err error
		indexFile, err = os.Open(indexPath)
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
		l.fdCache[indexPath] = indexFile
	}

	logFile, exists := l.fdCache[logPath]
	if !exists {
		var err error
		logFile, err = os.Open(logPath)
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
		l.fdCache[logPath] = logFile
	}

	var entries []Entry
	var sizeCount int = 0

	for sizeCount < int(sizeLimit) {
		absOffset, err := l.activeIndex.lookupOffset(indexFile, startOffset)
		if err != nil {
			break
		}

		headerBuf := make([]byte, 16)
		_, err = logFile.ReadAt(headerBuf, int64(absOffset))
		if err != nil {
			break
		}

		savedChecksum := binary.BigEndian.Uint32(headerBuf[0:4])
		timestamp := binary.BigEndian.Uint64(headerBuf[4:12])
		payloadLen := binary.BigEndian.Uint32(headerBuf[12:16])

		payloadBuf := make([]byte, payloadLen)

		_, err = logFile.ReadAt(payloadBuf, int64(absOffset)+16)
		if err != nil {
			break
		}

		if savedChecksum != crc32.ChecksumIEEE(payloadBuf) {
			return nil, fmt.Errorf("Data corrupt, checksum mismatch")
		}

		sizeCount += len(headerBuf) + len(payloadBuf)

		entries = append(entries, Entry{
			term:      node.currentTerm,
			timestamp: int64(timestamp),
			payload:   string(payloadBuf),
		})

		startOffset++
	}
	return entries, nil
}
