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

func (l *Log) Write(payloads [][]byte, node *Node) bool {
	var peerCount int
	for _, port := range node.peers {
		if port != node.port {
			peerCount++
		}
	}

	ackChan := make(chan bool, peerCount)

	if node.role == "Leader" {
		prevLen := len(node.entries)
		for _, payload := range payloads {
			node.entries = append(node.entries, Entry{timestamp: time.Now().UnixMilli(), payload: payload, term: node.currentTerm})
		}

		if len(node.entries) > 1000 {
			node.compactMemory()
		}

		l.writeToDisk(node.entries[prevLen:], node)

		for _, port := range node.peers {
			if port == node.port {
				continue
			}

			node.mu.Lock()
			pNextIdx := node.nextIndex[port]
			prevLogIndex := pNextIdx - 1
			prevLogTerm := node.entries[prevLogIndex].term
			entriesToSend := node.entries[pNextIdx:]
			node.mu.Unlock()

			go func(p int16, prevIdx int32, prevTerm int32, entries []Entry) {

				logMsg := appendLogMsg{
					leaderTerm:   node.currentTerm,
					leaderPort:   node.port,
					prevLogIndex: prevIdx,
					prevLogTerm:  prevTerm,
					commitIndex:  node.commitIndex,
					entries:      entries,
					topic:        l.topic,
				}

				conn, err := node.connPool.GetOrCreateConnection(p)
				if err != nil {
					fmt.Printf("[LOG] cannot connect %d: %s\n", p, err)
					return
				}

				conn.SetDeadline(time.Now().Add(5 * time.Second))
				ok, success, err := sendAppendEntries(conn, logMsg)
				if err != nil {
					fmt.Printf("[LOG] cannot reach %d: %s\n", p, err)
					return
				}

				if ok && success {
					node.mu.Lock()
					node.matchIndex[p] = prevIdx + int32(len(entries))
					node.nextIndex[p] = node.matchIndex[p] + 1
					node.mu.Unlock()
					ackChan <- true

				} else {
					if ok && !success {
						node.mu.Lock()
						node.nextIndex[p]--
						if node.nextIndex[p] < 1 {
							node.nextIndex[p] = 1
						}
						node.mu.Unlock()
					}
					ackChan <- false
				}
			}(port, prevLogIndex, prevLogTerm, entriesToSend)

		}

		successCount := 1
		majority := len(node.peers)/2 + 1

		for i := 0; i < peerCount; i++ {
			if <-ackChan {
				successCount++
			}

			if successCount >= majority {
				node.mu.Lock()
				node.commitIndex = int32(len(node.entries) - 1)
				node.mu.Unlock()
				return true
			}
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

	prevLen := len(node.entries)
	for _, entry := range entries {
		node.entries = append(node.entries, entry)
	}

	l.writeToDisk(node.entries[prevLen:], node)

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
		payloadFrame := make([]byte, 12+len(logMsg.entries[i].payload))
		binary.BigEndian.PutUint64(payloadFrame[0:8], uint64(logMsg.entries[i].timestamp))
		binary.BigEndian.PutUint32(payloadFrame[8:12], uint32(len(logMsg.entries[i].payload)))
		copy(payloadFrame[12:], logMsg.entries[i].payload)
		conn.Write(payloadFrame)
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

func (l *Log) writeToDisk(entries []Entry, node *Node) {
	// each entry on disk: [4B CRC32][8B timestamp][4B payload len][payload]
	bufPtr := writeBufferPool.Get().(*[]byte)
	buf := *bufPtr

	totalSize := 0
	for _, entry := range entries {
		totalSize += 4 + 8 + 4 + len(entry.payload)
	}

	if totalSize > len(buf) {
		buf = make([]byte, totalSize)
	}
	writeBuf := buf[:totalSize]
	off := 0

	for _, entry := range entries {
		checksum := crc32.ChecksumIEEE(entry.payload)
		binary.BigEndian.PutUint32(writeBuf[off:], checksum)
		off += 4
		binary.BigEndian.PutUint64(writeBuf[off:], uint64(entry.timestamp))
		off += 8
		binary.BigEndian.PutUint32(writeBuf[off:], uint32(len(entry.payload)))
		off += 4

		copy(writeBuf[off:], entry.payload)
		off += len(entry.payload)
	}

	l.mu.Lock()
	segment, _ := l.prepareSegment(int64(totalSize), node.port)

	{
		currentOffset := l.nextOffset
		for _, entry := range entries {
			entrySize := int64(4 + 8 + 4 + len(entry.payload))
			l.activeIndex.WriteIndex(currentOffset, entrySize)
			currentOffset++
		}
		l.nextOffset += int64(len(entries))
	}

	l.mu.Unlock()

	_, err := segment.file.Write(writeBuf)
	if err != nil {
		writeBufferPool.Put(bufPtr)
		panic("writeToDisk failed")
	}

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
			payload:   payloadBuf,
		})

		startOffset++
	}
	return entries, nil
}
