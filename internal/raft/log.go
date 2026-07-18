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
	partitionId    int32

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
	partitionId  int32
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

func NewLog(port int16, topic string, partitionId int32) *Log {
	l := &Log{
		topic:       topic,
		partitionId: partitionId,
		fdCache:     make(map[string]*os.File),
	}

	var savedOffsets []int64

	files, _ := os.ReadDir(fmt.Sprintf("data/%d/log/%s/%d/", port, topic, partitionId))
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			fileName := strings.TrimSuffix(file.Name(), ".log")
			baseOffset, _ := strconv.ParseInt(fileName, 10, 64)
			savedOffsets = append(savedOffsets, baseOffset)
		}
	}

	if len(savedOffsets) == 0 {
		savedOffsets = append(savedOffsets, 0)
	}

	sort.Slice(savedOffsets, func(i, j int) bool { return savedOffsets[i] < savedOffsets[j] })
	l.segmentOffsets = savedOffsets

	latestOffset := l.segmentOffsets[len(l.segmentOffsets)-1]

	l.activeSegment = NewSegment(port, latestOffset, topic, partitionId)
	l.activeIndex = NewIndex(port, latestOffset, l.activeSegment.currentSize, topic, partitionId)

	entryCount, err := l.activeIndex.EntryCount()
	if err == nil {
		l.nextOffset = entryCount
	}

	return l
}

func (l *Log) prepareSegment(size int64, port int16) (*Segment, int64) {
	if (l.activeSegment.currentSize + size) > l.activeSegment.maxSize {
		l.activeSegment.writer.Flush()
		l.activeSegment.file.Close()
		if l.activeIndex != nil {
			l.activeIndex.writer.Flush()
			l.activeIndex.file.Close()
		}
		l.segments = append(l.segments, l.activeSegment)

		l.segmentOffsets = append(l.segmentOffsets, l.nextOffset)
		l.activeSegment = NewSegment(port, l.segmentOffsets[len(l.segmentOffsets)-1], l.topic, l.partitionId)
		l.activeIndex = NewIndex(port, l.segmentOffsets[len(l.segmentOffsets)-1], 0, l.topic, l.partitionId)
	}

	writeOffset := l.activeSegment.currentSize
	l.activeSegment.currentSize += size

	return l.activeSegment, writeOffset
}

func (l *Log) Write(payloads [][]byte, node *Node, conn net.Conn) {
	node.mu.Lock()
	if node.role != "Leader" {
		node.mu.Unlock()
		responseFail(conn)
		return
	}

	prevLen := len(node.entries)
	for _, payload := range payloads {
		node.entries = append(node.entries, Entry{timestamp: time.Now().UnixMilli(), payload: payload, term: node.currentTerm})
	}
	if len(node.entries) > 1000 {
		node.compactMemory()
	}
	snap := make([]Entry, len(node.entries[prevLen:]))
	copy(snap, node.entries[prevLen:])
	node.mu.Unlock()

	l.writeToDisk(snap, node)

	var peerCount int32
	for _, p := range node.peers {
		if p != node.port {
			peerCount++
		}
	}
	majority := int32(len(node.peers)/2 + 1)

	if peerCount == 0 {
		node.mu.Lock()
		node.commitIndex = int32(len(node.entries) - 1)
		node.mu.Unlock()
		responseSuccess(conn)
		return
	}

	var ackCount atomic.Int32
	var once sync.Once

	for _, port := range node.peers {
		if port == node.port {
			continue
		}

		node.mu.Lock()
		pNextIdx := node.nextIndex[port]
		prevLogIndex := pNextIdx - 1
		prevLogTerm := node.entries[prevLogIndex].term
		snap := make([]Entry, len(node.entries[pNextIdx:]))
		copy(snap, node.entries[pNextIdx:])
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
				partitionId:  l.partitionId,
			}

			var ok, success bool
			var err error
			for attempt := 0; attempt < 2; attempt++ {
				if attempt > 0 {
					node.connPool.Evict(p)
					time.Sleep(100 * time.Millisecond)
				}

				var pconn net.Conn
				pconn, err = node.connPool.GetOrCreateConnection(p)
				if err != nil {
					continue
				}

				pconn.SetDeadline(time.Now().Add(5 * time.Second))
				ok, success, err = sendAppendEntries(pconn, logMsg)
				if err != nil {
					continue
				}
				break
			}
			if err != nil {
				fmt.Printf("[LOG] cannot reach %d after retry: %s\n", p, err)
				node.connPool.Evict(p)
				return
			}

			if ok && success {
				node.mu.Lock()
				node.matchIndex[p] = prevIdx + int32(len(entries))
				node.nextIndex[p] = node.matchIndex[p] + 1
				node.mu.Unlock()

				if ackCount.Add(1) >= majority-1 {
					once.Do(func() {
						node.mu.Lock()
						node.commitIndex = int32(len(node.entries) - 1)
						node.mu.Unlock()
						responseSuccess(conn)
					})
				}
			} else if ok && !success {
				node.mu.Lock()
				node.nextIndex[p]--
				if node.nextIndex[p] < 1 {
					node.nextIndex[p] = 1
				}
				node.mu.Unlock()
			}
		}(port, prevLogIndex, prevLogTerm, snap)
	}

	go func() {
		time.Sleep(6 * time.Second)
		once.Do(func() {
			responseFail(conn)
		})
	}()
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
	frameSize := 2 + 2 + 4 + 4 + 4 + 4 + 4 + len(logMsg.topic) + 4 + 2
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

	binary.BigEndian.PutUint32(frame[off:], uint32(logMsg.partitionId))
	off += 4

	entriesCount := len(logMsg.entries)
	binary.BigEndian.PutUint16(frame[off:], uint16(entriesCount))

	entriesSize := 0
	for _, entry := range logMsg.entries {
		entriesSize += 12 + len(entry.payload)
	}

	allBuf := make([]byte, frameSize+entriesSize)
	copy(allBuf, frame)
	off = frameSize

	for _, entry := range logMsg.entries {
		binary.BigEndian.PutUint64(allBuf[off:], uint64(entry.timestamp))
		off += 8
		binary.BigEndian.PutUint32(allBuf[off:], uint32(len(entry.payload)))
		off += 4
		copy(allBuf[off:], entry.payload)
		off += len(entry.payload)
	}

	_, err := conn.Write(allBuf)
	if err != nil {
		return false, false, err
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
	// each entry on disk: [4B CRC32][4B term][8B timestamp][4B payload len][payload]
	bufPtr := writeBufferPool.Get().(*[]byte)
	buf := *bufPtr

	totalSize := 0
	for _, entry := range entries {
		totalSize += 4 + 4 + 8 + 4 + len(entry.payload)
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
		binary.BigEndian.PutUint32(writeBuf[off:], uint32(node.currentTerm))
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

	var batch []indexEntry

	currentOffset := l.nextOffset
	for _, entry := range entries {
		entrySize := int64(4 + 4 + 8 + 4 + len(entry.payload))
		batch = append(batch, indexEntry{currentOffset, entrySize})
		currentOffset++
	}
	l.activeIndex.WriteIndex(batch)
	l.activeIndex.writer.Flush()
	l.nextOffset += int64(len(entries))

	l.mu.Unlock()

	_, err := segment.writer.Write(writeBuf)
	if err != nil {
		writeBufferPool.Put(bufPtr)
		panic("writeToDisk failed")
	}
	segment.writer.Flush()

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

	return fmt.Sprintf("data/%d/log/%s/%d/%020d.index", port, l.topic, l.partitionId, l.segmentOffsets[lo])
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
			return nil, err
		}
		l.fdCache[indexPath] = indexFile
	}

	logFile, exists := l.fdCache[logPath]
	if !exists {
		var err error
		logFile, err = os.Open(logPath)
		if err != nil {
			return nil, err
		}
		l.fdCache[logPath] = logFile
	}

	var indexFileSize int64
	if l.activeIndex != nil && indexFile == l.activeIndex.file {
		indexFileSize = l.activeIndex.entryCount * 16
	} else {
		fi, err := indexFile.Stat()
		if err != nil {
			return nil, err
		}
		indexFileSize = fi.Size()
	}

	var entries []Entry
	var sizeCount int = 0
	var payloadLenBuf [4]byte

	for sizeCount < int(sizeLimit) {
		absOffset, err := l.activeIndex.lookupOffset(indexFile, startOffset, indexFileSize)
		if err != nil {
			break
		}

		_, err = logFile.ReadAt(payloadLenBuf[:], int64(absOffset)+12)
		if err != nil {
			break
		}
		payloadLen := binary.BigEndian.Uint32(payloadLenBuf[:])

		entryBuf := make([]byte, 20+payloadLen)
		_, err = logFile.ReadAt(entryBuf, int64(absOffset))
		if err != nil {
			break
		}

		savedChecksum := binary.BigEndian.Uint32(entryBuf[0:4])
		termAdded := binary.BigEndian.Uint32(entryBuf[4:8])
		timestamp := binary.BigEndian.Uint64(entryBuf[8:16])
		if savedChecksum != crc32.ChecksumIEEE(entryBuf[20:]) {
			return nil, fmt.Errorf("Data corrupt, checksum mismatch")
		}

		sizeCount += len(entryBuf)

		entries = append(entries, Entry{
			term:      int32(termAdded),
			timestamp: int64(timestamp),
			payload:   entryBuf[20:],
		})

		startOffset++
	}
	return entries, nil
}
