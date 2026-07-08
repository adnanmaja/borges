package main

import (
	"encoding/binary"
	"fmt"
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

type Log struct {
	segments       []*Segment
	mu             sync.Mutex
	activeSegment  *Segment
	activeIndex    *Index
	segmentOffsets []int64
	nextOffset     int64
	topic          string
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

func NewLog(port int16, topic string) *Log {
	l := &Log{
		topic: topic,
	}

	var savedOffsets []int64

	files, _ := os.ReadDir(fmt.Sprintf("data/%d/%s/", port, topic))
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			fileName := strings.TrimSuffix(file.Name(), ".log")
			baseOffset, _ := strconv.ParseInt(fileName, 10, 64)
			savedOffsets = append(savedOffsets, baseOffset)
		}
	}

	if len(savedOffsets) == 0 {
		spawnInitFile(port)
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

func spawnInitFile(port int16) {
	path := fmt.Sprintf("logs/%d/%020d.log", port, 0)
	f, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		f.Close()
	}
	ipath := fmt.Sprintf("logs/%d/%020d.index", port, 0)
	f2, _ := os.OpenFile(ipath, os.O_CREATE|os.O_WRONLY, 0644)
	if f2 != nil {
		f2.Close()
	}
}

// func (node *Node) loadEntriesFromDisk() {
// 	l := node.log
// 	l.mu.Lock()
// 	offsets := l.segmentOffsets
// 	l.mu.Unlock()

// 	for _, baseOffset := range offsets {
// 		logPath := fmt.Sprintf("logs/%d/%020d.log", node.port, baseOffset)
// 		logFile, err := os.Open(logPath)
// 		if err != nil {
// 			continue
// 		}

// 		for {
// 			header := make([]byte, 12)
// 			_, err := logFile.Read(header)
// 			if err != nil {
// 				break
// 			}
// 			timestamp := int64(binary.BigEndian.Uint64(header[0:8]))
// 			payloadLen := binary.BigEndian.Uint32(header[8:12])

// 			payload := make([]byte, payloadLen)
// 			_, err = logFile.Read(payload)
// 			if err != nil {
// 				break
// 			}

// 			node.logs = append(node.logs, Entry{
// 				timestamp: timestamp,
// 				payload:   string(payload),
// 				term:      0,
// 			})
// 		}

// 		logFile.Close()
// 	}
// }

func (l *Log) prepareSegment(size int64, port int16) (*Segment, int64) {
	if (l.activeSegment.currentSize + size) > l.activeSegment.maxSize {
		l.activeSegment.file.Close()
		if l.activeIndex != nil {
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

func (l *Log) WriteLog(entry []byte, node *Node) bool {
	node.logs = append(node.logs, Entry{timestamp: time.Now().UnixMilli(), payload: string(entry), term: node.currentTerm})
	l.writeToDisk(node.logs[len(node.logs)-1], node)

	var successCount int32 = 1
	var wg sync.WaitGroup

	fmt.Println("[LOG] Appending the log entry:", string(entry))

	if node.role == "Leader" {
		ports := []int16{8080, 8081, 8082}
		for _, port := range ports {
			if port == node.port {
				continue
			}
			wg.Add(1)
			go func(p int16) {
				defer wg.Done()

				conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", p), 5*time.Second)
				if err != nil {
					fmt.Printf("[LOG] cannot reach %d: %s\n", p, err)
					return
				}
				logMsg := appendLogMsg{
					leaderTerm:   node.currentTerm,
					leaderPort:   node.port,
					prevLogIndex: node.nextIndex[p] - 1,
					prevLogTerm:  node.logs[node.nextIndex[p]-1].term,
					commitIndex:  node.commitIndex,
					entries:      node.logs[node.nextIndex[p]:],
					topic:        l.topic,
				}

				ok, success, err := sendLog(conn, logMsg)
				fmt.Println("[LOG] Sending append log entries")

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
		if successCount > int32(len(ports)/2) {
			node.commitIndex = int32(len(node.logs) - 1)
			return true
		}
	}
	return false
}

func (l *Log) AppendLog(leaderTerm, prevLogIdx, prevLogTerm, leaderCommit int32, entries []Entry, node *Node) bool {
	if leaderTerm < node.currentTerm {
		return false
	}

	if prevLogIdx >= int32(len(node.logs)) {
		return false
	}

	if node.logs[prevLogIdx].term != prevLogTerm {
		return false
	}

	node.logs = node.logs[:prevLogIdx+1]

	for _, entry := range entries {
		node.logs = append(node.logs, entry)
		l.writeToDisk(entry, node)
		fmt.Println("[LOG] Appending the log entry:", string(entry.payload))
	}

	if leaderCommit > node.commitIndex {
		node.commitIndex = min(leaderCommit, int32(len(node.logs)-1))
	}

	return true
}

func sendLog(conn net.Conn, logMsg appendLogMsg) (bool, bool, error) {
	// [0x0007][2B port][4B lead's term][4B prevLogIdx][4B prevLogTerm][4b leadCommitIndex][4B topic len][topic][2B entryCount] + loop([8B timestamp][4B entryLen][entry])
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
	totalSize := 8 + 4 + len(entry.payload)
	buf := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint64(buf[off:], uint64(entry.timestamp))
	off += 8

	binary.BigEndian.PutUint32(buf[off:], uint32(len(entry.payload)))
	off += 4

	copy(buf[off:], []byte(entry.payload))

	l.mu.Lock()
	defer l.mu.Unlock()

	segment, _ := l.prepareSegment(int64(totalSize), node.port)
	_, err := segment.file.Write(buf)
	if err != nil {
		panic("writeToDisk failed")
	}

	l.activeIndex.IndexWrite(l.nextOffset, int64(totalSize))
	l.nextOffset++
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

	return fmt.Sprintf("data/%d/log/%020d.index", port, l.segmentOffsets[lo])
}

func (l *Log) readLog(targetOffset int64, node *Node) (Entry, error) {
	l.mu.Lock()
	indexPath := l.findIndexFile(targetOffset, node.port)

	logPath := strings.TrimSuffix(indexPath, filepath.Ext(indexPath)) + ".log"
	l.mu.Unlock()

	indexFile, err := os.Open(indexPath)
	if err != nil {
		return Entry{}, err
	}

	logFile, err := os.Open(logPath)
	if err != nil {
		return Entry{}, err
	}

	absOffset, err := l.activeIndex.offsetLookup(indexFile, targetOffset)
	if err != nil {
		return Entry{}, err
	}

	// each log on disk: [8B timestamp][4B payload len][payload]
	headerBuf := make([]byte, 12)
	_, err = logFile.ReadAt(headerBuf, int64(absOffset))
	if err != nil {
		return Entry{}, err
	}

	timestamp := binary.BigEndian.Uint64(headerBuf[0:8])
	payloadLen := binary.BigEndian.Uint32(headerBuf[8:12])

	payloadBuf := make([]byte, payloadLen)

	_, err = logFile.ReadAt(payloadBuf, int64(absOffset)+12)
	if err != nil {
		return Entry{}, err
	}

	return Entry{
		term:      node.currentTerm,
		timestamp: int64(timestamp),
		payload:   string(payloadBuf),
	}, nil
}
