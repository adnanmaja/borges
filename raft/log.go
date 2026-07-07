package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type appendLogMsg struct { // append log message model
	leaderTerm   int32
	leaderPort   int16
	prevLogIndex int32
	prevLogTerm  int32
	commitIndex  int32
	entries      []Entry
}

func (node *Node) WriteLog(entry []byte) bool {
	node.logs = append(node.logs, Entry{timestamp: time.Now().UnixMilli(), payload: string(entry), term: node.currentTerm})
	writeToDisk(node.logs[len(node.logs)-1], node.port)

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
					fmt.Printf("[LOG] cannot reach %d: %s\n", port, err)
					return
				}
				logMsg := appendLogMsg{
					leaderTerm:   node.currentTerm,
					leaderPort:   node.port,
					prevLogIndex: node.nextIndex[port] - 1,
					prevLogTerm:  node.logs[node.nextIndex[port]-1].term,
					commitIndex:  node.commitIndex,
					entries:      node.logs[node.nextIndex[port]:],
				}

				ok, success, err := sendLog(conn, logMsg)
				fmt.Println("[LOG] Sending append log entries")

				if ok && success {
					atomic.AddInt32(&successCount, 1)

					node.mu.Lock()
					node.matchIndex[port] = logMsg.prevLogIndex + int32(len(logMsg.entries))
					node.nextIndex[port] = node.matchIndex[port] + 1
					node.mu.Unlock()

				} else if ok && !success {
					node.mu.Lock()
					node.nextIndex[port]--
					node.mu.Unlock()

					if node.nextIndex[port] < 1 {
						node.nextIndex[port] = 1
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

func (node *Node) AppendLog(leaderTerm, prevLogIdx, prevLogTerm, leaderCommit int32, entries []Entry) bool {
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
		writeToDisk(entry, node.port)
		fmt.Println("[LOG] Appending the log entry:", string(entry.payload))
	}

	if leaderCommit > node.commitIndex {
		node.commitIndex = min(leaderCommit, int32(len(node.logs)-1))
	}

	return true
}

func sendLog(conn net.Conn, logMsg appendLogMsg) (bool, bool, error) {
	// [0x0007][2B from][4B term][4B prev log idx][4B prev log term][4B lead's commit index][2B num of entries] + loop([8B timestamp][4B entry len][entry])
	frameSize := 2 + 2 + 4 + 4 + 4 + 4 + 2
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

	entriesCount := len(logMsg.entries)
	binary.BigEndian.PutUint16(frame[off:], uint16(entriesCount))
	off += 2

	fmt.Println("[DEBUG] sendLog frame:", frame)

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

func writeToDisk(entry Entry, port int16) {
	// on disk payload: [8B timestamp][4B payload len][payload]

	totalSize := 8 + 4 + len(entry.payload)
	buf := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint64(buf[off:], uint64(entry.timestamp))
	off += 8

	binary.BigEndian.PutUint32(buf[off:], uint32(len(entry.payload)))
	off += 4

	copy(buf[off:], []byte(entry.payload))

	fmt.Println("[DEBUG] MkdirAll reached")

	filePath := fmt.Sprintf("logs/%d/1.log", port)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("log failed log fialed log failed: %s", err))
	}
	file.Write(buf)

	defer file.Close()
}
