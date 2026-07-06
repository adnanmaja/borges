package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func (node *Node) WriteLog(entry []byte) bool {
	node.logs = append(node.logs, Entry{
		command: string(entry),
		term:    node.currentTerm,
	})

	fmt.Println("[LOG] Appending the log entry:", string(entry))

	if node.role == "Leader" {
		ports := []int16{8080, 8081, 8082}
		for _, port := range ports {
			if port == node.port {
				continue
			} else {
				// [0x0002][2B from]
				conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 5*time.Second)
				if err != nil {
					fmt.Printf("[LOG] cannot reach %d: %s\n", port, err)
					continue
				}
				ok, err := sendLog(conn, node.port, node.currentTerm, node.logs[len(node.logs)-1].command)
				fmt.Println("[LOG] Sending append log entries")
				if ok {
					conn.Close()
				}
			}
		}
	}

	return true
}

func (node *Node) AppendLog(term int32, entry []byte) bool {
	node.logs = append(node.logs, Entry{
		command: string(entry),
		term:    term,
	})

	fmt.Println("[LOG] Appending the log entry:", string(entry))

	if node.role == "Leader" {
		ports := []int16{8080, 8081, 8082}
		for _, port := range ports {
			if port == node.port {
				continue
			} else {
				// [0x0002][2B from]
				conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 5*time.Second)
				if err != nil {
					fmt.Printf("[LOG] cannot reach %d: %s\n", port, err)
					continue
				}
				ok, err := sendLog(conn, node.port, node.currentTerm, node.logs[len(node.logs)-1].command)
				fmt.Println("[LOG] Sending append log entries")
				if ok {
					conn.Close()
				}
			}
		}
	}

	return true
}

func sendLog(conn net.Conn, senderPort int16, term int32, entry string) (bool, error) {
	// [0x0007][2B from][4B term][4B entry len][entry]
	totalSize := 2 + 2 + 4 + 4 + len(entry)
	frame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(frame[off:], 0x0007)
	off += 2

	binary.BigEndian.PutUint16(frame[off:], uint16(senderPort))
	off += 2

	binary.BigEndian.PutUint32(frame[off:], uint32(term))
	off += 4

	binary.BigEndian.PutUint32(frame[off:], uint32(len(entry)))
	off += 4

	copy(frame[off:], []byte(entry))

	_, err := conn.Write(frame)
	if err != nil {
		return false, err
	}

	responseFrame := make([]byte, 2)
	_, err = conn.Read(responseFrame)
	if err != nil {
		return false, err
	}

	responseCode := binary.BigEndian.Uint16(responseFrame)

	switch responseCode {
	case 0x0001:
		return true, nil
	case 0x0002:
		return false, nil
	}
	return false, nil
}
