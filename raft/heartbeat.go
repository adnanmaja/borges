package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func (node *Node) Heartbeat() {
	for _, port := range ports {
		if port == node.port {
			continue
		}
		conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 5*time.Second)
		if err != nil {
			fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
			continue
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		err = sendHeartbeat(node.port, node.currentTerm, conn)
		conn.Close()
		if err != nil {
			fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
		}
	}
}

func sendHeartbeat(senderPort int16, term int32, conn net.Conn) error {
	totalSize := 2 + 2 + 4
	messageFrame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(messageFrame[off:], 0x0004)
	off += 2

	binary.BigEndian.PutUint16(messageFrame[off:], uint16(senderPort))
	off += 2

	binary.BigEndian.PutUint32(messageFrame[off:], uint32(term))

	_, err := conn.Write(messageFrame)
	return err
}
