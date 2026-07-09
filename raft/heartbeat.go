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
		err := node.pool.Send(port, 5*time.Second, func(conn net.Conn) error {
			return sendHeartbeat(node.port, conn)
		})
		if err != nil {
			fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
		}
	}
}

func sendHeartbeat(senderPort int16, conn net.Conn) error {
	payload := "heartbeat!"

	totalSize := 2 + 2 + 10
	messageFrame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(messageFrame[off:], 0x0004)
	off += 2

	binary.BigEndian.PutUint16(messageFrame[off:], uint16(senderPort))
	off += 2

	copy(messageFrame[off:], []byte(payload))

	_, err := conn.Write(messageFrame)
	return err
}
