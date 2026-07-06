package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

type Message struct {
	from    int16
	to      int16
	message string
}

func (node *Node) Heartbeat() {
	ports := []int16{8080, 8081, 8082}

	for _, port := range ports {
		if port == node.port {
			continue
		} else {
			//[2B command (heartbeat (0x0001))][2B from][10B heartbeat! msg]
			conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 5*time.Second)
			if err != nil {
				fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
				continue
			}
			sendHeartbeat(node.port, conn)
			defer conn.Close()
		}
	}
}

func sendHeartbeat(senderPort int16, conn net.Conn) {
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
	if err != nil {
		panic(err)
	}
}
