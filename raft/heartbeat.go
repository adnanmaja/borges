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
			sendHeartbeat(node.port, port)
		}
	}
}

func sendHeartbeat(senderPort, destPort int16) {
	//[2B command (heartbeat (0x0001))][2B from][10B heartbeat! msg]
	address := fmt.Sprintf(":%d", destPort)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		panic(fmt.Sprintf("error starting messenger: %s", err))
	}
	defer conn.Close()

	payload := "heartbeat!"

	totalSize := 2 + 2 + 10
	messageFrame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(messageFrame[off:], 0x0001)
	off += 2

	binary.BigEndian.PutUint16(messageFrame[off:], uint16(senderPort))
	off += 2

	copy(messageFrame[off:], []byte(payload))

	_, err = conn.Write(messageFrame)
	if err != nil {
		panic(err)
	}
}
