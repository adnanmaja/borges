package raft

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func (node *Node) Heartbeat() {
	for _, port := range node.peers {
		if port == node.port {
			continue
		}

		var err error
		for attempt := 0; attempt < 2; attempt++ {
			if attempt > 0 {
				node.connPool.Evict(port)
				time.Sleep(100 * time.Millisecond)
			}

			var conn net.Conn
			conn, err = node.connPool.GetOrCreateConnection(port)
			if err != nil {
				continue
			}
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			err = sendHeartbeat(node.port, node.currentTerm, conn)
			if err != nil {
				continue
			}
			break
		}
		if err != nil {
			fmt.Printf("[HEARTBEAT] cannot reach %d after retry: %s\n", port, err)
			node.connPool.Evict(port)
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
