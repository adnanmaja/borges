package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

func produce(conn net.Conn, topicStr string) {
	payloadStr := "Anytime Anywhere"
	fmt.Println("Producing, payload:", payloadStr)
	payload := []byte(payloadStr)
	payloadLen := uint32(len(payload))

	topic := []byte(topicStr)
	topicLen := uint16(len(topic))

	// [1 byte command][2 byte topic length][topic][4 bytes length][N bytes payload]
	totalSize := 1 + 2 + topicLen + 4 + uint16(payloadLen)

	frame := make([]byte, totalSize)
	off := 0

	frame[off] = 0x01
	off += 1

	binary.BigEndian.PutUint16(frame[off:], topicLen)
	off += 2

	copy(frame[off:], topic)
	off += len(topic)

	binary.BigEndian.PutUint32(frame[off:], payloadLen)
	off += 4

	copy(frame[off:], payload)

	_, err := conn.Write(frame)
	if err != nil {
		panic(err)
	}
}

func consume(conn net.Conn, topicStr string) {
	targetOffset := 0
	fmt.Printf("Consuming, targetOffset: %d\n", targetOffset)

	topic := []byte(topicStr)

	// [1 byte command][2 byte topic length][topic][8 byte target offset]
	totalSize := 1 + 2 + len(topic) + 8

	frame := make([]byte, totalSize)
	off := 0

	frame[off] = 0x02
	off += 1

	binary.BigEndian.PutUint16(frame[off:], uint16(len(topic)))
	off += 2

	copy(frame[off:], topic)
	off += len(topic)

	binary.BigEndian.PutUint64(frame[off:], uint64(targetOffset))

	_, err := conn.Write(frame)
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	topic := "song"

	produce(conn, topic)
	consume(conn, topic)

	fmt.Println("Data sent! Press Enter to close client...")
	fmt.Scanln()
}
