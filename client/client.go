package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

func produce(conn net.Conn) {
	payload := []byte("mini-kafka-test")
	payloadLen := uint32(len(payload))

	// [1 byte command][4 bytes length][N bytes payload]
	frame := make([]byte, 1+4+len(payload))
	frame[0] = 0x01
	binary.BigEndian.PutUint32(frame[1:5], payloadLen)
	copy(frame[5:], payload)

	_, err := conn.Write(frame)
	if err != nil {
		panic(err)
	}
}

func consume(conn net.Conn) {
	targetOffset := 80

	frame := make([]byte, 1+8)
	frame[0] = 0x02
	binary.BigEndian.PutUint64(frame[1:9], uint64(targetOffset))

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

	produce(conn)
	consume(conn)

	fmt.Println("Data sent! Press Enter to close client...")
	fmt.Scanln()
}
