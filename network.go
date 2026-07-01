package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func StartServer(log *Log) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(fmt.Sprintf("error starting server: %s", err))
	}
	fmt.Println("[DEBUG] Listeing at :8080")
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("error accepting connection: ", err)
			continue
		}
		go handleClient(conn, log)
	}
}

func handleClient(conn net.Conn, b *Log) {
	defer conn.Close()

	for {
		commandBuf := make([]byte, 1)
		_, err := io.ReadFull(conn, commandBuf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected normally.")
			} else {
				fmt.Println("\nClient disconnected abruptly:", err)
			}
			break
		}

		fmt.Println("[DEBUG] Command: ", commandBuf)

		if commandBuf[0] == 0x01 { // produce
			lengthBuf := make([]byte, 4)
			io.ReadFull(conn, lengthBuf)
			payloadLength := binary.BigEndian.Uint32(lengthBuf)

			payloadBuf := make([]byte, payloadLength)
			io.ReadFull(conn, payloadBuf)

			_, err := b.Write(payloadBuf)

			if err != nil {
				conn.Write([]byte{0x01}) //success
			} else {
				conn.Write([]byte{0x00}) // error
			}

		} else if commandBuf[0] == 0x02 {
			offsetBuf := make([]byte, 8)
			io.ReadFull(conn, offsetBuf)
			offset := binary.BigEndian.Uint64(offsetBuf)

			record, err := b.Read(int64(offset))
			if err != nil {
				conn.Write([]byte{0x00}) //error
			}

			conn.Write([]byte{0x00})

			timeBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(timeBuf, uint64(record.Timestamp))
			conn.Write(timeBuf)

			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(record.Payload)))
			conn.Write(lenBuf)

			conn.Write(record.Payload)
			fmt.Printf("\nTimestamp: %d, Payload: %s\n", record.Timestamp, record.Payload)
		}
	}
}
