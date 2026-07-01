package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func StartServer(broker *Broker) {
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
		go handleClient(conn, broker)
	}
}

func handleClient(conn net.Conn, b *Broker) {
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

		switch commandBuf[0] {
		case 0x01: //produce
			topicLenBuf := make([]byte, 2)
			io.ReadFull(conn, topicLenBuf)
			topicLength := binary.BigEndian.Uint16(topicLenBuf)
			topicBuf := make([]byte, topicLength)
			io.ReadFull(conn, topicBuf)
			topic := string(topicBuf)

			payloadLenBuf := make([]byte, 4)
			io.ReadFull(conn, payloadLenBuf)
			payloadLength := binary.BigEndian.Uint32(payloadLenBuf)

			payload := make([]byte, payloadLength)
			io.ReadFull(conn, payload)

			fmt.Println("[DEBUG] Topic: ", topic)

			log := b.GetOrCreateLog(topic)
			_, err := log.Write(payload)

			if err != nil {
				conn.Write([]byte{0x01}) //success
			} else {
				conn.Write([]byte{0x00}) // error
			}

		case 0x02: //consume
			topicLenBuf := make([]byte, 2)
			io.ReadFull(conn, topicLenBuf)
			topicLength := binary.BigEndian.Uint16(topicLenBuf)
			topicBuf := make([]byte, topicLength)
			io.ReadFull(conn, topicBuf)
			topic := string(topicBuf)

			offsetBuf := make([]byte, 8)
			io.ReadFull(conn, offsetBuf)
			offset := binary.BigEndian.Uint64(offsetBuf)

			log := b.GetOrCreateLog(topic)
			record, err := log.Read(int64(offset))
			if err != nil {
				conn.Write([]byte{0x01}) //error
			}

			conn.Write([]byte{0x01})

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
