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

		switch commandBuf[0] {
		case 0x01: //produce
			topic := parseTopic(conn)

			payloadLenBuf := make([]byte, 4)
			io.ReadFull(conn, payloadLenBuf)
			payloadLength := binary.BigEndian.Uint32(payloadLenBuf)

			payload := make([]byte, payloadLength)
			io.ReadFull(conn, payload)

			fmt.Println("[DEBUG] Topic: ", topic)

			log := b.GetOrCreateLog(topic)
			_, err := log.Write(payload)

			if err != nil {
				conn.Write([]byte{0x00}) //success
			} else {
				conn.Write([]byte{0x01}) // error
			}

		case 0x02: //consume
			topic := parseTopic(conn)
			offset := parseOffset(conn)

			log := b.GetOrCreateLog(topic)
			record, err := log.Read(offset)
			if err != nil {
				conn.Write([]byte{0x01}) //error
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

		case 0x03: // fetch offset
			groupId := parseGroupId(conn)
			topic := parseTopic(conn)

			offset := b.FetchOffset(groupId, topic)

			offsetBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(offsetBuf, uint64(offset))

			responseBuf := make([]byte, 9)

			responseBuf[0] = 0x00 //success
			copy(responseBuf[1:9], offsetBuf)

			conn.Write(responseBuf)

		case 0x04: // commmit offset
			groupId := parseGroupId(conn)
			topic := parseTopic(conn)
			offset := parseOffset(conn)

			b.SaveOffset(groupId, topic, offset)
			conn.Write([]byte{0x00}) // success
		}
	}
}

// ----------------------------------------------------- helpers

func parseGroupId(conn net.Conn) string {
	groupIdLenBuf := make([]byte, 2)
	io.ReadFull(conn, groupIdLenBuf)
	groupIdLen := binary.BigEndian.Uint16(groupIdLenBuf)
	groupIdBuf := make([]byte, groupIdLen)
	io.ReadFull(conn, groupIdBuf)
	groupId := string(groupIdBuf)

	return groupId
}

func parseTopic(conn net.Conn) string {
	topicLenBuf := make([]byte, 2)
	io.ReadFull(conn, topicLenBuf)
	topicLength := binary.BigEndian.Uint16(topicLenBuf)
	topicBuf := make([]byte, topicLength)
	io.ReadFull(conn, topicBuf)
	topic := string(topicBuf)

	return topic
}

func parseOffset(conn net.Conn) int64 {
	offsetBuf := make([]byte, 8)
	io.ReadFull(conn, offsetBuf)
	offset := binary.BigEndian.Uint64(offsetBuf)

	return int64(offset)
}
