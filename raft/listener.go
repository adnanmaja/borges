package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

func (node *Node) startListener() {
	address := fmt.Sprintf(":%d", node.port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(fmt.Sprintf("error starting server: %s", err))
	}
	fmt.Printf("Listening at :%d\n", node.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				fmt.Println("listener closed.")
				break
			}

			fmt.Println("error accepting connection: ", err)
			continue
		}

		go node.listenForMessage(conn)
	}
}

func (node *Node) listenForMessage(conn net.Conn) {
	defer conn.Close()

	for {
		commandBuf := make([]byte, 2)
		_, err := io.ReadFull(conn, commandBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				fmt.Println("Client disconnected normally.")
			} else {
				fmt.Println("\nClient disconnected abruptly:", err)
			}
			break
		}
		command := binary.BigEndian.Uint16(commandBuf)
		fmt.Println("command:", command)

		switch command {
		case 0x0001: // heartbeat
			//[2B from][10B payload]
			from, err := parseSender(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			message, err := parseHeartbeat(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			fmt.Printf("[HEARTBEAT] message from %d: %s\n", from, string(message))
			node.resetElectionTimer()

		case 0x0002: // vote request
			sender, err := parseSender(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			ok := node.Vote(sender)
			if ok {
				response := make([]byte, 2)
				binary.BigEndian.PutUint16(response, 0x0003) // "you got my vote"
				conn.Write(response)
			} else {
				response := make([]byte, 2)
				binary.BigEndian.PutUint16(response, 0x0004) // "who do you think you are"
				conn.Write(response)
			}
		}
	}
}

func parseSender(conn net.Conn) (int16, error) {
	buf := make([]byte, 2)
	if err := readFull(conn, buf); err != nil {
		return 0, err
	}

	message := binary.BigEndian.Uint16(buf)
	return int16(message), nil
}

func parseHeartbeat(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 10)
	if err := readFull(conn, buf); err != nil {
		return []byte{}, err
	}

	return buf, nil
}

func readFull(conn net.Conn, buf []byte) error {
	_, err := io.ReadFull(conn, buf)
	return err
}
