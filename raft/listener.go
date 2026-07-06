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
		opcodeBuf := make([]byte, 2)
		_, err := io.ReadFull(conn, opcodeBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// fmt.Println("Client disconnected normally.")
			} else {
				fmt.Println("\nClient disconnected abruptly:", err)
			}
			break
		}
		opcode := binary.BigEndian.Uint16(opcodeBuf)
		fmt.Println("opcode:", opcode)

		switch opcode {
		case 0x0004: // heartbeat
			//[2B from][10B payload]
			from, err := parse2Bytes(conn)
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

		case 0x0005: // vote request
			sender, err := parse2Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			ok := node.Vote(sender)
			if ok {
				responseSuccess(conn) // "you got my vote"
			} else {
				responseFail(conn) // "who do you think you are"
			}
		case 0x0006: // log entry, [2B from (irrelevant)][4B entry length][entry]
			_, err := parse2Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entryLen, err := parse4Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entry := make([]byte, entryLen)
			if err := readFull(conn, entry); err != nil {
				fmt.Println("error:", err)
				break
			}

			ok := node.WriteLog(entry)
			if ok {
				responseSuccess(conn)
			} else {
				responseFail(conn)
			}

		case 0x0007: // leader's request to append entries.
			// [2B leader's port (irrelevant for now)][4B lead's term][4B prevLogIdx][4B prevLogTerm][4b leadCommitIndex][2B entryNum] + loop([4B entryLen][entry])
			_, err := parse2Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			leaderTerm, err := parse4Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			prevLogIdx, err := parse4Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			prevLogTerm, err := parse4Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			leaderCommit, err := parse4Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entryNum, err := parse2Bytes(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			fmt.Println("[DEBUG] entryNum:", entryNum)

			var entries []Entry
			for range entryNum {
				payloadLen, err := parse4Bytes(conn)
				if err != nil {
					fmt.Println("error:", err)
					break
				}

				payload := make([]byte, payloadLen)
				if err := readFull(conn, payload); err != nil {
					fmt.Println("error:", err)
					break
				}

				entries = append(entries, Entry{
					payload: string(payload),
					term:    int32(leaderTerm),
				})
			}

			ok := node.AppendLog(int32(leaderTerm), int32(prevLogIdx), int32(prevLogTerm), int32(leaderCommit), entries)
			if ok {
				responseSuccess(conn)
			} else {
				responseFail(conn)
			}
		}
	}
}

func parse2Bytes(conn net.Conn) (int16, error) {
	buf := make([]byte, 2)
	if err := readFull(conn, buf); err != nil {
		return 0, err
	}

	message := binary.BigEndian.Uint16(buf)
	return int16(message), nil
}

func parse4Bytes(conn net.Conn) (int16, error) {
	buf := make([]byte, 4)
	if err := readFull(conn, buf); err != nil {
		return 0, err
	}

	message := binary.BigEndian.Uint32(buf)
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

func responseSuccess(conn net.Conn) {
	response := make([]byte, 2)
	binary.BigEndian.PutUint16(response, 0x0001)
	conn.Write(response)
}

func responseFail(conn net.Conn) {
	response := make([]byte, 2)
	binary.BigEndian.PutUint16(response, 0x0002)
	conn.Write(response)
}
