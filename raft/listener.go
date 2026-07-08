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
			from, err := readInt16(conn)
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
			sender, err := readInt16(conn)
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
		case 0x0006: // log entry from client
			// incoming: [2B from (irrelevant)][4B topic len][topic][4B payload length][payload]
			_, err := readInt16(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			topic, err := parseTopic(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entryLen, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entry := make([]byte, entryLen)
			if err := readFull(conn, entry); err != nil {
				fmt.Println("error:", err)
				break
			}

			log := node.broker.GetOrCreateLog(topic, node.port)

			ok := log.Write(entry, node)
			if ok {
				responseSuccess(conn)
			} else {
				responseFail(conn)
			}

		case 0x0007: // leader's request to append entries.
			// [2B leader's port (irrelevant for now)][4B lead's term][4B prevLogIdx][4B prevLogTerm][4b leadCommitIndex][4B topic len][topic][2B entryNum] + loop([8B timestamp][4B entryLen][entry])
			_, err := readInt16(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			leaderTerm, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			prevLogIdx, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			prevLogTerm, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			leaderCommit, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			topic, err := parseTopic(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entryNum, err := readInt16(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			var entries []Entry
			for range entryNum {
				timestamp, err := readInt64(conn)
				if err != nil {
					fmt.Println("error:", err)
					break
				}

				payloadLen, err := readInt32(conn)
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
					timestamp: timestamp,
					payload:   string(payload),
					term:      int32(leaderTerm),
				})
			}

			log := node.broker.GetOrCreateLog(topic, node.port)
			ok := log.Append(leaderTerm, prevLogIdx, prevLogTerm, leaderCommit, entries, node)
			if ok {
				responseSuccess(conn)
			} else {
				responseFail(conn)
			}

		case 0x0008: // client's consume request
			// req: [2B from (irrelevant)][4B len topic][topic][8B target offset start][4B size limit]
			// res: [2B fail/success][4B entry count] + loop([8B timestamp][4B payload length][payload])
			_, err := readInt16(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			topic, err := parseTopic(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			offsetTarget, err := readInt64(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			sizeLimit, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			log := node.broker.GetOrCreateLog(topic, node.port)
			entries, err := log.Read(int64(offsetTarget), node, sizeLimit)
			if err != nil {
				fmt.Println("error:", err)
				responseFail(conn)
				break
			}

			headerFrame := make([]byte, 6)
			binary.BigEndian.PutUint16(headerFrame[0:2], 0x0001) // success
			binary.BigEndian.PutUint32(headerFrame[2:6], uint32(len(entries)))
			conn.Write(headerFrame)

			for _, entry := range entries {
				headerBuf := make([]byte, 12)
				binary.BigEndian.PutUint64(headerBuf[0:8], uint64(entry.timestamp))
				binary.BigEndian.PutUint32(headerBuf[8:12], uint32(len(entry.payload)))
				conn.Write(headerBuf)
				conn.Write([]byte(entry.payload))
			}

		case 0x0009: // client's commit offset
			// incoming: [4B group id len][group id][4B topic len][topic][8B offset commit]
			// response: [2B response code]
			groupIdLen, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupIdBuf := make([]byte, groupIdLen)
			err = readFull(conn, groupIdBuf)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupId := string(groupIdBuf)

			topic, err := parseTopic(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			offsetCommit, err := readInt64(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			node.broker.SaveOffset(groupId, topic, offsetCommit)

			responseSuccess(conn)

		case 0x0010: // fetch offset
			// incoming: [4B group len][group id][4B topic len][topic]
			// response: [2B response code][8B offset]
			groupIdLen, err := readInt32(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupIdBuf := make([]byte, groupIdLen)
			err = readFull(conn, groupIdBuf)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupId := string(groupIdBuf)

			topic, err := parseTopic(conn)
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			offset := node.broker.FetchOffset(groupId, topic)

			resFrame := make([]byte, 10)
			binary.BigEndian.PutUint16(resFrame[0:2], 0x0001)
			binary.BigEndian.PutUint64(resFrame[2:10], uint64(offset))
			_, err = conn.Write(resFrame)
			if err != nil {
				responseFail(conn)
				fmt.Println("error:", err)
			}
		}

	}
}

func readInt16(conn net.Conn) (int16, error) {
	buf := make([]byte, 2)
	if err := readFull(conn, buf); err != nil {
		return 0, err
	}

	message := binary.BigEndian.Uint16(buf)
	return int16(message), nil
}

func readInt32(conn net.Conn) (int32, error) {
	buf := make([]byte, 4)
	if err := readFull(conn, buf); err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(buf)), nil
}

func readInt64(conn net.Conn) (int64, error) {
	buf := make([]byte, 8)
	if err := readFull(conn, buf); err != nil {
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(buf)), nil
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

func parseTopic(conn net.Conn) (string, error) {
	topicLen, err := readInt32(conn)
	if err != nil {
		return "", err
	}

	topicBuf := make([]byte, topicLen)
	err = readFull(conn, topicBuf)
	if err != nil {
		return "", err
	}
	return string(topicBuf), nil
}
