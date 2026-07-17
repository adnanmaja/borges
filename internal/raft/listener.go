package raft

import (
	"bufio"
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
	node.listener = listener
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
	reader := bufio.NewReaderSize(conn, 8192)

	var opcodeBuf [2]byte
	var int16Buf [2]byte
	var int32Buf [4]byte
	var int64Buf [8]byte
	var maxEntryCount = 10000

	for {
		_, err := io.ReadFull(reader, opcodeBuf[0:2])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
			} else {
				fmt.Println("\nClient disconnected abruptly:", err)
			}
			break
		}
		opcode := binary.BigEndian.Uint16(opcodeBuf[0:2])
		// fmt.Println("opcode:", opcode)

		switch opcode {
		case 0x0004: // leader's heartbeat
			from, err := readInt16(reader, int16Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			term, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			fmt.Printf("[HEARTBEAT] from %d (term %d)\n", from, term)
			node.currentLeader = from
			node.resetElectionTimer(term)

		case 0x0005: // candidate's vote request
			sender, err := readInt16(reader, int16Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			term, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			lastLogIndex, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			lastLogTerm, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			ok := node.Vote(sender, term, lastLogIndex, lastLogTerm)
			if ok {
				responseSuccess(conn)
			} else {
				responseFail(conn)
			}
		case 0x0006: //produce
			_, err := readInt16(reader, int16Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			topic, err := parseTopic(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			partitionId, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entriesCount, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			if entriesCount > int32(maxEntryCount) {
				responseFail(conn)
				fmt.Println("[DEBUG] too mcuh entries")
				break
			}

			log := node.broker.GetOrCreatePartition(topic, partitionId, node.port).log

			var payloads [][]byte
			for range entriesCount {
				payloadLen, err := readInt32(reader, int32Buf[:])
				if err != nil {
					fmt.Println("error:", err)
					break
				}

				payload := make([]byte, payloadLen)
				if err := readFull(reader, payload); err != nil {
					fmt.Println("error:", err)
					break
				}
				payloads = append(payloads, payload)
			}

			log.Write(payloads, node, conn)

		case 0x0007: // leader's apeendLog request
			_, err := readInt16(reader, int16Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			leaderTerm, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			prevLogIdx, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			prevLogTerm, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			leaderCommit, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			topic, err := parseTopic(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			partitionId, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			entryNum, err := readInt16(reader, int16Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			var entries []Entry
			for range entryNum {
				timestamp, err := readInt64(reader, int64Buf[:])
				if err != nil {
					fmt.Println("error:", err)
					break
				}

				payloadLen, err := readInt32(reader, int32Buf[:])
				if err != nil {
					fmt.Println("error:", err)
					break
				}

				payload := make([]byte, payloadLen)
				if err := readFull(reader, payload); err != nil {
					fmt.Println("error:", err)
					break
				}

				entries = append(entries, Entry{
					timestamp: timestamp,
					payload:   payload,
					term:      int32(leaderTerm),
				})
			}

			log := node.broker.GetOrCreatePartition(topic, partitionId, node.port).log
			ok := log.Append(leaderTerm, prevLogIdx, prevLogTerm, leaderCommit, entries, node)
			if ok {
				responseSuccess(conn)
			} else {
				responseFail(conn)
			}

		case 0x0008: // consume
			_, err := readInt16(reader, int16Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			topic, err := parseTopic(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			partitionId, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			offsetTarget, err := readInt64(reader, int64Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			sizeLimit, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			log := node.broker.GetOrCreatePartition(topic, partitionId, node.port).log
			entries, err := log.Read(int64(offsetTarget), node, sizeLimit)
			if err != nil {
				fmt.Println("error:", err)
				responseFail(conn)
				break
			}

			bw := bufio.NewWriter(conn)
			headerFrame := make([]byte, 6)
			binary.BigEndian.PutUint16(headerFrame[0:2], 0x0001)
			binary.BigEndian.PutUint32(headerFrame[2:6], uint32(len(entries)))
			bw.Write(headerFrame)

			for _, entry := range entries {
				headerBuf := make([]byte, 12)
				binary.BigEndian.PutUint64(headerBuf[0:8], uint64(entry.timestamp))
				binary.BigEndian.PutUint32(headerBuf[8:12], uint32(len(entry.payload)))
				bw.Write(headerBuf)
				bw.Write(entry.payload)
			}
			bw.Flush()

		case 0x0009: // commit offset
			groupIdLen, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupIdBuf := make([]byte, groupIdLen)
			err = readFull(reader, groupIdBuf)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupId := string(groupIdBuf)

			topic, err := parseTopic(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			offsetCommit, err := readInt64(reader, int64Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			node.broker.SaveOffset(groupId, topic, offsetCommit)

			responseSuccess(conn)

		case 0x0010: // fetch offset
			groupIdLen, err := readInt32(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupIdBuf := make([]byte, groupIdLen)
			err = readFull(reader, groupIdBuf)
			if err != nil {
				fmt.Println("error:", err)
				break
			}
			groupId := string(groupIdBuf)

			topic, err := parseTopic(reader, int32Buf[:])
			if err != nil {
				fmt.Println("error:", err)
				break
			}

			offset := node.broker.FetchOffset(groupId, topic)
			fmt.Println("[DEBUG] abi nuppp ")

			resFrame := make([]byte, 10)
			binary.BigEndian.PutUint16(resFrame[0:2], 0x0001)
			binary.BigEndian.PutUint64(resFrame[2:10], uint64(offset))
			_, err = conn.Write(resFrame)
			if err != nil {
				responseFail(conn)
				fmt.Println("error:", err)
			}

		case 0x0011: // Client's search for the leader
			// req: just the opcode, res: [0x0001 success][2B leader's port]
			res := make([]byte, 2)
			binary.BigEndian.PutUint16(res, uint16(node.currentLeader))
			responseSuccess(conn)
			conn.Write(res)
		}

	}
}

func readInt16(r io.Reader, buf []byte) (int16, error) {
	if err := readFull(r, buf); err != nil {
		return 0, err
	}

	message := binary.BigEndian.Uint16(buf)
	return int16(message), nil
}

func readInt32(r io.Reader, buf []byte) (int32, error) {
	if err := readFull(r, buf); err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(buf)), nil
}

func readInt64(r io.Reader, buf []byte) (int64, error) {
	if err := readFull(r, buf); err != nil {
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(buf)), nil
}

func readFull(r io.Reader, buf []byte) error {
	_, err := io.ReadFull(r, buf)
	return err
}

func responseSuccess(conn net.Conn) {
	response := make([]byte, 2)
	binary.BigEndian.PutUint16(response, 0x0001)
	conn.Write(response)
}

func responseFail(conn net.Conn) {
	var response [2]byte
	binary.BigEndian.PutUint16(response[0:2], 0x0002)
	conn.Write(response[0:2])
}

func parseTopic(r io.Reader, buf []byte) (string, error) {
	topicLen, err := readInt32(r, buf)
	if err != nil {
		return "", err
	}

	topicBuf := make([]byte, topicLen)
	err = readFull(r, topicBuf)
	if err != nil {
		return "", err
	}
	return string(topicBuf), nil
}
