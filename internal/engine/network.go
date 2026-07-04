package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	MaxPayloadSize   = 64 * 1024 // 64kb
	MaxStringLen     = 65535     // max uint16 for topic/group names
	MaxBatchMsgCount = 1001
)

func StartServer(broker *Broker) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(fmt.Sprintf("error starting server: %s", err))
	}
	fmt.Println("Listeing at :8080")

	var wg sync.WaitGroup
	var mu sync.Mutex
	activeConn := make(map[net.Conn]struct{})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan

		listener.Close()

		mu.Lock()
		fmt.Printf("shutting down %d conncection ...\n", len(activeConn))
		for conn := range activeConn {
			conn.SetReadDeadline(time.Now())
		}
		mu.Unlock()
	}()

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

		mu.Lock()
		activeConn[conn] = struct{}{}
		mu.Unlock()

		wg.Add(1)

		go func(c net.Conn) {
			defer wg.Done()
			defer func() {
				mu.Lock()
				delete(activeConn, c)
				mu.Unlock()
			}()

			handleClient(c, broker)
		}(conn)
	}

	wg.Wait()
	broker.Close()
	fmt.Println("dead")
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

		var connErr error

		switch commandBuf[0] {
		case 0x01: //produce
			topic, err := parseTopic(conn)
			if err != nil {
				connErr = err
				break
			}

			msgCountBuf := make([]byte, 4)
			err = readFull(conn, msgCountBuf)
			if err != nil {
				connErr = err
				break
			}
			messageCount := binary.BigEndian.Uint32(msgCountBuf)

			if messageCount > MaxBatchMsgCount {
				_ = writeAll(conn, []byte{0x01})
				connErr = errors.New("batch message count exceeds limit")
				break
			}

			log := b.GetOrCreateLog(topic)

			payloadLenBuf := make([]byte, 4)
			for range messageCount {

				if err = readFull(conn, payloadLenBuf); err != nil {
					connErr = err
					break
				}
				payloadLength := binary.BigEndian.Uint32(payloadLenBuf)

				if payloadLength > MaxPayloadSize {
					err = errors.New("payload too large")
					break
				}

				payload := make([]byte, payloadLength)
				if err = readFull(conn, payload); err != nil {
					connErr = err
					break
				}

				_, err = log.Write(payload)
				if err != nil {
					break
				}
			}

			if err != nil {
				connErr = writeAll(conn, []byte{0x01})
			} else {
				connErr = writeAll(conn, []byte{0x00})
			}

		case 0x02: //consume
			topic, err := parseTopic(conn)
			if err != nil {
				connErr = err
				break
			}

			offset, err := parseOffset(conn)
			if err != nil {
				connErr = err
				break
			}

			log := b.GetOrCreateLog(topic)
			record, err := log.Read(offset)
			if err != nil {
				connErr = writeAll(conn, []byte{0x01})
				break
			}

			connErr = writeAll(conn, []byte{0x00})
			if connErr != nil {
				break
			}

			timeBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(timeBuf, uint64(record.Timestamp))
			connErr = writeAll(conn, timeBuf)
			if connErr != nil {
				break
			}

			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(record.Payload)))
			connErr = writeAll(conn, lenBuf)
			if connErr != nil {
				break
			}

			connErr = writeAll(conn, record.Payload)

		case 0x03: // fetch offset
			groupId, err := parseGroupId(conn)
			if err != nil {
				connErr = err
				break
			}

			topic, err := parseTopic(conn)
			if err != nil {
				connErr = err
				break
			}

			offset := b.FetchOffset(groupId, topic)

			offsetBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(offsetBuf, uint64(offset))

			responseBuf := make([]byte, 9)
			responseBuf[0] = 0x00
			copy(responseBuf[1:9], offsetBuf)

			connErr = writeAll(conn, responseBuf)

		case 0x04: // commit offset
			groupId, err := parseGroupId(conn)
			if err != nil {
				connErr = err
				break
			}

			topic, err := parseTopic(conn)
			if err != nil {
				connErr = err
				break
			}

			offset, err := parseOffset(conn)
			if err != nil {
				connErr = err
				break
			}

			b.SaveOffset(groupId, topic, offset)
			connErr = writeAll(conn, []byte{0x00})
		}

		if connErr != nil {
			fmt.Println("Connection error:", connErr)
			break
		}
	}
}

// ----------------------------------------------------- helpers

func readFull(conn net.Conn, buf []byte) error {
	_, err := io.ReadFull(conn, buf)
	return err
}

func writeAll(conn net.Conn, buf []byte) error {
	_, err := conn.Write(buf)
	return err
}

func parseGroupId(conn net.Conn) (string, error) {
	groupIdLenBuf := make([]byte, 2)
	if err := readFull(conn, groupIdLenBuf); err != nil {
		return "", err
	}
	groupIdLen := binary.BigEndian.Uint16(groupIdLenBuf)

	if groupIdLen > MaxStringLen {
		return "", nil
	}

	groupIdBuf := make([]byte, groupIdLen)
	if err := readFull(conn, groupIdBuf); err != nil {
		return "", err
	}

	return string(groupIdBuf), nil
}

func parseTopic(conn net.Conn) (string, error) {
	topicLenBuf := make([]byte, 2)
	if err := readFull(conn, topicLenBuf); err != nil {
		return "", err
	}
	topicLength := binary.BigEndian.Uint16(topicLenBuf)

	if topicLength > MaxStringLen {
		return "", nil
	}

	topicBuf := make([]byte, topicLength)
	if err := readFull(conn, topicBuf); err != nil {
		return "", err
	}

	return string(topicBuf), nil
}

func parseOffset(conn net.Conn) (int64, error) {
	offsetBuf := make([]byte, 8)
	if err := readFull(conn, offsetBuf); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(offsetBuf)), nil
}
