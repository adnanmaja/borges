package sdk

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func (client *Client) CreateTopic(topic string, numPartitions int) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.CurrentLeader), client.Timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	frame := make([]byte, 2+4+len(topic)+4)
	off := 0
	binary.BigEndian.PutUint16(frame[off:], 0x0012)
	off += 2
	binary.BigEndian.PutUint32(frame[off:], uint32(len(topic)))
	off += 4
	copy(frame[off:], topic)
	off += len(topic)
	binary.BigEndian.PutUint32(frame[off:], uint32(numPartitions))

	if _, err := conn.Write(frame); err != nil {
		return err
	}

	res := make([]byte, 2)
	if _, err := io.ReadFull(conn, res); err != nil {
		return err
	}

	if binary.BigEndian.Uint16(res) != 0x0001 {
		return fmt.Errorf("create topic failed\n")
	}
	return nil
}
