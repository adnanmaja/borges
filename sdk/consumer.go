package sdk

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type ConsumerConfig struct {
	Topic            string
	GroupId          string
	MaxBatch         int
	PartitionId      *int
	EnableAutoCommit *bool
}

type Consumer struct {
	topic            string
	groupId          string
	maxBatch         int
	conn             net.Conn
	lastOffsets      map[int]int64
	partitionId      *int
	numPartitions    int
	currentPartition int
	ch               chan Entry
	lastErr          error
	autoCommit       bool
}

type Entry struct {
	Timestamp int64
	Payload   []byte
	Partition int
	Offset    int64
}

func (client *Client) NewConsumer(config ConsumerConfig) (*Consumer, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.CurrentLeader), client.Timeout)
	if err != nil {
		return nil, err
	}

	var autoCommit bool
	if config.EnableAutoCommit == nil {
		autoCommit = true
	} else {
		autoCommit = *config.EnableAutoCommit
	}

	consumer := &Consumer{
		topic:       config.Topic,
		groupId:     config.GroupId,
		maxBatch:    config.MaxBatch,
		conn:        conn,
		lastOffsets: make(map[int]int64),
		partitionId: config.PartitionId,
		autoCommit:  autoCommit,
	}

	if consumer.partitionId == nil {
		consumer.currentPartition = 0
		if err := consumer.getNumPartition(consumer.topic); err != nil {
			return nil, err
		}

		for i := range consumer.numPartitions {
			if err = consumer.fetchOffset(i); err != nil {
				return nil, err
			}
		}
	} else {
		consumer.currentPartition = *consumer.partitionId
	}

	return consumer, nil
}

func (c *Consumer) Start() (<-chan Entry, error) {
	c.ch = make(chan Entry, 100)

	go func() {
		defer close(c.ch)
		for i := range c.numPartitions {
			currentOffset := c.lastOffsets[i]
			entries, err := c.consume(i, currentOffset)
			if err != nil {
				fmt.Println("[CONSUME] error:", err)
				c.lastErr = err
				return
			}

			for _, entry := range entries {
				c.ch <- entry
			}

			if c.autoCommit {
				nextOffset := currentOffset + int64(len(entries))
				if err = c.commitOffset(i, nextOffset); err != nil {
					c.lastErr = err
					return
				}
			}
		}
	}()
	return c.ch, nil
}

func (c *Consumer) Err() error { return c.lastErr }

func (c *Consumer) Commit(e Entry) error {
	if err := c.commitOffset(e.Partition, e.Offset); err != nil {
		return err
	}
	return nil
}

// note: do a regular consume, then return them to the client one by one
func (c *Consumer) consume(partition int, offset int64) ([]Entry, error) {
	totalSize := 2 + 2 + 4 + len(c.topic) + 4 + 8 + 4
	reqFrame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(reqFrame[off:], 0x0008)
	off += 2
	binary.BigEndian.PutUint16(reqFrame[off:], 0)
	off += 2
	binary.BigEndian.PutUint32(reqFrame[off:], uint32(len(c.topic)))
	off += 4
	copy(reqFrame[off:], []byte(c.topic))
	off += len(c.topic)

	binary.BigEndian.PutUint32(reqFrame[off:], uint32(partition))
	off += 4
	binary.BigEndian.PutUint64(reqFrame[off:], uint64(offset))
	off += 8
	binary.BigEndian.PutUint32(reqFrame[off:], uint32(c.maxBatch))

	if _, err := c.conn.Write(reqFrame); err != nil {
		return nil, err
	}

	resHeader := make([]byte, 6)
	if _, err := io.ReadFull(c.conn, resHeader); err != nil {
		return nil, err
	}
	statusCode := binary.BigEndian.Uint16(resHeader[0:2])
	entryCount := binary.BigEndian.Uint32(resHeader[2:6])

	if statusCode == 0x0002 {
		return nil, fmt.Errorf("server replies with a fail\n")
	}

	var entries []Entry
	for range entryCount {
		resFrame := make([]byte, 12)
		if _, err := io.ReadFull(c.conn, resFrame); err != nil {
			return nil, err
		}
		timestamp := binary.BigEndian.Uint64(resFrame[0:8])
		payloadLen := binary.BigEndian.Uint32(resFrame[8:12])
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return nil, err
		}

		entries = append(entries, Entry{
			Timestamp: int64(timestamp),
			Payload:   payload,
			Partition: partition,
			Offset:    offset,
		})
		offset++
	}

	if statusCode == 0x0001 {
		return entries, nil
	}
	return nil, fmt.Errorf("consume gone wrong\n")
}

func (c *Consumer) fetchOffset(partitionId int) error {
	totalSize := 2 + 4 + len(c.groupId) + 4 + len(c.topic) + 4

	reqFrame := make([]byte, totalSize)
	off := 0
	binary.BigEndian.PutUint16(reqFrame[off:], 0x0010)
	off += 2
	binary.BigEndian.PutUint32(reqFrame[off:], uint32(len(c.groupId)))
	off += 4
	copy(reqFrame[off:], []byte(c.groupId))
	off += len(c.groupId)
	binary.BigEndian.PutUint32(reqFrame[off:], uint32(len(c.topic)))
	off += 4
	copy(reqFrame[off:], []byte(c.topic))
	off += len(c.topic)
	binary.BigEndian.PutUint32(reqFrame[off:], uint32(partitionId))

	if _, err := c.conn.Write(reqFrame); err != nil {
		return err
	}

	resFrame := make([]byte, 10)
	if _, err := io.ReadFull(c.conn, resFrame); err != nil {
		return err
	}

	statusCode := binary.BigEndian.Uint16(resFrame[0:2])
	offset := binary.BigEndian.Uint64(resFrame[2:10])

	c.lastOffsets[partitionId] = int64(offset)

	if statusCode == 0x0001 {
		return nil
	} else {
		return fmt.Errorf("fetchOffset acting up\n")
	}
}

func (c *Consumer) getNumPartition(topic string) error {
	req := make([]byte, 2+4+len(topic))
	binary.BigEndian.PutUint16(req[0:2], 0x0013)
	binary.BigEndian.PutUint32(req[2:6], uint32(len(topic)))
	copy(req[6:], []byte(topic))
	c.conn.Write(req)

	resHeader := make([]byte, 6)
	if _, err := io.ReadFull(c.conn, resHeader); err != nil {
		return err
	}

	successCode := binary.BigEndian.Uint16(resHeader[0:2])
	partitionCount := binary.BigEndian.Uint32(resHeader[2:6])

	c.numPartitions = int(partitionCount)

	if successCode == 0x0001 {
		return nil
	} else {
		return fmt.Errorf("getPartitionId error")
	}
}

func (c *Consumer) commitOffset(partition int, offset int64) error {
	totalSize := 2 + 4 + len(c.groupId) + 4 + len(c.topic) + 4 + 8
	req := make([]byte, totalSize)
	off := 0
	binary.BigEndian.PutUint16(req[off:], 0x0009)
	off += 2
	binary.BigEndian.PutUint32(req[off:], uint32(len(c.groupId)))
	off += 4
	copy(req[off:], []byte(c.groupId))
	off += len(c.groupId)
	binary.BigEndian.PutUint32(req[off:], uint32(len(c.topic)))
	off += 4
	copy(req[off:], []byte(c.topic))
	off += len(c.topic)
	binary.BigEndian.PutUint32(req[off:], uint32(partition))
	off += 4
	binary.BigEndian.PutUint64(req[off:], uint64(offset))

	if _, err := c.conn.Write(req); err != nil {
		return err
	}

	res := make([]byte, 2)
	if _, err := io.ReadFull(c.conn, res); err != nil {
		return err
	}
	statusCode := binary.BigEndian.Uint16(res)

	if statusCode == 0x0001 {
		return nil
	}
	return fmt.Errorf("commitOffset gone wrong\n")
}

func (c *Consumer) Close() {
	c.conn.Close()
}
