package sdk

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type ConsumerConfig struct {
	Topic       string
	GroupId     string
	MaxBatch    int
	PartitionId *int
}

type consumer struct {
	topic            string
	groupId          string
	maxBatch         int
	conn             net.Conn
	lastOffsets      map[int]int64
	partitonId       *int
	numPartitions    int
	currentPartition int
	ch               chan entry
	lastErr          error
}

type entry struct {
	timestamp int64
	payload   []byte
}

func (client *Client) NewConsumer(config ConsumerConfig) (*consumer, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.CurrentLeader), client.Timeout)
	if err != nil {
		return nil, err
	}

	consumer := &consumer{
		topic:       config.Topic,
		groupId:     config.GroupId,
		maxBatch:    config.MaxBatch,
		conn:        conn,
		lastOffsets: make(map[int]int64),
		partitonId:  config.PartitionId,
	}

	if consumer.partitonId == nil {
		consumer.currentPartition = 0
		if err := consumer.getNumPartition(consumer.topic); err != nil {
			return nil, err
		}

		for i := range consumer.numPartitions {
			consumer.fetchOffset(i)
		}
	} else {
		consumer.currentPartition = *consumer.partitonId
	}

	return consumer, nil
}

func (c *consumer) Start() (<-chan entry, error) {
	c.ch = make(chan entry, 100)

	go func() {
		defer close(c.ch)
		currentOffset := c.lastOffsets[c.currentPartition]
		for i := range c.numPartitions {
			fmt.Printf("[SDK] consuming partition %d, offset %d\n", i, currentOffset)
			entries, err := c.consume(i, currentOffset)
			if err != nil {
				fmt.Println("[CONSUME] error:", err)
				c.lastErr = err
				return
			}
			if err = c.commitOffset(i, currentOffset); err != nil {
				c.lastErr = err
				return
			}
			fmt.Printf("[SDK] committed partition %d, offset %d\n", i, currentOffset)

			for _, entry := range entries {
				c.ch <- entry
				currentOffset++
			}
		}
	}()
	return c.ch, nil
}

func (c *consumer) Err() error { return c.lastErr }

// note: do a regular consume, then return them to the client one by one
func (c *consumer) consume(parition int, offset int64) ([]entry, error) {
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

	binary.BigEndian.PutUint32(reqFrame[off:], uint32(parition))
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

	var entries []entry
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

		entries = append(entries, entry{timestamp: int64(timestamp), payload: payload})
	}

	if statusCode == 0x0001 {
		return entries, nil
	}
	return nil, fmt.Errorf("consume gone wrong\n")
}

func (c *consumer) fetchOffset(partitionId int) error {
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

func (c *consumer) getNumPartition(topic string) error {
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

func (c *consumer) commitOffset(partition int, offset int64) error {
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

func (c *consumer) Close() {
	c.conn.Close()
}
