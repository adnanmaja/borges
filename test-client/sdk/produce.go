package sdk

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

type ProducerConfig struct {
	Topic           string
	BatchSize       int16
	FlushIntervalMs int
	PartitionId     *int
}

type Producer struct {
	topic         string
	batchSize     int16
	conn          net.Conn
	numPartitions int
	partitionId   *int
	batch         [][]byte
	stopCh        chan struct{}
}

func (client *Client) NewProducer(config ProducerConfig) (*Producer, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.CurrentLeader), client.Timeout)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		topic:         config.Topic,
		batchSize:     config.BatchSize,
		conn:          conn,
		numPartitions: 0,
		partitionId:   config.PartitionId,
		stopCh:        make(chan struct{}),
	}

	if p.partitionId == nil {
		zero := 0
		p.partitionId = &zero
	}

	p.getNumPartition(p.topic)

	if config.FlushIntervalMs > 0 {
		go p.flushLoop(time.Duration(config.FlushIntervalMs) * time.Millisecond)
	}

	return p, nil
}

func (c *Producer) Send(payload []byte) error {
	c.batch = append(c.batch, payload)
	if len(c.batch) >= int(c.batchSize) {
		return c.Flush()
	}
	return nil
}
func (c *Producer) Flush() error {
	if len(c.batch) == 0 {
		return nil
	}
	batch := c.batch
	c.batch = nil
	return c.write(batch)
}

func (c *Producer) write(batch [][]byte) error {
	// opcode(2) + from(2) + topicLen(4) + topic(N) + partitionId(4) + entriesCount(4)
	totalSize := 2 + 2 + 4 + len(c.topic) + 4 + 4
	for _, payload := range batch {
		totalSize += 4 + len(payload)
	}
	frame := make([]byte, totalSize)
	off := 0
	binary.BigEndian.PutUint16(frame[off:], 0x0006)
	off += 2
	binary.BigEndian.PutUint16(frame[off:], 0)
	off += 2
	binary.BigEndian.PutUint32(frame[off:], uint32(len(c.topic)))
	off += 4
	copy(frame[off:], c.topic)
	off += len(c.topic)
	partitionId := c.getPartitionId()
	binary.BigEndian.PutUint32(frame[off:], uint32(partitionId))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(len(batch)))
	off += 4
	for _, payload := range batch {
		binary.BigEndian.PutUint32(frame[off:], uint32(len(payload)))
		off += 4
		copy(frame[off:], payload)
		off += len(payload)
	}
	if _, err := c.conn.Write(frame); err != nil {
		return err
	}
	resFrame := make([]byte, 2)
	if _, err := io.ReadFull(c.conn, resFrame); err != nil {
		return err
	}
	if binary.BigEndian.Uint16(resFrame) != 0x0001 {
		return fmt.Errorf("produce failed, figure out why yourself\n")
	}
	return nil
}

func (c *Producer) flushLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.Flush()
		case <-c.stopCh:
			return
		}
	}
}

func (c *Producer) Close() error {
	close(c.stopCh)
	c.Flush()
	return c.conn.Close()
}

func (p *Producer) getNumPartition(topic string) error {
	req := make([]byte, 2+4+len(topic))
	binary.BigEndian.PutUint16(req[0:2], 0x0013)
	binary.BigEndian.PutUint32(req[2:6], uint32(len(topic)))
	copy(req[6:], []byte(topic))
	p.conn.Write(req)

	resHeader := make([]byte, 6)
	if _, err := io.ReadFull(p.conn, resHeader); err != nil {
		return err
	}

	successCode := binary.BigEndian.Uint16(resHeader[0:2])
	partitionCount := binary.BigEndian.Uint32(resHeader[2:6])

	p.numPartitions = int(partitionCount)

	if successCode == 0x0001 {
		return nil
	} else {
		return fmt.Errorf("getPartitionId error")
	}
}

func (c *Producer) getPartitionId() int {
	if c.partitionId != nil {
		return *c.partitionId
	}

	targetPartition := *c.partitionId % c.numPartitions
	*c.partitionId++

	return targetPartition
}
