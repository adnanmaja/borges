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

func (c *Client) NewProducer(config ProducerConfig) (*Producer, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", c.CurrentLeader), c.Timeout)
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

func (p *Producer) Send(payload []byte) error {
	p.batch = append(p.batch, payload)
	if len(p.batch) >= int(p.batchSize) {
		return p.Flush()
	}
	return nil
}
func (p *Producer) Flush() error {
	if len(p.batch) == 0 {
		return nil
	}
	batch := p.batch
	p.batch = nil
	return p.write(batch)
}

func (p *Producer) write(batch [][]byte) error {
	// opcode(2) + from(2) + topicLen(4) + topic(N) + partitionId(4) + entriesCount(4)
	totalSize := 2 + 2 + 4 + len(p.topic) + 4 + 4
	for _, payload := range batch {
		totalSize += 4 + len(payload)
	}
	frame := make([]byte, totalSize)
	off := 0
	binary.BigEndian.PutUint16(frame[off:], 0x0006)
	off += 2
	binary.BigEndian.PutUint16(frame[off:], 0)
	off += 2
	binary.BigEndian.PutUint32(frame[off:], uint32(len(p.topic)))
	off += 4
	copy(frame[off:], p.topic)
	off += len(p.topic)
	partitionId := p.getPartitionId()
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
	if _, err := p.conn.Write(frame); err != nil {
		return err
	}
	resFrame := make([]byte, 2)
	if _, err := io.ReadFull(p.conn, resFrame); err != nil {
		return err
	}
	if binary.BigEndian.Uint16(resFrame) != 0x0001 {
		return fmt.Errorf("produce failed, figure out why yourself")
	}
	return nil
}

func (p *Producer) flushLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.Flush()
		case <-p.stopCh:
			return
		}
	}
}

func (p *Producer) Close() error {
	close(p.stopCh)
	p.Flush()
	return p.conn.Close()
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

func (p *Producer) getPartitionId() int {
	if p.partitionId != nil {
		return *p.partitionId
	}

	targetPartition := *p.partitionId % p.numPartitions
	*p.partitionId++

	return targetPartition
}
