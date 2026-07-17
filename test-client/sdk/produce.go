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
}

type Producer struct {
	topic          string
	batchSize      int16
	conn           net.Conn
	partitionCount int
	batch          [][]byte
	stopCh         chan struct{}
}

func (c *Client) NewProducer(config ProducerConfig) (*Producer, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", c.CurrentLeader), c.Timeout)
	if err != nil {
		return nil, err
	}

	p := &Producer{
		topic:          config.Topic,
		batchSize:      config.BatchSize,
		conn:           conn,
		partitionCount: 0,
		stopCh:         make(chan struct{}),
	}

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
	binary.BigEndian.PutUint32(frame[off:], 0)
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
