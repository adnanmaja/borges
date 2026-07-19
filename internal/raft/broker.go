package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Broker struct {
	partitions map[string]map[int32]*Partition
	offsets    map[string]map[string]map[int32]int64
	mu         sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		partitions: make(map[string]map[int32]*Partition),
		offsets:    make(map[string]map[string]map[int32]int64),
	}
}

func (b *Broker) GetOrCreatePartition(topic string, partitionId int32, port int16) *Partition {
	b.mu.RLock()
	pmap, topicExists := b.partitions[topic]
	if topicExists {
		p, exists := pmap[partitionId]
		if exists {
			b.mu.RUnlock()
			return p
		}
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	pmap, topicExists = b.partitions[topic]
	if !topicExists {
		pmap = make(map[int32]*Partition)
		b.partitions[topic] = pmap
	}

	p, exists := pmap[partitionId]
	if !exists {
		p = NewPartition(port, topic, partitionId)
		pmap[partitionId] = p
	}
	return p
}

func (node *Node) CreateTopic(topic string, numPartitions int32) error {
	for i := range numPartitions {
		node.broker.GetOrCreatePartition(topic, i, node.port)
	}

	if node.role == "Leader" {
		for _, port := range node.peers {
			conn, err := node.connPool.GetOrCreateConnection(port)
			if err != nil {
				continue
			}

			frame := make([]byte, 2+4+len(topic)+4)
			off := 0
			binary.BigEndian.PutUint16(frame[off:], 0x0012)
			off += 2
			binary.BigEndian.PutUint32(frame[off:], uint32(len(topic)))
			off += 4
			copy(frame[off:], []byte(topic))
			off += len(topic)
			binary.BigEndian.PutUint32(frame[off:], uint32(numPartitions))

			_, err = conn.Write(frame)
			return err
		}
	}
	return nil
}

func (b *Broker) SaveOffset(groupId, topic string, partitionId int32, offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, exists := b.offsets[groupId]
	if !exists {
		b.offsets[groupId] = make(map[string]map[int32]int64)
	}

	_, exists = b.offsets[groupId][topic]
	if !exists {
		b.offsets[groupId][topic] = make(map[int32]int64)
	}

	b.offsets[groupId][topic][partitionId] = offset
}

func (b *Broker) FetchOffset(groupId, topic string, partitionId int32) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	groupMap, exists := b.offsets[groupId]
	if exists {
		return groupMap[topic][partitionId]
	}

	return 0
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var errs []error
	for _, pmap := range b.partitions {
		for _, p := range pmap {
			if err := p.log.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("broker close errors: %v", errs)
	}
	return nil
}

func (b *Broker) saveSnapshot(port int16, stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mu.Lock()

			data, err := json.Marshal(b.offsets)
			b.mu.Unlock()
			if err != nil {
				fmt.Println("error marshaling:", err)
				continue
			}

			snapshotPath := fmt.Sprintf("data/%d/offset_snapshot.json", port)
			tmpPath := snapshotPath + ".tmp"

			err = os.WriteFile(tmpPath, data, 0644)
			if err != nil {
				fmt.Println("error writefile:", err)
				continue
			}

			os.Rename(tmpPath, snapshotPath)

		case <-stopCh:
			b.mu.Lock()
			data, err := json.Marshal(b.offsets)
			b.mu.Unlock()
			if err != nil {
				fmt.Println("error marshaling:", err)
				return
			}

			snapshotPath := fmt.Sprintf("data/%d/offset_snapshot.json", port)
			os.WriteFile(snapshotPath, data, 0644)
			fmt.Println("[SHUTDOWN] offsets snapshot saved")
			return
		}
	}
}

func (node *Node) loadSnapshot(b *Broker) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotPath := fmt.Sprintf("data/%d/offset_snapshot.json", node.port)

	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		b.offsets = make(map[string]map[string]map[int32]int64)
		return nil
	}

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &b.offsets)
}
