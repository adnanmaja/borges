package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Broker struct {
	logs    map[string]*Log
	offsets map[string]map[string]int64
	mu      sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		logs:    make(map[string]*Log),
		offsets: make(map[string]map[string]int64),
	}
}

func (b *Broker) GetOrCreateLog(topic string, port int16) *Log {
	b.mu.Lock()
	defer b.mu.Unlock()

	log, exists := b.logs[topic]
	if !exists {
		log = NewLog(port, topic)
		b.logs[topic] = log
	}
	return log
}

func (b *Broker) SaveOffset(groupId, topic string, offset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, exists := b.offsets[groupId]
	if !exists {
		b.offsets[groupId] = make(map[string]int64)
	}

	b.offsets[groupId][topic] = offset
}

func (b *Broker) FetchOffset(groupId, topic string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	groupMap, exists := b.offsets[groupId]
	if exists {
		return groupMap[topic]
	}

	return 0 //default
}

func (node *Node) saveSnapshot(b *Broker) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		b.mu.Lock()
		data, err := json.Marshal(b.offsets)
		b.mu.Unlock()
		if err != nil {
			fmt.Println("error marshaling:", err)
			continue
		}

		snapshotPath := fmt.Sprintf("data/%d/offsets_sanpshot.json", node.port)
		tmpPath := snapshotPath + ".tmp"

		err = os.WriteFile(tmpPath, data, 0644)
		if err != nil {
			fmt.Println("error writefile:", err)
			continue
		}

		os.Rename(tmpPath, snapshotPath)
	}
}

func (node *Node) loadSnapshot(b *Broker) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotPath := fmt.Sprintf("data/%d/offsets_sanpshot.json", node.port)

	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		b.offsets = make(map[string]map[string]int64)
		return nil
	}

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &b.offsets)
}
