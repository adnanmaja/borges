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

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var errs []error
	for _, log := range b.logs {
		if err := log.Close(); err != nil {
			errs = append(errs, err)
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

			snapshotPath := fmt.Sprintf("data/%d/offsets_snapshot.json", port)
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

			snapshotPath := fmt.Sprintf("data/%d/offsets_snapshot.json", port)
			os.WriteFile(snapshotPath, data, 0644)
			fmt.Println("[SHUTDOWN] offsets snapshot saved")
			return
		}
	}
}

func (node *Node) loadSnapshot(b *Broker) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotPath := fmt.Sprintf("data/%d/offsets_snapshot.json", node.port)

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

// func (b *Broker) Close() error {

// 	b.mu.Lock()

// 	var errs []error
// 	for _, log := range b.logs {
// 		if err := log.activeIndex.writer.Flush(); err != nil {
// 			errs = append(errs, fmt.Errorf("failed to flush index: %w", err))
// 		}

// 		for path, f := range log.fdCache {
// 			f.Close()
// 			delete(log.fdCache, path)
// 		}
// 	}

// 	b.mu.Unlock()
// 	b.saveSnapshot()

// 	if len(errs) > 0 {
// 		return fmt.Errorf("encountered errors during shutdown: %v", errs)
// 	}
// 	return nil
// }
