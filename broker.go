package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Broker struct {
	mu      sync.RWMutex
	logs    map[string]*Log
	offsets map[string]map[string]int64 // map[GroupID]map[TopicName]Offset
	stopCh  chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		logs:    make(map[string]*Log),
		offsets: make(map[string]map[string]int64),
		stopCh:  make(chan struct{}),
	}
	b.loadSnapshot()
	b.periodicSnapshot()
	return b
}

func (b *Broker) GetOrCreateLog(topic string) *Log {
	b.mu.Lock()
	defer b.mu.Unlock()

	log, exists := b.logs[topic]
	if !exists {
		log = NewLog(topic, b.stopCh)
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
	close(b.stopCh)

	b.mu.Lock()

	var errs []error
	for _, log := range b.logs {
		if err := log.activeIndex.writer.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("failed to flush index: %w", err))
		}

		for path, f := range log.fdCache {
			f.Close()
			delete(log.fdCache, path)
		}
	}

	b.mu.Unlock()
	b.saveSnapshot()

	if len(errs) > 0 {
		return fmt.Errorf("encountered errors during shutdown: %v", errs)
	}
	return nil
}

func (b *Broker) saveSnapshot() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := json.Marshal(b.offsets)
	if err != nil {
		return err
	}

	snapshotPath := "logs/offset_snapshot.json"
	tmpPath := snapshotPath + ".tmp"

	err = os.WriteFile(tmpPath, data, 0644)
	if err != nil {
		return err
	}

	return os.Rename(tmpPath, snapshotPath)
}

func (b *Broker) loadSnapshot() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotPath := "logs/offset_snapshot.json"

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

func (b *Broker) periodicSnapshot() {
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				b.saveSnapshot()
			case <-b.stopCh:
				return
			}
		}
	}()
}
