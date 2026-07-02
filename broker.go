package main

import (
	"sync"
)

type Broker struct {
	mu      sync.RWMutex
	logs    map[string]*Log
	offsets map[string]map[string]int64 // map[GroupID]map[TopicName]Offset
}

func NewBroker() *Broker {
	return &Broker{
		logs:    make(map[string]*Log),
		offsets: make(map[string]map[string]int64),
	}
}

func (b *Broker) GetOrCreateLog(topic string) *Log {
	b.mu.Lock()
	defer b.mu.Unlock()

	log, exists := b.logs[topic]
	if !exists {
		log = NewLog(topic)
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
