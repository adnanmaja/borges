package main

import (
	"sync"
)

type Broker struct {
	mu   sync.RWMutex
	logs map[string]*Log
}

func NewBroker() *Broker {
	return &Broker{
		logs: make(map[string]*Log),
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
