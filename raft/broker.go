package main

import "sync"

type Broker struct {
	logs map[string]*Log
	mu   sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		logs: make(map[string]*Log),
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
