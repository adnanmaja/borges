package raft

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type ConnPool struct {
	peers       []int16
	connections map[int16]net.Conn
	mu          sync.Mutex
}

func NewPool() *ConnPool {
	return &ConnPool{
		connections: make(map[int16]net.Conn),
	}
}

func (c *ConnPool) GetOrCreateConnection(port int16) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, exists := c.connections[port]
	if exists {
		return conn, nil
	}

	address := fmt.Sprintf(":%d", port)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, err
	}

	c.connections[port] = conn
	return conn, nil

}

func (c *ConnPool) Evict(port int16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.connections[port]; ok {
		conn.Close()
		delete(c.connections, port)
	}
}
