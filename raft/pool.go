package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type PeerPool struct {
	mu    sync.Mutex
	peers map[int16]*peerConn
}

type peerConn struct {
	conn net.Conn
	mu   sync.Mutex
}

func NewPeerPool() *PeerPool {
	return &PeerPool{peers: make(map[int16]*peerConn)}
}

func (p *PeerPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.peers {
		pc.mu.Lock()
		if pc.conn != nil {
			pc.conn.Close()
			pc.conn = nil
		}
		pc.mu.Unlock()
	}
}

func (p *PeerPool) Send(port int16, timeout time.Duration, fn func(net.Conn) error) error {
	p.mu.Lock()
	pc, ok := p.peers[port]
	if !ok {
		pc = &peerConn{}
		p.peers[port] = pc
	}
	p.mu.Unlock()

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn == nil {
		var err error
		pc.conn, err = net.DialTimeout("tcp", fmt.Sprintf(":%d", port), timeout)
		if err != nil {
			return err
		}
	}

	pc.conn.SetDeadline(time.Now().Add(timeout))
	err := fn(pc.conn)
	pc.conn.SetDeadline(time.Time{})

	if err != nil {
		pc.conn.Close()
		pc.conn = nil
	}
	return err
}
