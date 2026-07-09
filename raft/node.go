package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

type Entry struct {
	timestamp int64
	payload   string
	term      int32
}

type Node struct {
	port        int16
	role        string
	currentTerm int32
	votedFor    int16
	voteCount   int32

	heartbeat     <-chan time.Time
	electionTimer *time.Timer
	lastHeartbeat time.Time

	entries     []Entry
	commitIndex int32
	nextIndex   map[int16]int32
	matchIndex  map[int16]int32

	broker *Broker

	mu sync.Mutex
}

func NewNode(port int16) *Node {
	randSec := 8 + rand.Intn(11)

	electionTicker := time.NewTimer(time.Duration(randSec) * time.Second)
	heartbeatTicker := time.NewTicker(5 * time.Second)

	node := &Node{
		port:          port,
		role:          "Candidate",
		electionTimer: electionTicker,
		heartbeat:     heartbeatTicker.C,
		voteCount:     0,
		lastHeartbeat: time.Now(),
		entries: []Entry{
			0: {payload: "", term: 0},
		},
		commitIndex: 0,
		nextIndex:   make(map[int16]int32),
		matchIndex:  make(map[int16]int32),
	}

	ports := []int16{8080, 8081, 8082}
	for _, port := range ports {
		os.MkdirAll(fmt.Sprintf("data/%d/log", port), 0755)
	}

	node.broker = NewBroker()
	node.loadStates()
	go node.saveSnapshot(node.broker)
	go node.saveStates()
	node.loadSnapshot(node.broker)

	for _, port := range ports {
		if port == node.port {
			continue
		} else {
			node.nextIndex[port] = 1
			node.matchIndex[port] = 0
		}
	}

	return node
}

func (node *Node) startLoop() {
	go node.startListener()

	rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-node.heartbeat:
			if node.role == "Leader" {
				node.Heartbeat()
			} else {
				// nothing
			}
		case <-node.electionTimer.C:
			if node.role == "Leader" {
				fmt.Println("[DEBUG] current role:", node.role)
			} else {
				fmt.Println("[DEBUG] electing myself")
				node.role = "Candidate"
				node.StartElection()
			}
		}
	}
}

func (node *Node) saveStates() {
	path := fmt.Sprintf("data/%d/states.bin", node.port)

	buf := make([]byte, 6)
	binary.BigEndian.PutUint32(buf[0:4], uint32(node.currentTerm))
	binary.BigEndian.PutUint16(buf[4:6], uint16(node.votedFor))
	err := os.WriteFile(path, buf, 0644)
	if err != nil {
		fmt.Println("error writing file:", err)
		return
	}
}

func (node *Node) loadStates() {
	node.mu.Lock()
	defer node.mu.Unlock()

	path := fmt.Sprintf("data/%d/states.bin", node.port)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		node.currentTerm = 0
		node.votedFor = 0
		return
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Println("error reading file:", err)
		return
	}

	buf := make([]byte, 6)

	_, err = io.ReadFull(file, buf)
	if err != nil {
		fmt.Println("error reading file:", err)
		return
	}

	currentTerm := binary.BigEndian.Uint32(buf[0:4])
	votedFor := binary.BigEndian.Uint16(buf[4:6])

	node.currentTerm = int32(currentTerm)
	node.votedFor = int16(votedFor)
}
