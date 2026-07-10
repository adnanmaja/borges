package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
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
	peers       []int16
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

	broker          *Broker
	listener        net.Listener
	heartbeatTicker *time.Ticker
	stopCh          chan struct{}
	connPool        *ConnPool

	mu sync.Mutex
}

func NewNode(port int16, peers []int16) *Node {
	randSec := 8 + rand.Intn(11)

	electionTicker := time.NewTimer(time.Duration(randSec) * time.Second)
	heartbeatTicker := time.NewTicker(5 * time.Second)

	node := &Node{
		port:            port,
		peers:           peers,
		role:            "Follower",
		electionTimer:   electionTicker,
		heartbeat:       heartbeatTicker.C,
		heartbeatTicker: heartbeatTicker,
		stopCh:          make(chan struct{}),
		voteCount:       0,
		lastHeartbeat:   time.Now(),
		entries: []Entry{
			0: {payload: "", term: 0},
		},
		commitIndex: 0,
		nextIndex:   make(map[int16]int32),
		matchIndex:  make(map[int16]int32),
	}

	for _, p := range peers {
		os.MkdirAll(fmt.Sprintf("data/%d/log", p), 0755)
	}

	node.connPool = NewPool()
	node.broker = NewBroker()
	node.loadStates()
	go node.broker.saveSnapshot(node.port, node.stopCh)
	go node.saveStates()
	node.loadSnapshot(node.broker)

	for _, peer := range peers {
		if peer == node.port {
			continue
		}
		node.nextIndex[peer] = 1
		node.matchIndex[peer] = 0
	}

	return node
}

func (node *Node) Start() {
	go node.startListener()

	rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-node.heartbeat:
			if node.role == "Leader" {
				node.Heartbeat()
			}
		case <-node.electionTimer.C:
			if node.role == "Leader" {
				fmt.Println("[DEBUG] current role:", node.role)
			} else {
				fmt.Println("[DEBUG] electing myself")
				node.role = "Candidate"
				node.StartElection()
			}
		case <-node.stopCh:
			fmt.Println("[DEBUG] event loop exiting")
			return
		}
	}
}

func (node *Node) Shutdown() {
	fmt.Println("\n[DEBUG] saving state...")

	node.mu.Lock()
	node.saveStates()
	node.mu.Unlock()

	node.broker.mu.Lock()
	data, err := json.Marshal(node.broker.offsets)
	node.broker.mu.Unlock()
	if err == nil {
		snapshotPath := fmt.Sprintf("data/%d/offsets_snapshot.json", node.port)
		os.WriteFile(snapshotPath, data, 0644)
	}

	fmt.Println("[DEBUG] dead")
	os.Exit(0)
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
