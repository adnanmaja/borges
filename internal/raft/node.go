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
	payload   []byte
	term      int32
}

type entrySnapshot struct {
	Timestamp int64  `json:"ts"`
	Payload   []byte `json:"data"`
	Term      int32  `json:"term"`
}

func toEntrySnapshots(entries []Entry) []entrySnapshot {
	snap := make([]entrySnapshot, len(entries))
	for i, e := range entries {
		snap[i] = entrySnapshot{Timestamp: e.timestamp, Payload: e.payload, Term: e.term}
	}
	return snap
}

func fromEntrySnapshots(snap []entrySnapshot) []Entry {
	entries := make([]Entry, len(snap))
	for i, s := range snap {
		entries[i] = Entry{timestamp: s.Timestamp, payload: s.Payload, term: s.Term}
	}
	return entries
}

type Node struct {
	port          int16
	peers         []int16
	role          string
	currentTerm   int32
	votedFor      int16
	voteCount     int32
	currentLeader int16

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
	randSec := 1 + rand.Intn(5)

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
			0: {payload: nil, term: 0},
		},
		commitIndex: 0,
		nextIndex:   make(map[int16]int32),
		matchIndex:  make(map[int16]int32),
	}

	for _, p := range peers {
		os.MkdirAll(fmt.Sprintf("data/%d/log", p), 0755)
	}
	err := node.loadEntrySnapshot()
	if err != nil {
		fmt.Println("error loading entry snapshot:", err)
	}
	go node.snapshotEntries(node.stopCh)

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

	snap := toEntrySnapshots(node.entries[1:])
	data, err := json.Marshal(snap)
	node.mu.Unlock()
	if err == nil {
		os.WriteFile(fmt.Sprintf("data/%d/entry_snapshot.json", node.port), data, 0644)
		fmt.Println("[SHUTDOWN] entry snapshot saved")
	}

	node.broker.mu.Lock()
	data, err = json.Marshal(node.broker.offsets)
	node.broker.mu.Unlock()
	if err == nil {
		snapshotPath := fmt.Sprintf("data/%d/offset_snapshot.json", node.port)
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

func (node *Node) compactMemory() {
	maxEntries := 1000

	if len(node.entries) > maxEntries {
		keepFrom := len(node.entries) - maxEntries

		newEntries := make([]Entry, maxEntries)
		copy(newEntries, node.entries[keepFrom:])
		node.entries = newEntries
	}
}

func (node *Node) snapshotEntries(stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			node.mu.Lock()
			snap := toEntrySnapshots(node.entries[1:])
			node.mu.Unlock()

			data, err := json.Marshal(snap)
			if err != nil {
				fmt.Println("error marshaling:", err)
				continue
			}

			snapshotPath := fmt.Sprintf("data/%d/entry_snapshot.json", node.port)
			tmpPath := snapshotPath + ".tmp"
			err = os.WriteFile(tmpPath, data, 0644)
			if err != nil {
				fmt.Println("error writefile:", err)
				continue
			}
			os.Rename(tmpPath, snapshotPath)

		case <-stopCh:
			node.mu.Lock()
			snap := toEntrySnapshots(node.entries[1:])
			node.mu.Unlock()

			data, err := json.Marshal(snap)
			if err != nil {
				fmt.Println("error marshaling:", err)
				return
			}

			os.WriteFile(fmt.Sprintf("data/%d/entry_snapshot.json", node.port), data, 0644)
			fmt.Println("[SHUTDOWN] entry snapshot saved")
			return
		}
	}
}

func (node *Node) loadEntrySnapshot() error {
	node.mu.Lock()
	defer node.mu.Unlock()

	snapshotPath := fmt.Sprintf("data/%d/entry_snapshot.json", node.port)

	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		node.entries = []Entry{0: {payload: nil, term: 0}}
		return nil
	}

	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		return err
	}

	var snap []entrySnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	node.entries = append([]Entry{{payload: nil, term: 0}}, fromEntrySnapshots(snap)...)
	return nil
}
