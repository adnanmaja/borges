package main

import (
	"fmt"
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

	heartbeatTick <-chan time.Time
	electionTimer *time.Timer
	lastHeartbeat time.Time

	logs        []Entry
	commitIndex int32
	nextIndex   map[int16]int32
	matchIndex  map[int16]int32

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
		heartbeatTick: heartbeatTicker.C,
		voteCount:     0,
		votedFor:      0,
		lastHeartbeat: time.Now(),
		logs: []Entry{
			0: {payload: "", term: 0}, // fill index 0 with dummy data, so it starts appending at index 1
		},
		commitIndex: 0,
		nextIndex:   make(map[int16]int32),
		matchIndex:  make(map[int16]int32),
	}

	ports := []int16{8080, 8081, 8082}
	for _, port := range ports {
		os.MkdirAll(fmt.Sprintf("logs/%d", port), 0755)
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
		case <-node.heartbeatTick:
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
				node.ElectItself()
			}
		}
	}
}
