package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Node struct {
	port        int16
	role        string
	currentTerm int32
	votedFor    int16
	voteCount   int32

	heartbeatTick <-chan time.Time
	electionTimer *time.Timer
	lastHeartbeat time.Time
}

func NewNode(port int16) *Node {
	randSec := 8 + rand.Intn(11)

	electionTicker := time.NewTimer(time.Duration(randSec) * time.Second)
	heartbeatTicker := time.NewTicker(5 * time.Second)

	return &Node{
		port:          port,
		role:          "Candidate",
		electionTimer: electionTicker,
		heartbeatTick: heartbeatTicker.C,
		voteCount:     0,
		votedFor:      0,
		lastHeartbeat: time.Now(),
	}
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

// for {
// 	min := 8
// 	max := 18
// 	randomSeconds := min + rand.Intn(max-min+1)
// 	duration := time.Duration(randomSeconds) * time.Second

// 	time.Sleep(duration)

// 	fmt.Println("tick...")
// 	node.Heartbeat()
// }
