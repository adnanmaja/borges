package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"
)

func (node *Node) ElectItself() {
	if node.role == "Candidate" {

		fmt.Printf("[ELECTION] i wanna be the leader! \n")

		node.currentTerm++

		if node.votedFor != node.port && node.role == "Follower" {
			node.role = "Follower"
		} else {
			node.votedFor = node.port
			ports := []int16{8080, 8081, 8082}

			for _, port := range ports {
				if port == node.port {
					continue
				} else {
					conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 5*time.Second)
					if err != nil {
						fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
						continue
					}
					granted, err := sendVoteRequest(conn, node.port)
					fmt.Println("[ELECTION] Everybody please vote for me")
					if granted {
						conn.Close()
						node.RecapVote()

					}
				}
			}
		}
	}

}

func (node *Node) Vote(target int16) bool {
	if node.votedFor != 0 {
		return false
	} else {
		node.votedFor = target
		fmt.Println("[ELECTION] I voted for", node.votedFor)
		return true
	}
}

func (node *Node) RecapVote() {
	node.voteCount++
	fmt.Println("[ELECTION] Thank you!!")

	if node.voteCount >= 1 {
		node.role = "Leader"
		fmt.Println("[ELECTION] Im the leader now")
		ports := []int16{8080, 8081, 8082}

		for _, port := range ports {
			if port == node.port {
				continue
			} else {
				node.Heartbeat()
				node.nextIndex[port] = int32(len(node.logs))
			}
		}
	}
}

func (node *Node) resetElectionTimer() {
	node.role = "Follower"
	node.lastHeartbeat = time.Now()
	node.electionTimer.Stop()
	select {
	case <-node.electionTimer.C:
	default:
	}
	randSec := 8 + rand.Intn(11)
	node.electionTimer.Reset(time.Duration(randSec) * time.Second)
}

func sendVoteRequest(conn net.Conn, senderPort int16) (bool, error) {
	// [0x0005][2B from]
	totalSize := 2 + 2
	frame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(frame[off:], 0x0005)
	off += 2

	binary.BigEndian.PutUint16(frame[off:], uint16(senderPort))

	_, err := conn.Write(frame)
	if err != nil {
		return false, err
	}

	responseFrame := make([]byte, 2)
	_, err = conn.Read(responseFrame)
	if err != nil {
		return false, err
	}

	responseCode := binary.BigEndian.Uint16(responseFrame)

	switch responseCode {
	case 0x0001:
		return true, nil
	case 0x0002:
		return false, nil
	}
	return false, nil
}
