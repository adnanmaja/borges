package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"
)

func (node *Node) StartElection() {
	if node.role == "Candidate" {

		fmt.Printf("[ELECTION] i wanna be the leader! \n")

		if time.Since(node.lastHeartbeat) < 8*time.Second {
			fmt.Println("[ELECTION] leader is alive, stepping down")
			node.role = "Follower"
			randSec := 8 + rand.Intn(11)
			node.electionTimer.Reset(time.Duration(randSec) * time.Second)
			return
		}

		node.currentTerm++
		node.voteCount = 0
		node.votedFor = node.port

		node.saveStates()

		for _, port := range ports {
			if port == node.port {
				continue
			}
			lastLogIndex := len(node.entries) - 1
			lastLogTerm := node.entries[lastLogIndex].term
			conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), 5*time.Second)
			if err != nil {
				fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
				continue
			}
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			fmt.Println("[ELECTION] Everybody please vote for me")
			granted, err := sendVoteRequest(conn, node.port, node.currentTerm, int32(lastLogIndex), lastLogTerm)
			conn.Close()
			if err != nil {
				fmt.Printf("[ELECTION] cannot reach %d: %s\n", port, err)
				continue
			}
			if granted {
				node.CountVote()
			}
		}

		randSec := 8 + rand.Intn(11)
		node.electionTimer.Reset(time.Duration(randSec) * time.Second)
	}
}

func (node *Node) Vote(target int16, term, lastLogIndex, lastLogTerm int32) bool {
	if term < node.currentTerm {
		return false
	}

	if term > node.currentTerm {
		node.currentTerm = term
		node.votedFor = 0
		node.role = "Follower"
	}

	if node.votedFor != 0 && node.votedFor != target {
		return false
	}

	currentLastLogTerm := node.entries[len(node.entries)-1].term
	currentLastLogIndex := len(node.entries) - 1

	if lastLogTerm > currentLastLogTerm || (lastLogTerm == currentLastLogTerm && lastLogIndex >= int32(currentLastLogIndex)) {
		node.votedFor = target
		fmt.Println("[ELECTION] I voted for", node.votedFor)
		node.saveStates()
		return true
	}

	return false
}

func (node *Node) CountVote() {
	node.voteCount++
	fmt.Println("[ELECTION] Thank you!!")

	if node.voteCount > int32((len(ports))/2) {
		node.role = "Leader"
		fmt.Println("[ELECTION] Im the leader now")

		for _, port := range ports {
			if port == node.port {
				continue
			} else {
				node.Heartbeat()
				node.nextIndex[port] = int32(len(node.entries))
			}
		}
	}
}

func (node *Node) resetElectionTimer(leaderTerm int32) {
	if leaderTerm > node.currentTerm {
		node.currentTerm = leaderTerm
		node.votedFor = 0
	}
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

func sendVoteRequest(conn net.Conn, senderPort int16, term, lastLogIndex, lastLogTerm int32) (bool, error) {
	// [0x0005][2B from][4B sender's term][4B lastLogIndex][4B lastLogTerm]
	totalSize := 2 + 2 + 4 + 4 + 4
	frame := make([]byte, totalSize)
	off := 0

	binary.BigEndian.PutUint16(frame[off:], 0x0005)
	off += 2
	binary.BigEndian.PutUint16(frame[off:], uint16(senderPort))
	off += 2
	binary.BigEndian.PutUint32(frame[off:], uint32(term))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(lastLogIndex))
	off += 4
	binary.BigEndian.PutUint32(frame[off:], uint32(lastLogTerm))

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
