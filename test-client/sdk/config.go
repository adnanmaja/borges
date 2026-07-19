package sdk

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type Config struct {
	Brokers     []string
	ConnTimeout time.Duration
}

type Client struct {
	CurrentLeader int16
	Timeout       time.Duration
}

func ClientConfig(brokers []string, timeout ...time.Duration) *Config {
	connTimeout := 5 * time.Second
	if len(timeout) > 0 {
		connTimeout = timeout[0]
	}

	return &Config{
		Brokers:     brokers,
		ConnTimeout: connTimeout,
	}
}

func NewClient(c *Config) (*Client, error) {
	leader, err := findLeader(c)
	if err != nil {
		return nil, err
	}

	fmt.Println("[SDK] current leader:", leader)

	return &Client{
		CurrentLeader: leader,
		Timeout:       c.ConnTimeout,
	}, nil
}

func findLeader(c *Config) (int16, error) {
	for _, portStr := range c.Brokers {
		port, _ := strconv.Atoi(portStr)

		fmt.Printf("[DEBUG] Dialing %d with %d timeout\n", port, c.ConnTimeout)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", port), c.ConnTimeout)
		if err != nil {
			continue
		}

		reqFrame := make([]byte, 2)
		binary.BigEndian.PutUint16(reqFrame, 0x0011)
		conn.Write(reqFrame)

		resFrame := make([]byte, 4)
		_, err = io.ReadFull(conn, resFrame)
		conn.Close()
		if err != nil {
			return 0, err
		}

		successCode := binary.BigEndian.Uint16(resFrame[0:2])
		leaderPort := binary.BigEndian.Uint16(resFrame[2:4])

		if successCode == 0x0001 {
			return int16(leaderPort), nil
		}
	}
	return 0, fmt.Errorf("cant find the leader")
}

func (client *Client) Close() {
	fmt.Println("Closed, definetely")
}
