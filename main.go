package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/adnanmaja/borges/internal/raft"
)

func main() {
	port := flag.Int("port", 8080, "port for this node")
	peersRaw := flag.String("peers", "", "its peers (8081,8082)")
	flag.Parse()

	allPorts := []int16{int16(*port)}
	for _, p := range strings.Split(*peersRaw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid peer port: %s\n", p)
			os.Exit(1)
		}
		allPorts = append(allPorts, int16(v))
	}

	node := raft.NewNode(int16(*port), allPorts)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		node.Shutdown()
	}()

	node.Start()
}
