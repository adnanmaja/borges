package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
)

var ports = []int16{8080, 8081, 8082}

const MaxPayloadSize int = 64 * 1024 // 64 KB

func main() {
	port := flag.Int("port", 8080, "which port this instance is assigned to")
	flag.Parse()

	node := NewNode(int16(*port))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		node.Shutdown()
	}()

	node.startLoop()
}
