package main

import "flag"

var ports = []int16{8080, 8081, 8082}

func main() {
	port := flag.Int("port", 8080, "which port this instance is assigned to")
	flag.Parse()

	node := NewNode(int16(*port))
	node.startLoop()
}
