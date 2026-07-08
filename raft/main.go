package main

import "flag"

func main() {
	port := flag.Int("port", 8080, "which port this instance is assigned to")
	flag.Parse()

	node := NewNode(int16(*port))
	node.startLoop()
}
