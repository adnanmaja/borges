package main

import "flag"

func main() {
	port := flag.Int("port", 8080, "which port this instance is assigned to")
	flag.Parse()

	switch {
	case *port == 8080:
		node := NewNode(8080)
		node.startLoop()
	case *port == 8081:
		node := NewNode(8081)
		node.startLoop()
	case *port == 8082:
		node := NewNode(8082)
		node.startLoop()
	}
}
