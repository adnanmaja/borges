package main

const debug = false

func main() {
	broker := NewBroker()
	StartServer(broker)
}
