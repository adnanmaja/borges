package main

import (
	"github.com/adnanmaja/borges/internal/engine"
)

func main() {
	broker := engine.NewBroker()
	engine.StartServer(broker)
}
