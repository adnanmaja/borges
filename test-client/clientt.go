package main

import "github.com/adnanmaja/borges/test-client/sdk"

func main() {
	brokers := []string{"8080", "8081", "8082"}
	cfg := sdk.ClientConfig(brokers)
	client, err := sdk.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	producer, err := client.NewProducer(sdk.ProducerConfig{
		Topic:     "test",
		BatchSize: 10,
	})
	if err != nil {
		panic(err)
	}

	if err = producer.Send([]byte("here goes nothing")); err != nil {
		panic(err)
	}
	producer.Close()
}
