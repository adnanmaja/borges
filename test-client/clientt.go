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

	payload := `[Verse 1: Harry, Liam]
Remember when we would stay out too late
We were young, havin' fun, made mistakes
Did we ever know? Did we ever know?
Did we ever know? Yeah
All the things we'd just think of and say
Never wrong, always right, not afraid
Did we ever know? Did we ever know?
Did we ever know?

[Pre-Chorus: Zayn]
Is it all inside of my head?
Maybe you still think I don't care
But all I need is you
Yeah, you know it's true, yeah, you know it's true

[Chorus: Harry, All]
Forget about where we are and let go
We're so close
If you don't know where to start, just hold on
And don't run, no
We're looking back, we messed around
But that was then and this is now
All we need's enough love to hold us
Where we are`

	if err = producer.Send([]byte(payload)); err != nil {
		panic(err)
	}
	producer.Close()
}
