# Borges Client SDK

A Go client SDK for the Borges message broker. Lives in `sdk/`.

---

## Setup

```go
import "github.com/adnanmaja/borges/sdk"
```

Create a client with a list of known broker ports. The SDK auto-discovers the leader.

```go
brokers := []string{"8080", "8081", "8082"}
cfg := sdk.ClientConfig(brokers)
client, err := sdk.NewClient(cfg)
if err != nil {
    // no reachable leader
}
defer client.Close()
```

---

## Producing messages

```go
producer, err := client.NewProducer(sdk.ProducerConfig{
    Topic:     "orders",
    BatchSize: 10,         // auto-flush every 10 messages
    FlushIntervalMs: 5000, // also flush every 5s (0 to disable)
})
if err != nil {
    // topic not found
}
defer producer.Close()

for _, msg := range messages {
    if err := producer.Send([]byte(msg)); err != nil {
        // handle error
    }
}
producer.Flush() // flush any remaining buffered messages
```

`Producer` buffers messages in memory. It auto-flushes when the batch reaches `BatchSize`, on a periodic timer if `FlushIntervalMs` is set, or you can call `Flush()` manually. Partition assignment is round-robin; set `PartitionId` in the config to pin a specific partition.

---

## Consuming messages

### Auto-commit (default)

```go
consumer, err := client.NewConsumer(sdk.ConsumerConfig{
    Topic:    "orders",
    GroupId:  "my-group",
    MaxBatch: 100,
})
if err != nil {
    // topic not found
}
defer consumer.Close()

for msg := range consumer.Start() {
    fmt.Println("Received:", msg)
}

if err := consumer.Err(); err != nil {
    // consumer exited with error
}
```

`Consumer` discovers all partitions for the topic, picks up from the last committed offset for the group, and streams messages through a channel. Offsets are committed automatically after each fetch. `Start()` blocks the caller — run it in a goroutine if you need concurrent work.

### Manual commit

```go
consumer, err := client.NewConsumer(sdk.ConsumerConfig{
    Topic:            "orders",
    GroupId:          "my-group",
    MaxBatch:         100,
    EnableAutoCommit: boolPtr(false),
})
if err != nil {
    // topic not found
}
defer consumer.Close()

for msg := range consumer.Start() {
    process(msg)
    consumer.Commit(msg) // commit offset after processing
}

if err := consumer.Err(); err != nil {
    // consumer exited with error
}
```

Offsets are committed only when you call `Commit(e Entry)` — useful for at-least-once semantics.

### Entry type

Each message from the channel has:

| Field | Type | Description |
|---|---|---|
| `Timestamp` | `int64` | Millisecond Unix timestamp |
| `Payload` | `[]byte` | Message body |
| `Partition` | `int` | Partition the message came from |
| `Offset` | `int64` | Offset within the partition |

---

## API reference

### Config

| Field | Type | Default | Description |
|---|---|---|---|
| `Brokers` | `[]string` | required | Broker ports to try |
| `Timeout` | `time.Duration` | `5s` | Dial timeout |

### Client methods

| Method | Description |
|---|---|
| `NewProducer(cfg)` | Creates a producer for a topic |
| `NewConsumer(cfg)` | Creates a consumer for a topic + group |
| `Close()` | Tears down connections |

### ProducerConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `Topic` | `string` | required | Topic to write to |
| `BatchSize` | `int` | `1` | Messages before auto-flush |
| `FlushIntervalMs` | `int` | `0` | Periodic flush (disabled if 0) |
| `PartitionId` | `int` | `-1` | Pin a partition (< 0 = round-robin) |

### Producer methods

| Method | Description |
|---|---|
| `Send(payload)` | Buffers a message; flushes if batch is full |
| `Flush()` | Sends buffered messages immediately |
| `Close()` | Flushes and closes the connection |

### ConsumerConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `Topic` | `string` | required | Topic to read from |
| `GroupId` | `string` | required | Consumer group for offset tracking |
| `MaxBatch` | `int` | `100` | Max messages per fetch |
| `PartitionId` | `int` | `-1` | Pin a partition (< 0 = all partitions) |
| `EnableAutoCommit` | `*bool` | `true` | Auto-commit offsets after fetch (set `false` for manual commit) |

### Consumer methods

| Method | Description |
|---|---|
| `Start()` | Returns `<-chan Entry`, begins streaming messages |
| `Commit(e Entry)` | Manually commits an entry's offset (only needed when `EnableAutoCommit` is `false`) |
| `Err()` | Returns any fatal error |
| `Close()` | Stops the consumer and closes the connection |
