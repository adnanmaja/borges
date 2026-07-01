## Borges - temu Kafka

Built this as a freshman to wrap my head around backend fundamentals. The idea was to implement a bare-bones Kafka-like pub/sub from scratch — strictly for learning, definitely not production-ready.

## Architecture

Borges is a minimal Kafka-like pub/sub system built from scratch in Go. It consists of:

- **Broker** (`broker.go`) — Manages topic-to-log mappings; creates or retrieves logs on demand.
- **TCP Server** (`network.go`) — Listens on `:8080`, accepts concurrent clients, and handles produce (0x01) and consume (0x02) commands.
- **Log** (`log.go`) — The core append-only log. Manages segments, indexes, and provides `Write(payload)` / `Read(offset)`.
- **Segments** (`segment.go`) — Fixed-size log files (default 1 KB for testing, 1 MB intended for real use). When a segment fills up, a new one is created at the next offset.
- **Index** (`index.go`) — Each segment has a corresponding `.index` file mapping relative offsets to physical byte positions within the `.log` file (16 bytes per entry: 8-byte relative offset + 8-byte absolute offset).
- **Client** (`client/client.go`) — Example TCP client that sends a produce request then a consume request.

## Wire Protocol

| Command | Byte | Payload |
|---------|------|---------|
| Produce | `0x01` | `[2 byte topic length][topic][4 byte payload length][N bytes payload]` |
| Consume | `0x02` | `[2 byte topic length][topic][8 byte offset]` |

Responses: `0x00` = success, `0x01` = error (produce only). Consume replies with `[0x00][8 byte timestamp][4 byte payload length][N bytes payload]`.

## Storage Format

Logs are stored per-topic under `logs/<topic>/`:

```
logs/
└── <topic>/
    ├── 00000000000000000000.log   # segment file (raw records)
    ├── 00000000000000000000.index # index file (offset → byte position)
    ├── 00000000000000000032.log   # new segment after rotation
    └── 00000000000000000032.index
```

Each record on disk: `[4 byte payload length][8 byte Unix ms timestamp][payload]`.

Index entries are 16 bytes each: `[8 byte relative offset][8 byte absolute byte offset]`.

## Running

```bash
go run .
```

Then in another terminal:

```bash
cd client && go run .
```

## Project

```
├── main.go          # entry point
├── broker.go        # broker (topic → log manager)
├── log.go           # core Log (write/read/segment management)
├── segment.go       # log segment file
├── index.go         # offset index file
├── network.go       # TCP server & client handler
├── client/
│   └── client.go    # test client (produce + consume)
├── logs/            # per-topic segment + index files (created at runtime)
└── go.mod
```
