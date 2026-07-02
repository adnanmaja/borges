## Borges - A Lightweight, Single-Node Pub/Sub Engine
Borges is a minimal, low-level message broker inspired by Apache Kafka, implemented entirely from scratch in Go.

The project focuses on the core storage and networking primitives of event streaming: building an append-only log, managing segment rotation, implementing custom binary wire protocols, and structuring efficient index files

## Architecture

Borges is structured as a single-node broker designed for concurrent TCP clients. It avoids high-level database abstractions in favor of direct file-system mechanics:

- **Broker** (`broker.go`) — Manages topic-to-log mappings and consumer group offsets; creates or retrieves logs on demand.
- **TCP Server** (`network.go`) — Listens on `:8080`, accepts concurrent clients, and handles produce (`0x01`), consume (`0x02`), fetch offset (`0x03`), and commit offset (`0x04`) commands.
- **Log** (`log.go`) — The core abstraction managing an append-only sequence of records distributed across disk segments. A background goroutine runs every 10 seconds, deleting `.log` and `.index` files for segments that have been closed and inactive for 5 minutes (retention-based cleanup).
- **Segments** (`segment.go`) — Fixed-size log files (default 1 KB for testing, 1 MB intended for real use). When a segment fills up, a new one is created at the next offset.
- **Index** (`index.go`) — Each segment has a corresponding `.index` file mapping relative offsets to physical byte positions within the `.log` file (16 bytes per entry: 8-byte relative offset + 8-byte absolute offset).
- **Consumer Group Offsets** (`broker.go`) — In-memory offset tracking per `(groupId, topic)` pair, committed and fetched via the wire protocol. Enables at-least-once consumption semantics.
- **Client** (`client/client.go`) — Example TCP client demonstrating produce, consume, fetch offset, and commit offset operations.

## Wire Protocol
Borges utilizes a custom binary protocol over raw TCP for minimal framing overhead.

| Command | Byte | Payload |
|---------|------|---------|
| Produce | `0x01` | `[2 byte topic length][topic][4 byte payload length][N bytes payload]` |
| Consume | `0x02` | `[2 byte topic length][topic][8 byte offset]` |
| Fetch Offset | `0x03` | `[2 byte group id length][group id][2 byte topic length][topic]` |
| Commit Offset | `0x04` | `[2 byte group id length][group id][2 byte topic length][topic][8 byte offset]` |

Responses: Produce replies with `0x00` (success) or `0x01` (error). Consume replies with `[0x00][8 byte timestamp][4 byte payload length][N bytes payload]`. Fetch Offset replies with `[0x00][8 byte offset]`. Commit Offset replies with `0x00` (success).

## Storage Format

Topics are isolated into dedicated directories under `logs/<topic>/`. Storage files utilize zero-padded 64-bit integer naming schemas based on the base offset of the segment:

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

Old segments are automatically cleaned up: closed segments with `.log` and `.index` files older than 10 minutes are deleted by a background goroutine that runs every 20 seconds.

## Running
To spin up the broker:

```bash
go run .
```

To run the reference client and perform produce/consume operations, execute this in a separate terminal:

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
│   └── client.go    # test client (produce, consume, fetch/commit offset)
├── logs/            # per-topic segment + index files (created at runtime)
└── go.mod
```
