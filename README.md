## Borges - A Lightweight, Single-Node Pub/Sub Engine
Borges is a minimal, low-level message broker inspired by Apache Kafka, implemented entirely from scratch in Go.

The project focuses on the core storage and networking primitives of event streaming: building an append-only log, managing segment rotation, implementing custom binary wire protocols, and structuring efficient index files.

## Architecture

Borges is structured as a single-node broker designed for concurrent TCP clients. It avoids high-level database abstractions in favor of direct file-system mechanics:

- **Broker** (`broker.go`) — Manages topic-to-log mappings and consumer group offsets; creates or retrieves logs on demand.
- **TCP Server** (`network.go`) — Listens on `:8080`, accepts concurrent clients, and handles produce (`0x01`), consume (`0x02`), fetch offset (`0x03`), and commit offset (`0x04`) commands.
- **Log** (`log.go`) — The core abstraction managing an append-only sequence of records distributed across disk segments. A background goroutine runs every 30 seconds, deleting `.log` and `.index` files for segments closed and inactive for 10 minutes (retention-based cleanup).
- **Segments** (`segment.go`) — Fixed-size log files (default 1 KB for testing, 1 MB intended for real use). When a segment fills up, a new one is created at the next offset. Each segment carries its own mutex for fine-grained locking during concurrent writes.
- **Index** (`index.go`) — Each segment has a corresponding `.index` file mapping relative offsets to physical byte positions within the `.log` file (16 bytes per entry: 8-byte relative offset + 8-byte absolute offset). Index lookups use binary search (O(log n)) with buffered writes batched through a 4 KB write buffer.
- **Consumer Group Offsets** (`broker.go`) — In-memory offset tracking per `(groupId, topic)` pair, committed and fetched via the wire protocol. Enables at-least-once consumption semantics.
- **Clients** — Two clients are provided:
  - `client/client.go` — Minimal example client demonstrating all four operations.
  - `client/stress/stress.go` — Concurrent stress tester spawning 20 workers with 50 randomized operations each.

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

Old segments are automatically cleaned up: closed segments with `.log` and `.index` files older than 10 minutes are deleted by a background goroutine that runs every 30 seconds.

## Optimizations

A series of throughput and latency optimizations have been applied beyond the initial implementation:

- **Binary search index lookup** — Offset resolution in `offsetLookup` was changed from a linear scan to binary search, reducing index lookup from O(n) to O(log n) per read.
- **Segment-level locking** — A per-segment mutex (`segment.mu`) allows concurrent producers writing to different segments to operate in parallel, instead of contending on a single log-level lock for the entire write path.
- **Buffered index writes** — A 4 KB `bufio.Writer` buffers index entries, coalescing many small writes into fewer syscalls.
- **Write buffer pooling** — A `sync.Pool` reuses pre-allocated write buffers, eliminating per-message heap allocations on the hot produce path.
- **File descriptor caching** — Open `.log` file handles are cached per path in `readCache`, avoiding `os.Open`/`Close` on every consume call. Cached descriptors are evicted when their segment is cleaned up.
- **`ReadAt` for random access** — Reads use `file.ReadAt` with an explicit offset rather than `Seek` + `Read`, avoiding file position state and enabling safe concurrent reads on the same file descriptor.
- **Debug print gating** — All `[DEBUG]` print statements are guarded by a `const debug` compile-time toggle (set to `false` in production), eliminating `fmt.Println` overhead from the hot path.
- **Cleanup interval tuning** — The background retention sweep was reduced from every 20 seconds to every 30 seconds, lowering periodic I/O pressure.

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
├── main.go          # entry point (debug toggle constant)
├── broker.go        # broker (topic → log manager)
├── log.go           # core Log (write/read/segment management)
├── segment.go       # log segment file
├── index.go         # offset index file
├── network.go       # TCP server & client handler
├── client/
│   ├── client.go         # example client
│   └── stress/
│       └── stress.go     # concurrent stress tester
├── logs/            # per-topic segment + index files (created at runtime)
└── go.mod
```
