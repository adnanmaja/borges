# Raft Consensus Implementation

A minimal [Raft](https://raft.github.io/) consensus protocol implementation over TCP in Go with log persistence.

## How to Run

### Running the Nodes

Open three terminals and run one instance on each port:

```bash
go run . -port 8080
go run . -port 8081
go run . -port 8082
```

All nodes start as **Candidate**. The first node to timeout and gather a majority of votes becomes the **Leader** and begins sending periodic heartbeats to the others.

### Running the Client

A reference client lives in the `client/` subdirectory:

```bash
cd client
go run . -port 8080
```


## Architecture

```
┌─────────────┐
│   main.go   │  Entry point. Parses the -port flag and starts a Node.
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   node.go   │  Node & Entry structs, constructor, startLoop() event loop, graceful Shutdown().
└──────┬──────┘
       │
├──────────────────┬──────────────────┬──────────────────┬──────────────────┬─────────────────┐
│                  │                  │                  │                  │                 │
▼                  ▼                  ▼                  ▼                  ▼                 ▼
┌───────────┐ ┌─────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌──────────┐
│heartbeat.go│ │election.go│ │listener.go  │ │  broker.go  │ │   log.go    │ │ pool.go  │
│Send        │ │Election  │ │TCP server   │ │Multi-topic  │ │Log write,   │ │Persistent│
│heartbeats  │ │campaign  │ │& dispatcher │ │log registry │ │append, read │ │peer pool │
└───────────┘ └─────────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────────┘
                                 │               │               │
                                 │               └───────┬───────┘
                                 │                       │
                                 └───────────┬───────────┘
                                             │
                                             ▼
                                      ┌─────────────┐
                                      │ segment.go  │  Log segment files on disk
                                      └──────┬──────┘
                                             │
                                             ▼
                                      ┌─────────────┐
                                      │  index.go   │  Index lookup files on disk
                                      └─────────────┘
```

### File Responsibilities

| File | Role |
|---|---|
| `main.go` | CLI flag parsing, node instantiation |
| `node.go` | `Node` & `Entry` structs, `NewNode()` constructor, `startLoop()` event loop, `Shutdown()` for graceful teardown, `saveStates()`/`loadStates()` binary persistence |
| `heartbeat.go` | Leader sends heartbeats to peers via the peer pool |
| `election.go` | Election campaign (`StartElection`), vote request/response (`Vote`), victory announcement (`CountVote`) |
| `listener.go` | TCP listener, opcode parsing, and dispatching client produce/consume and Raft requests |
| `broker.go` | Multi-topic log registry (`Broker`), maps topic names to `Log` instances, tracks consumer-group offsets (`SaveOffset`/`FetchOffset`), periodic offset snapshot persistence |
| `log.go` | Per-topic log manager (`Log` struct), write/append/read functions, wire-level log transmission, and disk log file management |
| `pool.go` | `PeerPool` — persistent, reused TCP connections to peer nodes, with per-peer mutex and automatic reconnection on failure |
| `segment.go` | Representation of individual `.log` data files (max 1MB) |
| `index.go` | Representation of individual `.index` files with binary search offsets for fast log lookups |

---

## Node State

```go
type Entry struct {
	timestamp int64
	payload   string
	term      int32
}

type Node struct {
	port        int16
	role        string            // "Follower", "Candidate", or "Leader"
	currentTerm int32
	votedFor    int16
	voteCount   int32

	heartbeat     <-chan time.Time  // fires every 5s — Leader sends heartbeats
	electionTimer *time.Timer      // fires on timeout — triggers election
	lastHeartbeat time.Time

	entries     []Entry           // replicated log entries (1-based index)
	commitIndex int32             // index of highest log entry known to be committed
	nextIndex   map[int16]int32   // for each server, index of the next log entry to send
	matchIndex  map[int16]int32   // for each server, index of highest log entry known to be replicated

	broker          *Broker       // multi-topic log registry
	pool            *PeerPool     // persistent peer connection pool

	listener        net.Listener  // TCP listener handle (used by Shutdown)
	heartbeatTicker *time.Ticker  // underlying ticker (used by Shutdown)
	stopCh          chan struct{}  // closed to signal all goroutines to exit

	mu sync.Mutex                 // protects node state access
}
```

### Role Transitions

```
  ┌──────────┐
  │ Follower │◄────────── Heartbeat from Leader
  └────┬─────┘
       │ Election timeout
       ▼
   ┌───────────┐
   │ Candidate │── Re-discovers Leader (heartbeat) ──► Follower
   └─────┬─────┘
         │ Wins majority vote (voteCount >= 1)
         ▼
   ┌────────┐
   │ Leader │
   └────────┘
```

---

## Event Loop (`startLoop`)

The main loop runs in `node.go` and multiplexes on three channels:

| Channel | When it fires | Action |
|---|---|---|
| `heartbeat` | Every 5 seconds | If **Leader**, broadcast `Heartbeat()` to all peers |
| `electionTimer.C` | After random timeout (8–18s) | If not Leader, call `StartElection()` |
| `stopCh` | On `Shutdown()` | Exit the loop and clean up |

---

## Peer Connection Pool (`PeerPool`)

`pool.go` introduces a persistent connection pool for outbound peer communication. Rather than opening a fresh TCP dial on every heartbeat or vote request, `PeerPool` maintains one long-lived connection per peer port.

```
PeerPool
  ├── peers[8081] → peerConn { conn, mu }
  └── peers[8082] → peerConn { conn, mu }
```

`pool.Send(port, timeout, fn)` locks the target peer's connection, lazily dials if `conn == nil`, sets a deadline, calls the user-supplied function, and resets the connection to `nil` on any error so the next call reconnects cleanly. This eliminates repeated dial overhead and fixes the previous `defer conn.Close()` misuse in the heartbeat loop.

---

## Wire Protocol

All messages are TCP frames with a binary layout.

### Common Header

All request frames start with a common header:
```
┌───────────┬─────────┬──────────┐
│ 2B opcode │ 2B from │ (payload)│
└───────────┴─────────┴──────────┘
```

Response frames consist of a 2-byte status/response code:
```
┌───────────────┐
│ 2B response   │
└───────────────┘
```

### Opcode Types

| Opcode | Command Name | Direction | Description |
|---|---|---|---|
| `0x0004` | Heartbeat | Leader → Follower | Periodic keep-alive |
| `0x0005` | Vote Request | Candidate → Peers | Candidate requests votes |
| `0x0006` | Client Produce | Client → Node | Client sends a **batch** of log entries to a topic |
| `0x0007` | Append Entries | Leader → Follower | Leader replicates log entries to followers |
| `0x0008` | Client Consume | Client → Node | Client requests log entries by topic, offset, and count |
| `0x0009` | Commit Offset | Client → Node | Client commits a consumer offset for a group/topic |
| `0x0010` | Fetch Offset | Client → Node | Client retrieves a previously committed offset for a group/topic |

#### Client Produce (`0x0006`) Payload Details

The produce request now supports **message batching** — multiple entries can be sent in a single request (up to a maximum of 100 entries per batch):

```
┌───────────┬─────────┬──────────────┬──────────┬───────────────────────────────────────────────┐
│ 2B opcode │ 2B from │ 4B topicLen  │  topic   │ 4B entriesCount + loop([4B payloadLen][payload])│
└───────────┴─────────┴──────────────┴──────────┴───────────────────────────────────────────────┘
```

* **topicLen** (`4B int32`): Length of the topic name.
* **topic** (`topicLen` bytes): The topic name to write to.
* **entriesCount** (`4B int32`): Number of log entries in this batch (max 100).
* For each entry in the batch:
  * **payloadLen** (`4B int32`): Length of the entry payload.
  * **payload** (`payloadLen` bytes): The log entry value.

#### Append Entries (`0x0007`) Payload Details

The `0x0007` payload structure is:
```
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬───────────┬───────────────────────────┐
│ 4B leadTerm │ 4B prevIdx  │ 4B prevTerm │ 4B commitIdx│ 4B topicLen │    topic    │ 2B entryNum│ (entryNum * entry structs)│
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴───────────┴───────────────────────────┘
```
Where each entry struct is:
```
┌──────────────┬─────────────┬──────────────┐
│ 8B timestamp │ 4B entryLen │ entryPayload │
└──────────────┴─────────────┴──────────────┘
```

* **leadTerm** (`4B int32`): Leader's current term.
* **prevIdx** (`4B int32`): Index of log entry immediately preceding new ones.
* **prevTerm** (`4B int32`): Term of `prevIdx` entry.
* **commitIdx** (`4B int32`): Leader's commit index.
* **topicLen** (`4B int32`): Length of the topic name.
* **topic** (`topicLen` bytes): The topic name this entry belongs to.
* **entryNum** (`2B int16`): Number of log entries being sent.
* **timestamp** (`8B int64`): Millisecond Unix timestamp of when the entry was created.
* **entryLen** (`4B int32`): The log command string length.
* **entryPayload** (`entryLen` bytes): The log command string.

#### Client Consume Request (`0x0008`) Payload Details

The `0x0008` request payload structure is:
```
┌──────────────┬──────────────┬───────────────────┬──────────────┐
│ 4B topicLen  │    topic     │ 8B startOffset    │ 4B sizeLimit │
└──────────────┴──────────────┴───────────────────┴──────────────┘
```

* **topicLen** (`4B int32`): Length of the topic name.
* **topic** (`topicLen` bytes): The topic name to consume from.
* **startOffset** (`8B int64`): The starting relative offset of log entries to retrieve.
* **sizeLimit** (`4B int32`): Maximum number of entries to return.

The response to `0x0008` starts with a 2-byte response code:
* **Success (`0x0001`)** response layout:

  ```
  [2B status success][4B entryCount](entryCount * [8B timestamp][4B payloadLen][payload])
  ```
  * **status** (`2B uint16`): Success status code (`0x0001`).
  * **entryCount** (`4B int32`): Number of log entries being returned.
  * **timestamp** (`8B int64`): Timestamp of a log entry.
  * **payloadLen** (`4B uint32`): Length of the log payload.
  * **payload**: The string value of the entry's payload.
* **Failure (`0x0002`)** response layout:
  ```
    [2B status code]
  ```
  * **status** (`2B uint16`): Failure status code (`0x0002`).

#### Commit Offset (`0x0009`) Payload Details

```
┌──────────────────┬────────────┬──────────────┬──────────┬──────────────────┐
│ 4B groupIdLen    │  groupId   │ 4B topicLen  │  topic   │ 8B offsetCommit  │
└──────────────────┴────────────┴──────────────┴──────────┴──────────────────┘
```

* **groupIdLen** (`4B int32`): Length of the consumer group id.
* **groupId** (`groupIdLen` bytes): Consumer group identifier.
* **topicLen** (`4B int32`): Length of the topic name.
* **topic** (`topicLen` bytes): Topic name.
* **offsetCommit** (`8B int64`): Offset value to commit for this group+topic.

Response is a 2-byte status code.

#### Fetch Offset (`0x0010`) Payload Details

```
┌──────────────────┬────────────┬──────────────┬──────────┐
│ 4B groupIdLen    │  groupId   │ 4B topicLen  │  topic   │
└──────────────────┴────────────┴──────────────┴──────────┘
```

* **groupIdLen** (`4B int32`): Length of the consumer group id.
* **groupId** (`groupIdLen` bytes): Consumer group identifier.
* **topicLen** (`4B int32`): Length of the topic name.
* **topic** (`topicLen` bytes): Topic name.

The response to `0x0010` starts with a 2-byte response code followed by the offset:
```
[2B status success][8B offset]
```

### Response / Status Codes (Common)

| Code | Value | Meaning |
|---|---|---|
| Success / Granted | `0x0001` | Vote granted, log write/append succeeded, or consume succeeded |
| Failure / Denied | `0x0002` | Vote denied, log write/append failed, or consume failed |

---

## State Persistence

### Volatile State (`saveStates` / `loadStates`)

`currentTerm` and `votedFor` are persisted to disk as a compact 6-byte **binary file** (`data/<port>/states.bin`):

```
┌──────────────────┬────────────────┐
│ 4B currentTerm   │ 2B votedFor    │
└──────────────────┴────────────────┘
```

Unlike the previous implementation that used a JSON ticker goroutine running every 10 seconds, `saveStates` is now a **synchronous, on-demand call** invoked at every state-change point that matters for correctness:

| When `saveStates` is called |
|---|
| A node votes for itself at the start of an election |
| A node grants a vote to a remote candidate |
| A follower's `Append` call detects that the leader's term is newer than its own |

This ensures volatile state is durable at each critical transition rather than on a best-effort periodic schedule.

### Offset Snapshot (`saveSnapshot` / `loadSnapshot`)

The `Broker`'s consumer-group offset map is still persisted periodically (every 10s) to `data/<port>/offsets_sanpshot.json` by the `broker.saveSnapshot` goroutine launched from `NewNode`.

---

## Log Persistence (WAL & Indexing)

The node implements segmented log persistence with accompanying index files to optimize retrieval:

1. **Log Segments**: Per-topic log files are stored on disk inside `data/<port>/log/<topic>/` as `<offset>.log` files (e.g. `00000000000000000000.log`). Each file represents a log segment with a maximum size limit of 1MB.
2. **Index Files**: For each segment, a companion `.index` file (e.g. `<offset>.index`) is created. It stores 16-byte index entries with the binary layout:
    ```
   [8B relativeOffset][8B byteOffset]
    ```
   This is used to perform high-performance binary search lookups mapping a target relative offset directly to a byte offset position in the corresponding segment log file.
3. **Log Disk Format**:
   Each entry written to a `.log` file has the following binary layout:
   ```
   [8B timestamp][4B payloadLen][payload]
   ```
4. **Startup Recovery**:
   When a node starts up, it creates per-topic directories under `data/<port>/log/<topic>/`. In-memory entry recovery from disk segments is not yet re-implemented for the multi-topic layout.

---

## Graceful Shutdown

`node.Shutdown()` performs an ordered teardown:
1. Closes `stopCh`, signalling the event loop and snapshot goroutine to exit.
2. Closes the TCP listener to unblock `Accept()`.
3. Calls `pool.Close()` to close all persistent peer connections.
4. Calls `broker.Close()` to flush and close all open log segment files.
5. Stops the election timer and heartbeat ticker.

---

## Current Limitations & Future Improvements

* **In-Memory Volatile Metadata** — `currentTerm` and `votedFor` are now persisted on every state-change, but in-memory log entries (`[]Entry`) are not yet recovered from disk segments on restart.
* **No RPC retry** — Dial failures via `PeerPool.Send` are printed but silently skipped.
* **Single-response produce** — The server sends one success/failure response after the entire batch is processed; partial batch failures are not distinguished.
