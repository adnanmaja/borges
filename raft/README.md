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


## Architecture

```
┌─────────────┐
│   main.go   │  Entry point. Parses the -port flag and starts a Node.
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   node.go   │  Node & Entry structs, constructor, and main event loop.
└──────┬──────┘
       │
├──────────────────┬──────────────────┬──────────────────┬──────────────────┬─────────────────┐
│                  │                  │                  │                  │                 │
▼                  ▼                  ▼                  ▼                  ▼                 ▼
┌───────────┐ ┌─────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│heartbeat.go│ │election.go│ │listener.go  │ │  broker.go  │ │   log.go    │
│Send        │ │Election  │ │TCP server   │ │Multi-topic  │ │Log write,   │
│heartbeats  │ │campaign  │ │& dispatcher │ │log registry │ │append, read │
└───────────┘ └─────────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
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
| `node.go` | `Node` & `Entry` structs, `NewNode()` constructor, `startLoop()` event loop, background goroutines for periodic state/snapshot persistence |
| `heartbeat.go` | Leader sends heartbeats |
| `election.go` | Election campaign (`StartElection`), vote request/response, victory announcement (`CountVote`) |
| `listener.go` | TCP listener, command/opcode parsing, and dispatching client reads/writes and raft requests |
| `broker.go` | Multi-topic log registry (`Broker`), maps topic names to `Log` instances, tracks consumer-group offsets (`SaveOffset`/`FetchOffset`), periodic offset snapshot persistence |
| `log.go` | Per-topic log manager (`Log` struct), write/append/read functions, wire-level log transmission, and disk log file management |
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
	port          int16
	role          string            // "Follower", "Candidate", or "Leader"
	currentTerm   int32
	votedFor      int16
	voteCount     int32
	heartbeat     <-chan time.Time // fires every 5s — Leader sends heartbeats
	electionTimer *time.Timer      // fires on timeout — triggers election
	lastHeartbeat time.Time

	entries     []Entry           // replicated log entries (1-based index)
	commitIndex int32             // index of highest log entry known to be committed
	nextIndex   map[int16]int32   // for each server, index of the next log entry to send
	matchIndex  map[int16]int32   // for each server, index of highest log entry known to be replicated
	broker      *Broker           // multi-topic log registry

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

The main loop runs in `node.go` and multiplexes on two channels. On startup, two background goroutines are also launched for periodic disk persistence:

| Channel | When it fires | Action |
|---|---|---|
| `heartbeat` | Every 5 seconds | If **Leader**, broadcast `Heartbeat()` to all peers |
| `electionTimer.C` | After random timeout (8–18s) | If not Leader, call `StartElection()` |

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

| Opcode | Command Name | Direction | Payload | Description |
|---|---|---|---|---|
| `0x0004` | Heartbeat | Leader → Follower | `10B` message text (`"heartbeat!"`) | Periodic keep-alive |
| `0x0005` | Vote Request | Candidate → Peers | *(none beyond header)* | Candidate requests votes |
| `0x0006` | Client Log Write | Client → Node | `4B` topic len + `topic` + `4B` payload len + `payload` | Client sends new log command to a specific topic |
| `0x0007` | Append Entries | Leader → Follower | See detailed payload below | Leader replicates log entries to followers |
| `0x0008` | Client Consume Request | Client → Node | `4B` topic len + `topic` + `8B` start offset + `4B` size limit | Client requests log entries by topic, offset, and count |
| `0x0009` | Commit Offset | Client → Node | `4B` group id len + `group id` + `4B` topic len + `topic` + `8B` offset commit | Client commits a consumer offset for a group/topic |
| `0x0010` | Fetch Offset | Client → Node | `4B` group id len + `group id` + `4B` topic len + `topic` | Client retrieves a previously committed offset for a group/topic |

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
* **entryNum** (`2B int16`): Number of log entries being sent (can be 0 for heartbeat/probe, but currently heartbeat uses `0x0004`).
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

## Background Persistence Goroutines

Two goroutines run in the background from `NewNode()` to periodically persist state to disk:

| Goroutine | Interval | Persists | File |
|---|---|---|---|
| `saveStates` | Every 10s | `currentTerm` and `votedFor` to JSON | `data/<port>/states.json` |
| `saveSnapshot` | Every 10s | `Broker.offsets` (consumer-group offset map) to JSON | `data/<port>/offsets_sanpshot.json` |

On startup, `loadStates()` and `loadSnapshot()` read these files back to restore the last known state.

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

## Current Limitations & Future Improvements

* **In-Memory Volatile Metadata** — Node terms and election state (`votedFor`, `currentTerm`) are persisted to disk every 10s (see [Background Persistence Goroutines](#background-persistence-goroutines)), but recent changes between ticks may be lost on crash.
* **Connection Leaks** — `Heartbeat` broadcasts defer closing connections in a loop, keeping connections open longer than necessary.
* **No RPC retry** — Dial failures are printed but silently skipped.
