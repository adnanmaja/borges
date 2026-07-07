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

The client (located in `./client`) supports both log entry production and consumption. To run the client:

```bash
go run ./client -port 8080
```

Depending on the configuration in `client.go`, this will either:
* **Produce** a new log entry (`"hii"`) to the node.
* **Consume** log entries from the node starting at a specific offset.

---

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
├──────────────────┬──────────────────┬──────────────────┬──────────────────┐
│                  │                  │                  │                  │
▼                  ▼                  ▼                  ▼                  ▼
┌───────────┐ ┌─────────┐ ┌─────────────┐ ┌─────────────┐
│heartbeat.go│ │election.go│ │listener.go  │ │   log.go    │
│Send        │ │Election  │ │TCP server   │ │Log write &  │
│heartbeats  │ │campaign  │ │& dispatcher │ │replication  │
└───────────┘ └─────────┘ └──────┬──────┘ └──────┬──────┘
                                 │               │
                                 └──────┬────────┘
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
| `node.go` | `Node` & `Entry` structs, `NewNode()` constructor, `startLoop()` event loop |
| `heartbeat.go` | Leader sends heartbeats |
| `election.go` | Election campaign (`ElectItself`), vote request/response, victory announcement |
| `listener.go` | TCP listener, command/opcode parsing, and dispatching client reads/writes and raft requests |
| `log.go` | Log manager (`log` struct), write/append functions, load from disk, wire-level log transmission, and disk log file management |
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
	heartbeatTick <-chan time.Time // fires every 5s — Leader sends heartbeats
	electionTimer *time.Timer      // fires on timeout — triggers election
	lastHeartbeat time.Time

	logs        []Entry           // replicated log entries (1-based index)
	commitIndex int32             // index of highest log entry known to be committed
	nextIndex   map[int16]int32   // for each server, index of the next log entry to send
	matchIndex  map[int16]int32   // for each server, index of highest log entry known to be replicated
	log         *log              // log persistence manager (handles segments/index)

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

The main loop runs in `node.go` and multiplexes on two channels:

| Channel | When it fires | Action |
|---|---|---|
| `heartbeatTick` | Every 5 seconds | If **Leader**, broadcast `Heartbeat()` to all peers |
| `electionTimer.C` | After random timeout (8–18s) | If not Leader, call `ElectItself()` |

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
| `0x0006` | Client Log Write | Client → Node | `4B` payload len + `payload` | Client sends new log command to node |
| `0x0007` | Append Entries | Leader → Follower | See detailed payload below | Leader replicates log entries to followers |
| `0x0008` | Client Consume Request | Client → Node | `8B` relative offset | Client requests a log entry by offset |

#### Append Entries (`0x0007`) Payload Details

The `0x0007` payload structure is:
```
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬───────────────────────────┐
│ 4B leadTerm │ 4B prevIdx  │ 4B prevTerm │ 4B commitIdx│ 2B entryNum │ (entryNum * entry structs)│
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴───────────────────────────┘
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
* **entryNum** (`2B int16`): Number of log entries being sent (can be 0 for heartbeat/probe, but currently heartbeat uses `0x0004`).
* **timestamp** (`8B int64`): Millisecond Unix timestamp of when the entry was created.
* **entryLen** (`4B int32`): The log command string length.
* **entryPayload** (`entryLen` bytes): The log command string.

#### Client Consume Request (`0x0008`) Payload Details

The `0x0008` request payload structure is:
```
┌────────────────────┐
│ 8B relativeOffset  │
└────────────────────┘
```

* **relativeOffset** (`8B int64`): The target relative index/offset of the log entry to retrieve from disk.

The response to `0x0008` starts with a 2-byte response code:
* **Success (`0x0001`)** response layout:
  ```
  ┌─────────────┬──────────────┬───────────────┬────────────────────────┐
  │ 2B status   │ 8B timestamp │ 4B payloadLen │ payload (payloadLen B) │
  └─────────────┴──────────────┴───────────────┴────────────────────────┘
  ```
  * **status** (`2B uint16`): Success status code (`0x0001`).
  * **timestamp** (`8B int64`): Timestamp of the stored log entry.
  * **payloadLen** (`4B uint32`): Length of the log payload.
  * **payload**: The string value of the entry's payload.
* **Failure (`0x0002`)** response layout:
  ```
  ┌─────────────┐
  │ 2B status   │
  └─────────────┘
  ```
  * **status** (`2B uint16`): Failure status code (`0x0002`).

### Response / Status Codes (Common)

| Code | Value | Meaning |
|---|---|---|
| Success / Granted | `0x0001` | Vote granted, log write/append succeeded, or consume succeeded |
| Failure / Denied | `0x0002` | Vote denied, log write/append failed, or consume failed |

---

## Log Persistence (WAL & Indexing)

The node implements segmented log persistence with accompanying index files to optimize retrieval:

1. **Log Segments**: Node logs are stored on disk inside `data/<port>/log/` as `<offset>.log` files (e.g. `00000000000000000000.log`). Each file represents a log segment with a maximum size limit of 1MB.
2. **Index Files**: For each segment, a companion `.index` file (e.g. `<offset>.index`) is created. It stores 16-byte index entries with the binary layout:
   ```
   ┌─────────────────────┬─────────────────────┐
   │  8B relativeOffset  │  8B absoluteOffset  │
   └─────────────────────┴─────────────────────┘
   ```
   This is used to perform high-performance binary search lookups mapping a target relative offset directly to a byte offset position in the corresponding segment log file.
3. **Log Disk Format**:
   Each entry written to a `.log` file has the following binary layout:
   ```
   ┌────────────────┬─────────────────┬──────────────┐
   │  8B timestamp  │  4B payloadLen  │   payload    │
   └────────────────┴─────────────────┴──────────────┘
   ```
4. **Startup Recovery**:
   When a node starts up, it reads existing segments from the disk via `loadEntriesFromDisk()` to populate the in-memory logs slice. Note: Directory setup and initialization logs are configured under `logs/<port>/` at initialization.

---

## Current Limitations & Future Improvements

* **In-Memory Volatile Metadata** — Node terms and election state (`votedFor`, `currentTerm`) are stored only in memory. A node restart resets these, preventing a complete crash recovery.
* **Incomplete Term Validation in Voting** — Term verification is not implemented in the voting process.
* **Connection Leaks** — `Heartbeat` broadcasts defer closing connections in a loop, keeping connections open longer than necessary.
* **No RPC retry** — Dial failures are printed but silently skipped.
* **Simplistic Vote Counting** — Election victory triggers on the first positive response (`voteCount >= 1`) rather than verifying a true majority of live nodes.
* **Topics**
