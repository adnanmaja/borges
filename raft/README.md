# Raft Consensus Implementation

A minimal [Raft](https://raft.github.io/) consensus protocol implementation over TCP in Go.

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

To send a log entry (e.g., `"hii"`) to a node:

```bash
go run ./client -port 8080
```

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
└───────────┘ └─────────┘ └─────────────┘ └─────────────┘
```

### File Responsibilities

| File | Role |
|---|---|
| `main.go` | CLI flag parsing, node instantiation |
| `node.go` | `Node` & `Entry` structs, `NewNode()` constructor, `startLoop()` event loop |
| `heartbeat.go` | Leader sends heartbeats |
| `election.go` | Election campaign (`ElectItself`), vote request/response, victory announcement |
| `listener.go` | TCP listener, command/opcode parsing and dispatch |
| `log.go` | Log write / append functions, concurrent log replication, and Raft consistency validation |

---

## Node State

```go
type Entry struct {
	payload string
	term    int32
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
| `0x0006` | Client Log Write | Client → Node | `4B` entry len + `entry` | Client sends new log command to node |
| `0x0007` | Append Entries | Leader → Follower | See detailed payload below | Leader replicates log entries to followers |

#### Append Entries (`0x0007`) Payload Details

The `0x0007` payload structure is:
```
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬───────────────────────────┐
│ 4B leadTerm │ 4B prevIdx  │ 4B prevTerm │ 4B commitIdx│ 2B entryNum │ (entryNum * entry structs)│
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴───────────────────────────┘
```
Where each entry struct is:
```
┌─────────────┬──────────────┐
│ 4B entryLen │ entryPayload │
└─────────────┴──────────────┘
```

- **leadTerm** (`4B int32`): Leader's current term.
- **prevIdx** (`4B int32`): Index of log entry immediately preceding new ones.
- **prevTerm** (`4B int32`): Term of `prevIdx` entry.
- **commitIdx** (`4B int32`): Leader's commit index.
- **entryNum** (`2B int16`): Number of log entries being sent (can be 0 for heartbeat/probe, but currently heartbeat uses `0x0004`).
- **entryPayload** (`entryLen` bytes): The log command string.

### Response / Status Codes

| Code | Value | Meaning |
|---|---|---|
| Success / Granted | `0x0001` | Vote granted or log write/append succeeded |
| Failure / Denied | `0x0002` | Vote denied or log write/append failed |

---

## Current Limitations & Future Improvements

- **No persistent state** — term, votedFor, and log are in-memory only.
- **Incomplete Term Validation in Voting** — term verification is not implemented in the voting process.
- **Crash recovery** — node restart loses all state.
- **Connection Leaks** — `Heartbeat` broadcasts defer closing connections in a loop, keeping connections open longer than necessary.
- **No RPC retry** — dial failures are printed but silently skipped.
- **Simplistic Vote Counting** — election victory triggers on the first positive response (`voteCount >= 1`) rather than verifying a true majority of live nodes.
