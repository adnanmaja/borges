# Raft Consensus Implementation

A minimal [Raft](https://raft.github.io/) consensus protocol implementation over TCP in Go.

## How to Run

Open three terminals and run one instance on each port:

```bash
go run . -port 8080
go run . -port 8081
go run . -port 8082
```

All nodes start as **Candidate**. The first one to timeout and gather a majority of votes becomes the **Leader** and begins sending periodic heartbeats to the others.

---

## Architecture

```
┌─────────────┐
│   main.go   │  Entry point. Parses the -port flag and starts a Node.
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   raft.go   │  Node struct, constructor, and the main event loop.
└──────┬──────┘
       │
├──────────────────┬──────────────────┐
│                  │                  │
▼                  ▼                  ▼
┌───────────┐ ┌─────────┐ ┌─────────────┐
│heartbeat.go│ │election.go│ │listener.go  │
│Send/recv   │ │Vote      │ │TCP server   │
│heartbeats  │ │requests  │ │& dispatcher │
└───────────┘ └─────────┘ └─────────────┘
```

### File Responsibilities

| File | Role |
|---|---|
| `main.go` | CLI flag parsing, node instantiation |
| `raft.go` | `Node` struct, `NewNode()` constructor, `startLoop()` event loop |
| `heartbeat.go` | Leader sends heartbeats, message frame encoding |
| `election.go` | Election campaign (`ElectItself`), vote request/response, leader announcement |
| `listener.go` | TCP listener, command parsing and dispatch |

---

## Node State

```go
type Node struct {
    port        int16
    role        string            // "Follower", "Candidate", or "Leader"
    currentTerm int32
    votedFor    int16
    voteCount   int32
    heartbeatTick <-chan time.Time // fires every 5s — Leader sends heartbeats
    electionTimer *time.Timer      // fires on timeout — triggers election
    lastHeartbeat time.Time
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
        │ Wins majority vote
        ▼
  ┌────────┐
  │ Leader │
  └────────┘
```

---

## Event Loop (`startLoop`)

The main loop runs in `raft.go` and multiplexes on two channels:

| Channel | When it fires | Action |
|---|---|---|
| `heartbeatTick` | Every 5 seconds | If **Leader**, broadcast `Heartbeat()` to all peers |
| `electionTimer.C` | After random timeout (8–18s) | If not Leader, call `ElectItself()` |

---

## Wire Protocol

All messages are TCP frames with a binary layout.

### Common Header

```
┌────────┬─────────┬──────────┐
│ 2B cmd │ 2B from │ (payload)│
└────────┴─────────┴──────────┘
```

### Command Types

| Command | Value | Direction | Payload |
|---|---|---|---|
| Heartbeat | `0x0001` | Leader → Follower | `10B` message text (padded) |
| Vote Request | `0x0002` | Candidate → Peers | *(none beyond header)* |
| Vote Granted | `0x0003` | Peer → Candidate | *(response only, 2B)* |
| Leader Announce | `0x0004` | New Leader → Peers | *(none beyond header)* |

---

## Current Limitations & Future Improvements

- **No persistent state** — term, votedFor, and log are in-memory only
- **No log replication** — heartbeats carry no log entries
- **No split-brain handling** — term comparison is not implemented
- **Crash recovery** — node restart loses all state
- **No RPC retry** — dial failures are silently skipped
- **Vote counting** — `RecapVote` counts replies but doesn't handle duplicates or rejections correctly
- **Random sleep in election** — `time.Sleep` blocks the event loop; should use timer-based backoff instead
