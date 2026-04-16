# Distributed Real-Time Drawing Board with Mini-RAFT Consensus

This project is a distributed real-time drawing board built with a WebSocket gateway, multiple replica nodes, and a Mini-RAFT style consensus flow for leader election, log replication, failover, and replay.

Users draw on a shared canvas in the browser. Drawing events are forwarded through the gateway to the current leader replica, replicated across the cluster, and then broadcast back to all connected clients after commit.

## Features

- Real-time collaborative drawing over WebSockets
- Mini-RAFT style leader election with follower, candidate, and leader states
- Majority-based log replication and commit
- Catch-up synchronization for rejoining replicas using `/sync-log`
- Shared canvas replay from the committed RAFT log
- Global clear event stored in the log
- Dashboard showing leader, role, term, log length, commit index, blocked peers, and node health
- Network partition simulation with leader isolation and heal controls
- Vector-based undo/redo using compensating log events
- Fourth replica support as an implemented bonus feature
- Bind-mounted Docker development setup with watch-based reload

## Project Structure

```text
frontend/
  index.html        UI layout
  app.js            canvas logic, playback, undo/redo, dashboard, controls
  style.css         frontend styling

gateway/
  server.js         WebSocket gateway, leader routing, dashboard/playback APIs
  package.json      gateway dependencies and scripts

replica/
  common/
    config.js       election and RPC timing config
    election.js     RAFT-lite election/state-machine logic
    graceful.js     graceful shutdown helpers
    logger.js       replica logging
    replicaServer.js main replica RPC server and replication logic
    state.js        in-memory replica state
  replica1/server.js
  replica2/server.js
  replica3/server.js
  replica4/server.js
  package.json      replica dependencies and scripts

docker-compose.yml  multi-service local cluster
Dockerfile          shared Node image build
```

## Architecture Overview

### Frontend

- Captures drawing input from mouse and touch
- Sends drawing events to the gateway through WebSocket
- Renders committed events received from the gateway
- Provides playback, undo/redo, partition simulation, and dashboard controls

### Gateway

- Accepts browser WebSocket connections
- Forwards client events to the active leader replica
- Re-routes automatically if the leader changes
- Broadcasts committed events to all connected clients
- Exposes:
  - `/cluster-status`
  - `/playback-log`
  - partition control endpoints
  - replica stop control endpoint routing

### Replicas

Each replica maintains:

- node role
- current term
- voted-for state
- leader ID
- append-only log
- commit index
- peer list

Replica RPC endpoints include:

- `/request-vote`
- `/heartbeat`
- `/append-entries`
- `/sync-log`
- `/client-request`

## How It Works

### Drawing Flow

1. A user draws on the frontend canvas.
2. The frontend sends stroke events to the gateway over WebSocket.
3. The gateway forwards the event to the current leader replica.
4. The leader appends the event to its local log.
5. The leader replicates the event to followers.
6. After majority acknowledgment, the entry is committed.
7. The leader notifies the gateway.
8. The gateway broadcasts the committed event to all clients.

### Leader Election

- Followers wait for heartbeats.
- If heartbeats are missed, a follower becomes a candidate.
- The candidate increments the term, votes for itself, and requests votes.
- A node becomes leader only after majority support.
- Higher term always wins.

### Replay

- The frontend fetches the committed log through the gateway.
- Replay rebuilds the visible drawing by executing committed entries in order.
- Replay currently starts from the beginning of the committed history.

### Undo / Redo

- Stroke segments from one drag action are grouped into a logical stroke group.
- Undo and redo do not rewrite history.
- Instead, the system appends compensation events (`undo` / `redo`) to the log.
- The canvas state is derived from the committed event history.

### Network Partition Simulation

- `Isolate Leader` simulates a partition where the current leader is cut off from the majority.
- The majority side can elect a new valid leader.
- The isolated side cannot make progress without quorum.
- `Heal Network` removes the simulated partition.

## Running the Project

### Prerequisites

- Docker
- Docker Compose

### Start the Cluster

```bash
docker compose up --build
```

Then open the frontend in your browser. If you are serving the static frontend separately, use that local frontend URL. In many local setups this is served through a local static server such as:

```text
http://localhost:5500
```

The gateway services run on:

- HTTP API: `http://localhost:3000`
- WebSocket: `ws://localhost:8080`

Replicas run on:

- `http://localhost:5001`
- `http://localhost:5002`
- `http://localhost:5003`
- `http://localhost:5004`

## Useful Endpoints

### Cluster Status

```text
GET http://localhost:3000/cluster-status
```

### Playback Log

```text
GET http://localhost:3000/playback-log
```

### Replica Log Inspection

```text
GET http://localhost:5001/sync-log
GET http://localhost:5002/sync-log
GET http://localhost:5003/sync-log
GET http://localhost:5004/sync-log
```

### Partition Controls

```text
POST http://localhost:3000/partition/isolate-leader
POST http://localhost:3000/partition/heal
```

## Demo Checklist

Recommended demo flow:

1. Open the app in multiple browser tabs.
2. Draw in one tab and show that strokes appear in the others.
3. Show the dashboard leader, terms, and log lengths.
4. Click `Replay` to rebuild the drawing from the log.
5. Use `Undo` and `Redo` to show compensating log events.
6. Click `Isolate Leader` to simulate a partition.
7. Show leader change on the majority side.
8. Click `Heal Network` to recover the cluster.
9. Optionally inspect `/sync-log` on multiple replicas to show consistent history.

## Notes

- The replicated log is stored in memory inside each replica process.
- Restarting replica processes clears in-memory state unless the process is still alive and only logically stopped.
- The frontend `Stop Node` control simulates a logical node stop at the application level; it does not kill the Docker container.

## Tech Stack

- Node.js
- Express
- WebSockets (`ws`)
- Docker / Docker Compose
- HTML / CSS / JavaScript

## Current Status

Implemented:

- frontend drawing board
- gateway routing and broadcasting
- RAFT-lite election and replication
- replay from committed log
- dashboard
- network partition simulation
- fourth replica
- vector-based undo/redo using log compensation

## License

This project was created for academic/distributed systems coursework.
