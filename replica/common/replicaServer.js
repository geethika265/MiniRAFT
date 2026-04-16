const express = require("express");
const axios = require("axios");

const { createState } = require("./state");
const {
  becomeFollower,
  getStatus,
  handleVoteRequest,
  handleHeartbeat,
  resetElectionTimer
} = require("./election");
const { registerGracefulShutdown } = require("./graceful");
const { log } = require("./logger");
const config = require("./config");

function getMajority(state) {
  return Math.floor((state.peers.length + 1) / 2) + 1;
}

function peerIdFromUrl(peerUrl) {
  const match = String(peerUrl || "").match(/replica\d+/);
  return match ? match[0] : peerUrl;
}

function isPeerBlocked(state, peerOrId) {
  const peerId = peerIdFromUrl(peerOrId);
  return state.blockedPeerIds && state.blockedPeerIds.has(peerId);
}

function hasQuorum(state) {
  const reachablePeers = state.peers.filter((peer) => !isPeerBlocked(state, peer)).length;
  return reachablePeers + 1 >= getMajority(state);
}

function blockedResponse(state, sourcePeerId) {
  return {
    success: false,
    voteGranted: false,
    term: state.currentTerm,
    leaderId: state.leaderId,
    error: `Partitioned from ${sourcePeerId}`
  };
}

function getEntryTerm(entry) {
  return entry ? entry.term : null;
}

function buildLogResponse(state) {
  return {
    log: state.log,
    commitIndex: state.commitIndex,
    leaderId: state.leaderId,
    term: state.currentTerm
  };
}

async function notifyGateway(entry, state) {
  const gatewayUrl = process.env.GATEWAY_URL;

  if (!gatewayUrl) {
    return;
  }

  try {
    await axios.post(
      `${gatewayUrl}/commit`,
      {
        ...entry,
        committedBy: state.nodeId,
        commitIndex: state.commitIndex,
        term: state.currentTerm
      },
      { timeout: config.RPC_TIMEOUT }
    );
  } catch (error) {
    log(state, `Failed to notify gateway at ${gatewayUrl}`);
  }
}

async function replicateCommitIndex(state) {
  const committedEntry = state.log[state.commitIndex];

  if (!committedEntry) {
    return;
  }

  for (const peer of state.peers) {
    if (isPeerBlocked(state, peer)) {
      log(state, `Commit update blocked by partition for ${peerIdFromUrl(peer)}`);
      continue;
    }

    try {
      const response = await axios.post(
        `${peer}/append-entries`,
        {
          term: state.currentTerm,
          leaderId: state.nodeId,
          entry: null,
          prevLogIndex: state.commitIndex,
          prevLogTerm: getEntryTerm(committedEntry),
          leaderCommit: state.commitIndex
        },
        { timeout: config.RPC_TIMEOUT }
      );

      if (response.data.term > state.currentTerm) {
        becomeFollower(
          state,
          state.peers,
          response.data.term,
          response.data.leaderId || null
        );
        return;
      }
    } catch (error) {
      log(state, `Commit update failed for ${peer}`);
    }
  }
}

async function syncPeerFromIndex(state, peer, fromIndex) {
  const safeFromIndex = Math.max(0, Number.isInteger(fromIndex) ? fromIndex : 0);
  const entries = state.log.slice(safeFromIndex, state.commitIndex + 1);

  if (entries.length === 0 && state.commitIndex < safeFromIndex) {
    return true;
  }

  try {
    if (isPeerBlocked(state, peer)) {
      log(state, `Sync blocked by partition for ${peerIdFromUrl(peer)}`);
      return false;
    }

    const response = await axios.post(
      `${peer}/sync-log`,
      {
        term: state.currentTerm,
        leaderId: state.nodeId,
        fromIndex: safeFromIndex,
        entries,
        commitIndex: state.commitIndex
      },
      { timeout: config.RPC_TIMEOUT }
    );

    if (response.data.term > state.currentTerm) {
      becomeFollower(
        state,
        state.peers,
        response.data.term,
        response.data.leaderId || null
      );
      return false;
    }

    log(state, `Synced peer ${peer} from index ${safeFromIndex}`);
    return response.data.success !== false;
  } catch (error) {
    log(state, `Failed to sync peer ${peer} from index ${safeFromIndex}`);
    return false;
  }
}

async function syncFromLeader(state) {
  for (const peer of state.peers) {
    if (isPeerBlocked(state, peer)) {
      continue;
    }

    try {
      const leaderRes = await axios.get(`${peer}/who-is-leader`, {
        timeout: config.RPC_TIMEOUT
      });
      const leaderId = leaderRes.data.leaderId;
      const isLeader = leaderRes.data.isLeader;

      if (!leaderId && !isLeader) {
        continue;
      }

      const leaderUrl = isLeader
        ? peer
        : state.peers.find((candidate) => candidate.includes(leaderId));

      if (!leaderUrl || leaderId === state.nodeId) {
        continue;
      }

      const syncRes = await axios.get(`${leaderUrl}/sync-log`, {
        params: { fromIndex: state.log.length },
        timeout: config.RPC_TIMEOUT
      });

      const data = syncRes.data;
      const entries = Array.isArray(data.entries) ? data.entries : [];

      if (entries.length > 0) {
        state.log.push(...entries);
      }

      state.commitIndex = Math.min(
        data.commitIndex ?? state.commitIndex,
        state.log.length - 1
      );
      state.currentTerm = Math.max(state.currentTerm, data.term || 0);
      state.leaderId = data.leaderId || leaderId || state.leaderId;

      log(state, `Synced ${entries.length} entries from leader ${state.leaderId}`);
      return;
    } catch (error) {
      // Try another peer.
    }
  }

  log(state, "No leader found for startup sync");
}

async function replicateEntryToPeer(state, peer, entry, entryIndex) {
  const prevLogIndex = entryIndex - 1;
  const prevLogTerm = getEntryTerm(state.log[prevLogIndex]);

  try {
    if (isPeerBlocked(state, peer)) {
      log(state, `Replication blocked by partition for ${peerIdFromUrl(peer)}`);
      return false;
    }

    const response = await axios.post(
      `${peer}/append-entries`,
      {
        term: state.currentTerm,
        leaderId: state.nodeId,
        entry,
        prevLogIndex,
        prevLogTerm,
        leaderCommit: state.commitIndex
      },
      { timeout: config.RPC_TIMEOUT }
    );

    if (response.data.term > state.currentTerm) {
      becomeFollower(
        state,
        state.peers,
        response.data.term,
        response.data.leaderId || null
      );
      return false;
    }

    if (response.data.success) {
      return true;
    }

    if (Number.isInteger(response.data.currentLogLength)) {
      const synced = await syncPeerFromIndex(
        state,
        peer,
        response.data.currentLogLength
      );

      if (!synced || state.state !== "LEADER") {
        return false;
      }

      const retryResponse = await axios.post(
        `${peer}/append-entries`,
        {
          term: state.currentTerm,
          leaderId: state.nodeId,
          entry,
          prevLogIndex,
          prevLogTerm,
          leaderCommit: state.commitIndex
        },
        { timeout: config.RPC_TIMEOUT }
      );

      if (retryResponse.data.term > state.currentTerm) {
        becomeFollower(
          state,
          state.peers,
          retryResponse.data.term,
          retryResponse.data.leaderId || null
        );
        return false;
      }

      return retryResponse.data.success === true;
    }
  } catch (error) {
    log(state, `Replication failed for ${peer}`);
  }

  return false;
}

function appendIfNeeded(state, entry, prevLogIndex) {
  const nextIndex = prevLogIndex + 1;
  const existingEntry = state.log[nextIndex];

  if (!entry) {
    return true;
  }

  if (!existingEntry) {
    state.log.push(entry);
    log(state, `Appended entry at index ${nextIndex}`);
    return true;
  }

  if (existingEntry.term !== entry.term) {
    if (nextIndex <= state.commitIndex) {
      log(state, `Rejected overwrite attempt at committed index ${nextIndex}`);
      return false;
    }

    state.log = state.log.slice(0, nextIndex);
    state.log.push(entry);
    log(state, `Replaced divergent entry at index ${nextIndex}`);
  }

  return true;
}

function createReplicaServer({ nodeId, port, peers }) {
  const app = express();
  app.use(express.json());

  const state = createState(nodeId);
  state.peers = peers;

  app.get("/status", (req, res) => {
    res.json({
      ...getStatus(state),
      logLength: state.log.length,
      commitIndex: state.commitIndex,
      blockedPeers: Array.from(state.blockedPeerIds || [])
    });
  });

  app.get("/who-is-leader", (req, res) => {
    const status = getStatus(state);
    res.json({
      nodeId: status.nodeId,
      isLeader: status.isLeader,
      leaderId: status.leaderId,
      currentTerm: status.currentTerm
    });
  });

  app.get("/sync-log", (req, res) => {
    if (state.stopped) {
      return res.status(503).json({
        error: "Node stopped",
        leaderId: state.leaderId,
        term: state.currentTerm
      });
    }

    const fromIndex = Math.max(0, Number(req.query.fromIndex || 0));

    res.json({
      entries: state.log.slice(fromIndex, state.commitIndex + 1),
      fromIndex,
      ...buildLogResponse(state)
    });
  });

  app.post("/request-vote", (req, res) => {
    const candidateId = req.body?.candidateId;
    if (candidateId && isPeerBlocked(state, candidateId)) {
      return res.status(503).json(blockedResponse(state, candidateId));
    }

    res.json(handleVoteRequest(state, state.peers, req.body));
  });

  app.post("/heartbeat", (req, res) => {
    const leaderId = req.body?.leaderId;
    if (leaderId && isPeerBlocked(state, leaderId)) {
      return res.status(503).json(blockedResponse(state, leaderId));
    }

    res.json(handleHeartbeat(state, state.peers, req.body));
  });

  app.post("/append-entries", (req, res) => {
    if (state.stopped) {
      return res.status(503).json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        error: "Node stopped"
      });
    }

    const {
      term,
      leaderId,
      entry,
      prevLogIndex = -1,
      prevLogTerm = null,
      leaderCommit = state.commitIndex
    } = req.body;

    if (leaderId && isPeerBlocked(state, leaderId)) {
      return res.status(503).json(blockedResponse(state, leaderId));
    }

    if (typeof term !== "number" || !leaderId) {
      return res.status(400).json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        error: "Invalid append request"
      });
    }

    if (term < state.currentTerm) {
      return res.json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId
      });
    }

    becomeFollower(state, state.peers, term, leaderId);

    if (prevLogIndex >= state.log.length) {
      return res.json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        currentLogLength: state.log.length
      });
    }

    if (prevLogIndex >= 0 && getEntryTerm(state.log[prevLogIndex]) !== prevLogTerm) {
      log(state, `Log mismatch at index ${prevLogIndex}`);
      return res.json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        currentLogLength: state.log.length
      });
    }

    const appended = appendIfNeeded(state, entry, prevLogIndex);

    if (!appended) {
      return res.json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        currentLogLength: state.log.length
      });
    }

    const nextCommit = Math.min(leaderCommit, state.log.length - 1);
    if (nextCommit > state.commitIndex) {
      state.commitIndex = nextCommit;
      log(state, `Updated commitIndex to ${state.commitIndex}`);
    }

    return res.json({
      success: true,
      term: state.currentTerm,
      leaderId: state.leaderId,
      currentLogLength: state.log.length
    });
  });

  app.post("/sync-log", (req, res) => {
    if (state.stopped) {
      return res.status(503).json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        error: "Node stopped"
      });
    }

    const {
      term,
      leaderId,
      fromIndex = 0,
      entries = [],
      commitIndex = state.commitIndex
    } = req.body;

    if (leaderId && isPeerBlocked(state, leaderId)) {
      return res.status(503).json(blockedResponse(state, leaderId));
    }

    if (typeof term !== "number" || !leaderId || !Array.isArray(entries)) {
      return res.status(400).json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId,
        error: "Invalid sync request"
      });
    }

    if (term < state.currentTerm) {
      return res.json({
        success: false,
        term: state.currentTerm,
        leaderId: state.leaderId
      });
    }

    becomeFollower(state, state.peers, term, leaderId);

    const safeFromIndex = Math.max(0, Number(fromIndex || 0));
    state.log = state.log.slice(0, safeFromIndex);
    state.log.push(...entries);
    state.commitIndex = Math.min(commitIndex, state.log.length - 1);

    log(
      state,
      `Applied sync from ${leaderId} with ${entries.length} entries starting at ${safeFromIndex}`
    );

    return res.json({
      success: true,
      term: state.currentTerm,
      leaderId: state.leaderId,
      currentLogLength: state.log.length,
      commitIndex: state.commitIndex
    });
  });

  app.post("/client-request", async (req, res) => {
    if (state.stopped) {
      return res.status(503).json({
        success: false,
        error: "Node stopped",
        leaderId: state.leaderId,
        term: state.currentTerm
      });
    }

    if (state.state !== "LEADER") {
      return res.status(409).json({
        success: false,
        error: "Not leader",
        leaderId: state.leaderId,
        term: state.currentTerm
      });
    }

    const entry = {
      id: req.body.id || `stroke-${Date.now()}`,
      ...req.body,
      term: state.currentTerm
    };

    state.log.push(entry);
    const entryIndex = state.log.length - 1;
    let successCount = 1;

    for (const peer of state.peers) {
      const replicated = await replicateEntryToPeer(state, peer, entry, entryIndex);

      if (state.state !== "LEADER") {
        return res.status(409).json({
          success: false,
          error: "Stepped down",
          leaderId: state.leaderId,
          term: state.currentTerm
        });
      }

      if (replicated) {
        successCount += 1;
      }
    }

    if (successCount < getMajority(state)) {
      state.log.pop();
      return res.status(503).json({
        success: false,
        error: "Failed to reach majority",
        leaderId: state.nodeId,
        term: state.currentTerm
      });
    }

    state.commitIndex = entryIndex;
    log(state, `Committed entry at index ${state.commitIndex}`);

    await replicateCommitIndex(state);
    await notifyGateway(entry, state);

    return res.json({
      success: true,
      entry,
      commitIndex: state.commitIndex,
      term: state.currentTerm,
      leaderId: state.nodeId
    });
  });

  app.post("/partition", (req, res) => {
    const blockedPeerIds = Array.isArray(req.body?.blockedPeerIds)
      ? req.body.blockedPeerIds.map((peerId) => String(peerId))
      : [];

    state.blockedPeerIds = new Set(blockedPeerIds);

    if (state.state === "LEADER" && !hasQuorum(state)) {
      log(state, "Partition removed quorum for current leader, stepping down");
      becomeFollower(state, state.peers, state.currentTerm, null);
    }

    log(
      state,
      blockedPeerIds.length
        ? `Partition updated. Blocked peers: ${blockedPeerIds.join(", ")}`
        : "Partition healed. No blocked peers."
    );

    return res.json({
      success: true,
      nodeId: state.nodeId,
      blockedPeers: Array.from(state.blockedPeerIds)
    });
  });

  app.post("/control/:action", async (req, res) => {
    const action = req.params.action;

    if (action === "stop") {
      state.stopped = true;
      state.leaderId = null;
      clearElectionTimer(state);
      clearHeartbeatTimer(state);
      state.state = "FOLLOWER";
      state.electionInFlight = false;
      log(state, "Replica stopped via control endpoint");

      return res.json({
        success: true,
        nodeId: state.nodeId,
        state: "STOPPED"
      });
    }

    if (action === "start") {
      state.stopped = false;
      state.state = "FOLLOWER";
      state.votedFor = null;
      state.leaderId = null;
      state.electionInFlight = false;
      log(state, "Replica started via control endpoint");
      await syncFromLeader(state);
      resetElectionTimer(state, state.peers);

      return res.json({
        success: true,
        nodeId: state.nodeId,
        state: state.state
      });
    }

    return res.status(400).json({
      success: false,
      error: "Unknown control action"
    });
  });

  const server = app.listen(port, async () => {
    log(state, `Server started on port ${port}`);
    await syncFromLeader(state);
    resetElectionTimer(state, state.peers);
  });

  registerGracefulShutdown(server, state);

  return { app, server, state };
}

module.exports = { createReplicaServer };
