const express = require("express");
const axios = require("axios");
const { createState } = require("../common/state");
const {
  resetElectionTimer,
  handleVoteRequest,
  handleHeartbeat
} = require("../common/election");
const { log } = require("../common/logger");

const app = express();
app.use(express.json());

const state = createState("replica2");

state.peers = [
  "http://localhost:5001",
  "http://localhost:5003"
];

const PORT = 5002;

// ---------------- HELPER ----------------
function getPortFromNodeId(nodeId) {
  if (nodeId === "replica1") return 5001;
  if (nodeId === "replica2") return 5002;
  if (nodeId === "replica3") return 5003;
}

// ---------------- SYNC LOG (FIXED) ----------------
async function syncWithLeader() {
  if (!state.leaderId) {
    console.log("⏳ No leader yet, retrying...");
    setTimeout(syncWithLeader, 2000);
    return;
  }

  try {
    const leaderUrl = `http://localhost:${getPortFromNodeId(state.leaderId)}`;
    const res = await axios.get(`${leaderUrl}/sync-log`);

    state.log = res.data.log || [];
    console.log("🔄 Synced logs from leader");
  } catch (err) {
    console.log("❌ Sync failed, retrying...");
    setTimeout(syncWithLeader, 2000);
  }
}

// ---------------- STATUS ----------------
app.get("/status", (req, res) => {
  res.json({
    nodeId: state.nodeId,
    state: state.state,
    currentTerm: state.currentTerm,
    votedFor: state.votedFor,
    leaderId: state.leaderId,
    log: state.log
  });
});

// ---------------- RAFT ----------------
app.post("/request-vote", (req, res) => {
  res.json(handleVoteRequest(state, state.peers, req.body));
});

app.post("/heartbeat", (req, res) => {
  res.json(handleHeartbeat(state, state.peers, req.body));
});

// ---------------- APPEND ENTRIES ----------------
app.post("/append-entries", (req, res) => {
  const { term, leaderId, entry } = req.body;

  if (term < state.currentTerm) {
    return res.json({ success: false });
  }

  state.currentTerm = term;
  state.leaderId = leaderId;
  state.state = "FOLLOWER";

  state.log.push(entry);

  console.log("📥 Received log:", entry);

  res.json({ success: true });
});

// ---------------- REPLICATION ----------------
async function replicateToFollowers(entry) {
  let successCount = 1;

  for (const peer of state.peers) {
    try {
      const res = await axios.post(`${peer}/append-entries`, {
        term: state.currentTerm,
        leaderId: state.nodeId,
        entry
      });

      if (res.data.success) successCount++;
    } catch (err) {
      console.log("❌ Failed:", peer);
    }
  }

  return successCount;
}

// ---------------- CLIENT REQUEST ----------------
app.post("/client-request", async (req, res) => {
  if (state.state !== "LEADER") {
    return res.json({ error: "Not leader" });
  }

  const entry = req.body;

  const successCount = await replicateToFollowers(entry);
  const majority = Math.floor((state.peers.length + 1) / 2) + 1;

  if (successCount >= majority) {
    state.log.push(entry);
    state.commitIndex++;

    console.log("✅ Committed:", entry);
    return res.json({ success: true });
  }

  res.json({ success: false });
});

// ---------------- SYNC ENDPOINT ----------------
app.get("/sync-log", (req, res) => {
  res.json({ log: state.log });
});

// ---------------- START ----------------
app.listen(PORT, () => {
  console.log(`[replica1] running on port ${PORT}`);
  log(state, `Server started on port ${PORT}`);
  resetElectionTimer(state, state.peers);

  // ✅ Start sync process
  setTimeout(syncWithLeader, 3000);
});