const WebSocket = require("ws");
const axios = require("axios");
const express = require("express");

const WS_PORT = Number(process.env.WS_PORT || 8080);
const HTTP_PORT = Number(process.env.HTTP_PORT || 3000);
const REPLICAS = (process.env.REPLICA_URLS ||
  "http://localhost:5001,http://localhost:5002,http://localhost:5003,http://localhost:5004")
  .split(",")
  .map((url) => url.trim())
  .filter(Boolean);

const wss = new WebSocket.Server({ port: WS_PORT });
const app = express();

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

app.use(express.json());

const clients = new Set();
let currentLeader = null;

async function fetchReplicaStatus(replica) {
  try {
    const response = await axios.get(`${replica}/status`, {
      timeout: 500
    });

    return {
      url: replica,
      reachable: true,
      ...response.data
    };
  } catch (error) {
    return {
      url: replica,
      reachable: false,
      error: error.message
    };
  }
}

async function getClusterSnapshot() {
  const replicas = await Promise.all(REPLICAS.map(fetchReplicaStatus));
  const leaderCandidates = replicas
    .filter((replica) => replica.reachable && replica.isLeader)
    .sort((left, right) => {
      const termDelta = (right.currentTerm || 0) - (left.currentTerm || 0);
      if (termDelta !== 0) {
        return termDelta;
      }

      return String(left.nodeId || "").localeCompare(String(right.nodeId || ""));
    });
  const leaderReplica = leaderCandidates[0] || null;

  return {
    leader: leaderReplica ? leaderReplica.nodeId : null,
    leaderUrl: leaderReplica ? leaderReplica.url : currentLeader,
    replicas
  };
}

function replicaIdFromUrl(replicaUrl) {
  const match = String(replicaUrl || "").match(/replica\d+/);
  return match ? match[0] : replicaUrl;
}

async function updateReplicaPartition(replicaUrl, blockedPeerIds) {
  await axios.post(
    `${replicaUrl}/partition`,
    { blockedPeerIds },
    { timeout: 1000 }
  );
}

async function healNetworkPartition() {
  await Promise.all(
    REPLICAS.map((replicaUrl) => updateReplicaPartition(replicaUrl, []))
  );
}

async function isolateLeaderPartition() {
  const snapshot = await getClusterSnapshot();
  const leaderReplica = snapshot.replicas.find((replica) => replica.isLeader);

  if (!leaderReplica) {
    throw new Error("No leader available to isolate");
  }

  const leaderId = leaderReplica.nodeId;

  await Promise.all(
    REPLICAS.map((replicaUrl) => {
      const replicaId = replicaIdFromUrl(replicaUrl);
      const blockedPeerIds =
        replicaId === leaderId
          ? snapshot.replicas
              .map((replica) => replica.nodeId)
              .filter((nodeId) => nodeId && nodeId !== leaderId)
          : [leaderId];

      return updateReplicaPartition(replicaUrl, blockedPeerIds);
    })
  );

  return leaderId;
}

function leaderUrlFromId(leaderId) {
  if (!leaderId) {
    return null;
  }

  return REPLICAS.find((url) => url.includes(leaderId)) || null;
}

function replicaUrlFromId(replicaId) {
  if (!replicaId) {
    return null;
  }

  return REPLICAS.find((url) => url.includes(replicaId)) || null;
}

async function discoverLeader() {
  for (const replica of REPLICAS) {
    try {
      const response = await axios.get(`${replica}/who-is-leader`, {
        timeout: 300
      });

      if (response.data.isLeader) {
        currentLeader = replica;
        console.log("Leader discovered:", currentLeader);
        return currentLeader;
      }

      const hintedLeader = leaderUrlFromId(response.data.leaderId);
      if (hintedLeader) {
        currentLeader = hintedLeader;
        console.log("Leader hinted by follower:", currentLeader);
        return currentLeader;
      }
    } catch (error) {
      // Try another replica.
    }
  }

  currentLeader = null;
  return null;
}

async function forwardToLeader(payload) {
  const leader = currentLeader || (await discoverLeader());

  if (!leader) {
    throw new Error("No leader available");
  }

  try {
    const response = await axios.post(`${leader}/client-request`, payload, {
      timeout: 500
    });

    currentLeader = leader;
    return response.data;
  } catch (error) {
    const leaderId = error.response?.data?.leaderId;
    const hintedLeader = leaderUrlFromId(leaderId);

    if (hintedLeader && hintedLeader !== leader) {
      currentLeader = hintedLeader;
    } else {
      await discoverLeader();
    }

    if (!currentLeader || currentLeader === leader) {
      throw error;
    }

    const retry = await axios.post(`${currentLeader}/client-request`, payload, {
      timeout: 500
    });
    return retry.data;
  }
}

function broadcast(data) {
  const payload = JSON.stringify(data);

  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log("Client connected");

  ws.on("message", async (message) => {
    try {
      const payload = JSON.parse(message.toString());
      const result = await forwardToLeader(payload);

      ws.send(
        JSON.stringify({
          type: "ack",
          id: result.entry?.id || payload.id || null,
          commitIndex: result.commitIndex ?? null,
          leaderId: result.leaderId || currentLeader
        })
      );
    } catch (error) {
      ws.send(
        JSON.stringify({
          type: "error",
          message:
            error.response?.data?.error || error.message || "Leader unavailable"
        })
      );
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    console.log("Client disconnected");
  });
});

console.log(`WebSocket running on ws://localhost:${WS_PORT}`);

app.get("/", (req, res) => {
  res.send("Gateway is running");
});

app.get("/leader", async (req, res) => {
  const leader = currentLeader || (await discoverLeader());
  res.json({ leader });
});

app.get("/cluster-status", async (req, res) => {
  const snapshot = await getClusterSnapshot();

  if (!snapshot.leader && !currentLeader) {
    await discoverLeader();
    snapshot.leaderUrl = currentLeader;
  }

  res.json(snapshot);
});

app.post("/partition/isolate-leader", async (req, res) => {
  try {
    const isolatedLeader = await isolateLeaderPartition();
    res.json({
      success: true,
      isolatedLeader
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      error: error.message || "Failed to isolate leader"
    });
  }
});

app.post("/partition/heal", async (req, res) => {
  try {
    await healNetworkPartition();
    res.json({
      success: true
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      error: error.message || "Failed to heal partition"
    });
  }
});

app.post("/replica/:replicaId/:action", async (req, res) => {
  try {
    const replicaUrl = replicaUrlFromId(req.params.replicaId);
    const action = req.params.action;

    if (!replicaUrl) {
      return res.status(404).json({
        success: false,
        error: "Replica not found"
      });
    }

    if (!["start", "stop"].includes(action)) {
      return res.status(400).json({
        success: false,
        error: "Unsupported replica action"
      });
    }

    const response = await axios.post(
      `${replicaUrl}/control/${action}`,
      {},
      { timeout: 1000 }
    );

    return res.json({
      success: true,
      replicaId: req.params.replicaId,
      action,
      result: response.data
    });
  } catch (error) {
    return res.status(503).json({
      success: false,
      error: error.response?.data?.error || error.message || "Replica control failed"
    });
  }
});

app.get("/playback-log", async (req, res) => {
  try {
    const leader = currentLeader || (await discoverLeader());

    if (!leader) {
      return res.status(503).json({
        error: "No leader available"
      });
    }

    const response = await axios.get(`${leader}/sync-log`, {
      timeout: 1000
    });

    return res.json({
      leader,
      log: Array.isArray(response.data.log)
        ? response.data.log.slice(0, (response.data.commitIndex ?? -1) + 1)
        : [],
      commitIndex: response.data.commitIndex ?? -1,
      term: response.data.term ?? null,
      leaderId: response.data.leaderId ?? null
    });
  } catch (error) {
    return res.status(502).json({
      error: error.response?.data?.error || error.message || "Failed to fetch playback log"
    });
  }
});

app.post("/commit", (req, res) => {
  console.log("Commit received:", req.body.id || req.body.type || "entry");
  broadcast({
    type: "committed-stroke",
    stroke: req.body
  });
  res.json({ status: "ok" });
});

app.listen(HTTP_PORT, async () => {
  console.log(`HTTP server running on http://localhost:${HTTP_PORT}`);
  await discoverLeader();
});
