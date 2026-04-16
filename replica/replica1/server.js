const { createReplicaServer } = require("../common/replicaServer");

// Replica1 entrypoint supports env-driven startup for local runs and Docker.
createReplicaServer({
  nodeId: process.env.NODE_ID || "replica1",
  port: Number(process.env.PORT || 5001),
  peers: (process.env.PEERS || "http://localhost:5002,http://localhost:5003,http://localhost:5004")
    .split(",")
    .map((peer) => peer.trim())
    .filter(Boolean)
});
//hot reload test
