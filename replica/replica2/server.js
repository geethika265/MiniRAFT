const { createReplicaServer } = require("../common/replicaServer");

createReplicaServer({
  nodeId: process.env.NODE_ID || "replica2",
  port: Number(process.env.PORT || 5002),
  peers: (process.env.PEERS || "http://localhost:5001,http://localhost:5003,http://localhost:5004")
    .split(",")
    .map((peer) => peer.trim())
    .filter(Boolean)
});
