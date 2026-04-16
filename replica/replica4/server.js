const { createReplicaServer } = require("../common/replicaServer");

createReplicaServer({
  nodeId: process.env.NODE_ID || "replica4",
  port: Number(process.env.PORT || 5004),
  peers: (process.env.PEERS || "http://localhost:5001,http://localhost:5002,http://localhost:5003")
    .split(",")
    .map((peer) => peer.trim())
    .filter(Boolean)
});
