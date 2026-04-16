function createState(nodeId) {
  return {
    nodeId,
    state: "FOLLOWER",
    currentTerm: 0,
    votedFor: null,
    leaderId: null,
    electionTimer: null,
    heartbeatTimer: null,
    shuttingDown: false,
    electionInFlight: false,
    stopped: false,

    log: [],
    commitIndex: -1,
    peers: [],
    blockedPeerIds: new Set()
  };
}

module.exports = { createState };
