function createState(nodeId) {
  return {
    nodeId,
    state: "FOLLOWER",
    currentTerm: 0,
    votedFor: null,
    leaderId: null,
    electionTimer: null,
    heartbeatTimer: null,

    //YOUR PART STARTS HERE
    log: [],
    commitIndex: -1,
    peers: []
  };
}

module.exports = { createState };