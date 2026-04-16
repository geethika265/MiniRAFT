const axios = require("axios");
const config = require("./config");
const { log } = require("./logger");

function peerIdFromUrl(peerUrl) {
  const match = String(peerUrl || "").match(/replica\d+/);
  return match ? match[0] : peerUrl;
}

function isPeerBlocked(state, peerUrl) {
  const peerId = peerIdFromUrl(peerUrl);
  return state.blockedPeerIds && state.blockedPeerIds.has(peerId);
}

function hasQuorum(state, peers) {
  const reachablePeers = peers.filter((peer) => !isPeerBlocked(state, peer)).length;
  return reachablePeers + 1 >= Math.floor((peers.length + 1) / 2) + 1;
}

function getRandomTimeout() {
  return Math.floor(
    Math.random() *
      (config.ELECTION_TIMEOUT_MAX - config.ELECTION_TIMEOUT_MIN + 1)
  ) + config.ELECTION_TIMEOUT_MIN;
}

function clearElectionTimer(state) {
  if (state.electionTimer) {
    clearTimeout(state.electionTimer);
    state.electionTimer = null;
  }
}

function clearHeartbeatTimer(state) {
  if (state.heartbeatTimer) {
    clearInterval(state.heartbeatTimer);
    state.heartbeatTimer = null;
  }
}

function becomeFollower(state, peers, term, leaderId = null) {
  const nextTerm = typeof term === "number" ? term : state.currentTerm;
  const termChanged = nextTerm !== state.currentTerm;
  const roleChanged = state.state !== "FOLLOWER";

  state.currentTerm = nextTerm;
  state.state = "FOLLOWER";
  state.votedFor = null;
  state.leaderId = leaderId;
  state.electionInFlight = false;

  clearHeartbeatTimer(state);

  if (roleChanged || termChanged) {
    log(state, `Stepped down to follower${leaderId ? ` under ${leaderId}` : ""}`);
  }

  if (peers) {
    resetElectionTimer(state, peers);
  }
}

function resetElectionTimer(state, peers) {
  if (state.stopped) {
    return;
  }

  clearElectionTimer(state);

  const timeout = getRandomTimeout();
  log(state, `Election timer reset to ${timeout} ms`);

  state.electionTimer = setTimeout(() => {
    if (state.state !== "LEADER") {
      startElection(state, peers);
    }
  }, timeout);
}

async function startElection(state, peers) {
  if (state.stopped || state.electionInFlight || state.state === "LEADER") {
    return;
  }

  clearElectionTimer(state);
  state.electionInFlight = true;
  state.state = "CANDIDATE";
  state.currentTerm += 1;
  state.votedFor = state.nodeId;
  state.leaderId = null;

  const electionTerm = state.currentTerm;
  let votes = 1;
  const majority = Math.floor((peers.length + 1) / 2) + 1;

  log(state, `Starting election for term ${electionTerm}. Need ${majority} votes.`);

  try {
    const responses = await Promise.all(
      peers.map(async (peer) => {
        if (isPeerBlocked(state, peer)) {
          log(state, `Vote request blocked by partition for ${peerIdFromUrl(peer)}`);
          return null;
        }

        try {
          const response = await axios.post(
            `${peer}/request-vote`,
            {
              term: electionTerm,
              candidateId: state.nodeId
            },
            { timeout: config.RPC_TIMEOUT }
          );

          return response.data;
        } catch (error) {
          log(state, `Vote request timed out or failed for ${peer}`);
          return null;
        }
      })
    );

    for (const data of responses) {
      if (!data) {
        continue;
      }

      if (data.term > state.currentTerm) {
        becomeFollower(state, peers, data.term, data.leaderId || null);
        return;
      }

      if (state.state !== "CANDIDATE" || state.currentTerm !== electionTerm) {
        return;
      }

      if (data.voteGranted) {
        votes += 1;
      }
    }
  } finally {
    state.electionInFlight = false;
  }

  if (state.state !== "CANDIDATE" || state.currentTerm !== electionTerm) return;

  if (votes >= majority) {
    becomeLeader(state, peers);
    return;
  }

  log(state, `Election lost in term ${electionTerm} with ${votes} votes`);
  resetElectionTimer(state, peers);
}

function becomeLeader(state, peers) {
  if (state.stopped) {
    return;
  }

  state.state = "LEADER";
  state.leaderId = state.nodeId;
  state.electionInFlight = false;
  clearElectionTimer(state);

  log(state, "Became leader");
  startHeartbeat(state, peers);
}

function startHeartbeat(state, peers) {
  clearHeartbeatTimer(state);

  state.heartbeatTimer = setInterval(async () => {
    if (state.stopped || state.state !== "LEADER") return;

    if (!hasQuorum(state, peers)) {
      log(state, "Lost quorum due to partition, stepping down");
      becomeFollower(state, peers, state.currentTerm, null);
      return;
    }

    for (const peer of peers) {
      if (isPeerBlocked(state, peer)) {
        log(state, `Heartbeat blocked by partition for ${peerIdFromUrl(peer)}`);
        continue;
      }

      try {
        const response = await axios.post(
          `${peer}/heartbeat`,
          {
            term: state.currentTerm,
            leaderId: state.nodeId
          },
          { timeout: config.RPC_TIMEOUT }
        );

        const data = response.data;

        if (data.term > state.currentTerm) {
          becomeFollower(state, peers, data.term, data.leaderId || null);
          return;
        }
      } catch (error) {
        log(state, `Heartbeat failed for ${peer}`);
      }
    }
  }, config.HEARTBEAT_INTERVAL);

  log(state, "Heartbeat started");
}

function handleVoteRequest(state, peers, body) {
  if (state.stopped) {
    return {
      voteGranted: false,
      term: state.currentTerm,
      leaderId: state.leaderId,
      error: "Node stopped"
    };
  }

  const { term, candidateId } = body;

  if (typeof term !== "number" || !candidateId) {
    return {
      voteGranted: false,
      term: state.currentTerm,
      leaderId: state.leaderId,
      error: "Invalid vote request"
    };
  }

  if (term > state.currentTerm) {
    becomeFollower(state, null, term, null);
  }

  if (term < state.currentTerm) {
    return { voteGranted: false, term: state.currentTerm, leaderId: state.leaderId };
  }

  if (state.votedFor === null || state.votedFor === candidateId) {
    state.votedFor = candidateId;
    resetElectionTimer(state, peers);
    log(state, `Voted for ${candidateId}`);
    return {
      voteGranted: true,
      term: state.currentTerm,
      leaderId: state.leaderId
    };
  }

  return { voteGranted: false, term: state.currentTerm, leaderId: state.leaderId };
}

function handleHeartbeat(state, peers, body) {
  if (state.stopped) {
    return {
      success: false,
      term: state.currentTerm,
      leaderId: state.leaderId,
      error: "Node stopped"
    };
  }

  const { term, leaderId } = body;

  if (typeof term !== "number" || !leaderId) {
    return {
      success: false,
      term: state.currentTerm,
      leaderId: state.leaderId,
      error: "Invalid heartbeat"
    };
  }

  if (term < state.currentTerm) {
    return { success: false, term: state.currentTerm, leaderId: state.leaderId };
  }

  becomeFollower(state, peers, term, leaderId);

  log(state, `Accepted heartbeat from ${leaderId}`);

  return { success: true, term: state.currentTerm, leaderId: state.leaderId };
}

function getStatus(state) {
  return {
    nodeId: state.nodeId,
    role: state.stopped ? "STOPPED" : state.state,
    state: state.stopped ? "STOPPED" : state.state,
    currentTerm: state.currentTerm,
    votedFor: state.votedFor,
    leaderId: state.leaderId,
    isLeader: !state.stopped && state.state === "LEADER",
    isHealthy: !state.shuttingDown && !state.stopped,
    electionInFlight: state.electionInFlight
  };
}

module.exports = {
  becomeFollower,
  clearElectionTimer,
  clearHeartbeatTimer,
  getStatus,
  resetElectionTimer,
  handleVoteRequest,
  handleHeartbeat
};
