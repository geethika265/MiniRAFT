const axios = require("axios");
const config = require("./config");
const { log } = require("./logger");

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

function resetElectionTimer(state, peers) {
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
  state.state = "CANDIDATE";
  state.currentTerm += 1;
  state.votedFor = state.nodeId;
  state.leaderId = null;

  let votes = 1;
  const majority = Math.floor((peers.length + 1) / 2) + 1;

  log(state, `Starting election. Need ${majority} votes.`);

  for (const peer of peers) {
    try {
      const response = await axios.post(
        `${peer}/request-vote`,
        {
          term: state.currentTerm,
          candidateId: state.nodeId
        },
        { timeout: 1000 } // ✅ FIX: timeout added
      );

      const data = response.data;

      if (data.term > state.currentTerm) {
        state.state = "FOLLOWER";
        state.currentTerm = data.term;
        state.votedFor = null;
        state.leaderId = null;
        clearHeartbeatTimer(state);
        resetElectionTimer(state, peers);
        return;
      }

      if (data.voteGranted) {
        votes += 1;
      }
    } catch (error) {
      // ✅ FIX: less aggressive logging
      console.log(`⚠️ Vote request timeout/unreachable: ${peer}`);
    }
  }

  if (state.state !== "CANDIDATE") return;

  if (votes >= majority) {
    becomeLeader(state, peers);
  } else {
    log(state, `Election lost with ${votes} votes`);
    resetElectionTimer(state, peers);
  }
}

function becomeLeader(state, peers) {
  state.state = "LEADER";
  state.leaderId = state.nodeId;
  clearElectionTimer(state);

  log(state, "Became leader");
  startHeartbeat(state, peers);
}

function startHeartbeat(state, peers) {
  clearHeartbeatTimer(state);

  state.heartbeatTimer = setInterval(async () => {
    if (state.state !== "LEADER") return;

    for (const peer of peers) {
      try {
        const response = await axios.post(
          `${peer}/heartbeat`,
          {
            term: state.currentTerm,
            leaderId: state.nodeId
          },
          { timeout: 1000 } // ✅ FIX: timeout added
        );

        const data = response.data;

        if (data.term > state.currentTerm) {
          state.state = "FOLLOWER";
          state.currentTerm = data.term;
          state.votedFor = null;
          state.leaderId = null;
          clearHeartbeatTimer(state);
          resetElectionTimer(state, peers);
          return;
        }
      } catch (error) {
        // ✅ FIX: cleaner message (not scary)
        console.log(`⚠️ Peer not reachable: ${peer}`);
      }
    }
  }, config.HEARTBEAT_INTERVAL);

  log(state, "Heartbeat started");
}

function handleVoteRequest(state, peers, body) {
  const { term, candidateId } = body;

  if (term > state.currentTerm) {
    state.currentTerm = term;
    state.state = "FOLLOWER";
    state.votedFor = null;
    state.leaderId = null;
    clearHeartbeatTimer(state);
  }

  if (term < state.currentTerm) {
    return { voteGranted: false, term: state.currentTerm };
  }

  if (state.votedFor === null || state.votedFor === candidateId) {
    state.votedFor = candidateId;
    resetElectionTimer(state, peers);
    log(state, `Voted for ${candidateId}`);
    return { voteGranted: true, term: state.currentTerm };
  }

  return { voteGranted: false, term: state.currentTerm };
}

function handleHeartbeat(state, peers, body) {
  const { term, leaderId } = body;

  if (term < state.currentTerm) {
    return { success: false, term: state.currentTerm };
  }

  state.currentTerm = term;
  state.state = "FOLLOWER";
  state.votedFor = null;
  state.leaderId = leaderId;

  clearHeartbeatTimer(state);
  resetElectionTimer(state, peers);

  log(state, `Accepted heartbeat from ${leaderId}`);

  return { success: true, term: state.currentTerm };
}

module.exports = {
  resetElectionTimer,
  handleVoteRequest,
  handleHeartbeat
};