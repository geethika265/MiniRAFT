function log(state, message) {
  const time = new Date().toISOString();
  console.log(
    `[${time}] [${state.nodeId}] [${state.state}] [term=${state.currentTerm}] ${message}`
  );
}

module.exports = { log };