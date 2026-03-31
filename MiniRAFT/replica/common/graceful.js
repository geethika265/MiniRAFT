const { log } = require("./logger");
const { clearElectionTimer, clearHeartbeatTimer } = require("./election");

function registerGracefulShutdown(server, state) {
  const shutdown = (signal) => {
    if (state.shuttingDown) return;

    state.shuttingDown = true;
    log(state, `Received ${signal}. Shutting down gracefully...`);

    clearElectionTimer(state);
    clearHeartbeatTimer(state);

    server.close(() => {
      log(state, "HTTP server closed.");
      process.exit(0);
    });

    setTimeout(() => {
      log(state, "Forced shutdown.");
      process.exit(1);
    }, 3000);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

module.exports = { registerGracefulShutdown };