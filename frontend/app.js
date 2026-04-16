const canvas = document.getElementById("canvas");
const ctx = canvas.getContext("2d");
const clearButton = document.getElementById("clearBtn");
const undoButton = document.getElementById("undoBtn");
const redoButton = document.getElementById("redoBtn");
const playButton = document.getElementById("playBtn");
const pauseButton = document.getElementById("pauseBtn");
const replayButton = document.getElementById("replayBtn");
const partitionLeaderButton = document.getElementById("partitionLeaderBtn");
const healNetworkButton = document.getElementById("healNetworkBtn");
const replicaControlSelect = document.getElementById("replicaControl");
const stopReplicaButton = document.getElementById("stopReplicaBtn");
const colorPicker = document.getElementById("colorPicker");
const brushSlider = document.getElementById("brushSize");
const playbackSpeedSlider = document.getElementById("playbackSpeed");
const playbackSpeedValue = document.getElementById("playbackSpeedValue");
const statusText = document.getElementById("status");
const dashboardLeader = document.getElementById("dashboardLeader");
const replicaCards = document.getElementById("replicaCards");

const protocol = window.location.protocol === "https:" ? "wss" : "ws";
const httpBaseUrl = `${window.location.protocol}//${window.location.hostname}:3000`;
let socket = null;
let isConnected = false;
let reconnectDelay = 1000;
const maxReconnectDelay = 8000;
const outboundQueue = [];
let dashboardPoller = null;
let playbackTimer = null;
let playbackLog = [];
let playbackIndex = 0;
let playbackLoaded = false;

let drawing = false;
let lastPoint = null;
let currentStrokeGroupId = null;
let currentColor = colorPicker ? colorPicker.value : "#000000";
let brushSize = brushSlider ? Number(brushSlider.value) : 2;
let playbackSpeed = playbackSpeedSlider ? Number(playbackSpeedSlider.value) : 30;

function setStatus(message, tone = "normal") {
  statusText.textContent = message;
  statusText.dataset.tone = tone;
}

function drawLine(x1, y1, x2, y2, color = "#000000", width = 2) {
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.lineCap = "round";
  ctx.stroke();
}

function clearCanvas() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function stopPlayback() {
  if (playbackTimer) {
    clearInterval(playbackTimer);
    playbackTimer = null;
  }
}

function resetPlaybackState() {
  stopPlayback();
  playbackLog = [];
  playbackIndex = 0;
  playbackLoaded = false;
}

function applyLogEntry(entry) {
  if (!entry) {
    return;
  }

  if (entry.type === "clear") {
    clearCanvas();
    return;
  }

  if (entry.type === "stroke") {
    drawLine(entry.x1, entry.y1, entry.x2, entry.y2, entry.color, entry.width);
  }
}

function renderLog(log) {
  clearCanvas();

  for (const entry of log) {
    applyLogEntry(entry);
  }
}

function buildCanvasState(log) {
  const groups = new Map();
  const groupOrder = [];
  let undoStack = [];
  let redoStack = [];

  for (const entry of log) {
    if (!entry) {
      continue;
    }

    if (entry.type === "clear") {
      groups.clear();
      groupOrder.length = 0;
      undoStack = [];
      redoStack = [];
      continue;
    }

    if (entry.type === "stroke") {
      const groupId = entry.strokeGroupId || entry.id;
      let group = groups.get(groupId);

      if (!group) {
        group = {
          id: groupId,
          entries: [],
          visible: true
        };
        groups.set(groupId, group);
        groupOrder.push(groupId);
        undoStack.push(groupId);
        redoStack = [];
      }

      group.entries.push(entry);
      continue;
    }

    if (entry.type === "undo") {
      const groupId = entry.targetGroupId;
      const group = groups.get(groupId);

      if (!group || !group.visible) {
        continue;
      }

      group.visible = false;
      undoStack = undoStack.filter((candidate) => candidate !== groupId);
      redoStack.push(groupId);
      continue;
    }

    if (entry.type === "redo") {
      const groupId = entry.targetGroupId;
      const group = groups.get(groupId);

      if (!group || group.visible) {
        continue;
      }

      group.visible = true;
      redoStack = redoStack.filter((candidate) => candidate !== groupId);
      undoStack.push(groupId);
    }
  }

  const renderEntries = [];
  for (const groupId of groupOrder) {
    const group = groups.get(groupId);
    if (group && group.visible) {
      renderEntries.push(...group.entries);
    }
  }

  return {
    renderEntries,
    undoTargetGroupId: undoStack.length ? undoStack[undoStack.length - 1] : null,
    redoTargetGroupId: redoStack.length ? redoStack[redoStack.length - 1] : null
  };
}

function getCanvasPoint(event) {
  const rect = canvas.getBoundingClientRect();
  const source = event.touches ? event.touches[0] : event;

  return {
    x: source.clientX - rect.left,
    y: source.clientY - rect.top
  };
}

function safeSend(message) {
  if (socket && isConnected && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
    return true;
  }

  if (outboundQueue.length < 1000) {
    outboundQueue.push(message);
  }

  setStatus("Gateway disconnected, buffering strokes", "error");
  return false;
}

function flushQueue() {
  while (outboundQueue.length && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(outboundQueue.shift()));
  }
}

function formatReplicaCard(replica) {
  if (!replica.reachable) {
    return `
      <article class="replica-card replica-card--down">
        <h4>${replica.url.split("://")[1] || replica.url}</h4>
        <p>State: down</p>
        <p>Error: ${replica.error || "Unreachable"}</p>
      </article>
    `;
  }

  return `
    <article class="replica-card ${replica.isLeader ? "replica-card--leader" : ""}">
      <h4>${replica.nodeId}</h4>
      <p>Role: ${replica.role}</p>
      <p>Term: ${replica.currentTerm}</p>
      <p>Log length: ${replica.logLength}</p>
      <p>Commit index: ${replica.commitIndex}</p>
      <p>Blocked peers: ${(replica.blockedPeers || []).join(", ") || "none"}</p>
      <p>Status: ${replica.isHealthy ? "healthy" : "unhealthy"}</p>
    </article>
  `;
}

async function refreshDashboard() {
  try {
    const response = await fetch(`${httpBaseUrl}/cluster-status`);
    const data = await response.json();

    dashboardLeader.textContent = `Leader: ${data.leader || "none"}`;
    replicaCards.innerHTML = data.replicas.map(formatReplicaCard).join("");
  } catch (error) {
    dashboardLeader.textContent = "Leader: unavailable";
    replicaCards.innerHTML = `
      <article class="replica-card replica-card--down">
        <h4>Dashboard unavailable</h4>
        <p>Could not reach gateway status API.</p>
      </article>
    `;
  }
}

async function syncCommittedCanvas(statusMessage = null) {
  try {
    const log = await fetchCommittedLog(false);
    const state = buildCanvasState(log);
    renderLog(state.renderEntries);

    if (statusMessage) {
      setStatus(statusMessage, "ok");
    }
  } catch (error) {
    setStatus("Failed to sync committed canvas", "error");
  }
}

function startDashboardPolling() {
  refreshDashboard();

  if (dashboardPoller) {
    clearInterval(dashboardPoller);
  }

  dashboardPoller = setInterval(refreshDashboard, 1000);
}

function connect() {
  socket = new WebSocket(`${protocol}://localhost:8080`);

  socket.addEventListener("open", () => {
    isConnected = true;
    reconnectDelay = 1000;
    flushQueue();
    setStatus("Connected to gateway", "ok");
    syncCommittedCanvas();
  });

  socket.addEventListener("close", () => {
    isConnected = false;
    setStatus("Disconnected from gateway", "error");

    setTimeout(() => {
      reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
      connect();
    }, reconnectDelay);
  });

  socket.addEventListener("error", () => {
    setStatus("WebSocket error", "error");
  });

  socket.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);

    if (message.type === "committed-stroke") {
      if (message.stroke?.type === "stroke") {
        applyLogEntry(message.stroke);
      } else {
        syncCommittedCanvas(
          message.stroke?.type === "clear"
            ? "Canvas cleared across replicas"
            : "Canvas updated across replicas"
        );
      }
      return;
    }

    if (message.type === "ack") {
      setStatus(`Committed through ${message.leaderId || "leader"}`, "ok");
      return;
    }

    if (message.type === "error") {
      setStatus(message.message || "Gateway error", "error");
    }
  });
}

function sendStrokeSegment(nextPoint) {
  if (!lastPoint) {
    lastPoint = nextPoint;
    return;
  }

  const stroke = {
    type: "stroke",
    id: `stroke-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    strokeGroupId: currentStrokeGroupId,
    x1: lastPoint.x,
    y1: lastPoint.y,
    x2: nextPoint.x,
    y2: nextPoint.y,
    color: currentColor,
    width: brushSize
  };

  safeSend(stroke);
  lastPoint = nextPoint;
}

function startDrawing(event) {
  drawing = true;
  currentStrokeGroupId = `group-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  lastPoint = getCanvasPoint(event);
}

function continueDrawing(event) {
  if (!drawing) {
    return;
  }

  event.preventDefault();
  sendStrokeSegment(getCanvasPoint(event));
}

function stopDrawing() {
  drawing = false;
  lastPoint = null;
  currentStrokeGroupId = null;
}

async function fetchCommittedLog(showStatus = true) {
  if (showStatus) {
    setStatus("Fetching committed log for playback...", "normal");
  }

  const response = await fetch(`${httpBaseUrl}/playback-log`);

  if (!response.ok) {
    throw new Error("Playback log unavailable");
  }

  const data = await response.json();
  return Array.isArray(data.log) ? data.log.filter(Boolean) : [];
}

async function fetchPlaybackLog(showStatus = true) {
  const log = await fetchCommittedLog(showStatus);
  return buildCanvasState(log).renderEntries;
}

function startPlayback() {
  if (playbackTimer) {
    return;
  }

  if (!playbackLoaded || playbackLog.length === 0) {
    setStatus("No committed strokes available for playback", "normal");
    return;
  }

  if (playbackIndex >= playbackLog.length) {
    setStatus("Playback already finished, use Replay to restart", "normal");
    return;
  }

  setStatus(
    playbackIndex === 0
      ? `Playing back ${playbackLog.length} visible stroke segments`
      : `Resuming playback at segment ${playbackIndex + 1} of ${playbackLog.length}`,
    "ok"
  );

  playbackTimer = setInterval(() => {
    applyLogEntry(playbackLog[playbackIndex]);
    playbackIndex += 1;

    if (playbackIndex >= playbackLog.length) {
      stopPlayback();
      setStatus(`Playback complete: ${playbackLog.length} visible stroke segments`, "ok");
    }
  }, playbackSpeed);
}

async function handlePlay() {
  if (playbackTimer) {
    setStatus("Playback is already running", "normal");
    return;
  }

  if (!playbackLoaded) {
    try {
      playbackLog = await fetchPlaybackLog();
      playbackIndex = 0;
      playbackLoaded = true;
      clearCanvas();
    } catch (error) {
      resetPlaybackState();
      setStatus("Failed to fetch playback log", "error");
      return;
    }
  }

  startPlayback();
}

function handlePause() {
  if (!playbackTimer) {
    setStatus("Playback is not running", "normal");
    return;
  }

  stopPlayback();
  setStatus(
    `Playback paused at segment ${playbackIndex} of ${playbackLog.length}`,
    "normal"
  );
}

async function handleReplay() {
  stopPlayback();

  try {
    playbackLog = await fetchPlaybackLog();
    playbackIndex = 0;
    playbackLoaded = true;
    clearCanvas();
    startPlayback();
  } catch (error) {
    resetPlaybackState();
    setStatus("Failed to fetch playback log", "error");
  }
}

async function sendCompensationEvent(type) {
  try {
    const log = await fetchCommittedLog(false);
    const state = buildCanvasState(log);
    const targetGroupId =
      type === "undo" ? state.undoTargetGroupId : state.redoTargetGroupId;

    if (!targetGroupId) {
      setStatus(
        type === "undo" ? "Nothing to undo" : "Nothing to redo",
        "normal"
      );
      return;
    }

    const sent = safeSend({
      type,
      id: `${type}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
      targetGroupId
    });

    if (sent) {
      setStatus(
        type === "undo"
          ? "Undo event sent to RAFT log"
          : "Redo event sent to RAFT log",
        "normal"
      );
    }
  } catch (error) {
    setStatus(`Failed to ${type}`, "error");
  }
}

async function isolateLeaderPartition() {
  try {
    const response = await fetch(`${httpBaseUrl}/partition/isolate-leader`, {
      method: "POST"
    });
    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error || "Failed to isolate leader");
    }

    setStatus(`Partition created: isolated ${data.isolatedLeader}`, "normal");
    refreshDashboard();
  } catch (error) {
    setStatus(error.message || "Failed to isolate leader", "error");
  }
}

async function healNetworkPartition() {
  try {
    const response = await fetch(`${httpBaseUrl}/partition/heal`, {
      method: "POST"
    });
    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error || "Failed to heal network");
    }

    setStatus("Network partition healed", "ok");
    refreshDashboard();
  } catch (error) {
    setStatus(error.message || "Failed to heal network", "error");
  }
}

async function controlReplica(action) {
  const replicaId = replicaControlSelect ? replicaControlSelect.value : null;

  if (!replicaId) {
    setStatus("Select a replica first", "error");
    return;
  }

  try {
    const response = await fetch(`${httpBaseUrl}/replica/${replicaId}/${action}`, {
      method: "POST"
    });
    const data = await response.json();

    if (!response.ok) {
      throw new Error(data.error || `Failed to ${action} ${replicaId}`);
    }

    setStatus(
      `${action === "stop" ? "Stopped" : "Started"} ${replicaId}`,
      action === "start" ? "ok" : "normal"
    );
    refreshDashboard();
  } catch (error) {
    setStatus(error.message || `Failed to ${action} replica`, "error");
  }
}

canvas.addEventListener("mousedown", startDrawing);
canvas.addEventListener("mousemove", continueDrawing);
canvas.addEventListener("mouseup", stopDrawing);
canvas.addEventListener("mouseleave", stopDrawing);

canvas.addEventListener("touchstart", startDrawing, { passive: true });
canvas.addEventListener("touchmove", continueDrawing, { passive: false });
canvas.addEventListener("touchend", stopDrawing);
canvas.addEventListener("touchcancel", stopDrawing);

if (colorPicker) {
  colorPicker.addEventListener("input", (event) => {
    currentColor = event.target.value;
  });
}

if (brushSlider) {
  brushSlider.addEventListener("input", (event) => {
    brushSize = Number(event.target.value) || 2;
  });
}

if (playbackSpeedSlider) {
  playbackSpeedSlider.addEventListener("input", (event) => {
    playbackSpeed = Number(event.target.value) || 30;
    if (playbackSpeedValue) {
      playbackSpeedValue.textContent = `${playbackSpeed}ms`;
    }
  });
}

clearButton.addEventListener("click", () => {
  resetPlaybackState();

  const clearEvent = {
    type: "clear",
    id: `clear-${Date.now()}-${Math.random().toString(16).slice(2)}`
  };

  const sent = safeSend(clearEvent);
  if (sent) {
    setStatus("Clear event sent to RAFT log", "normal");
  }
});

if (undoButton) {
  undoButton.addEventListener("click", () => {
    sendCompensationEvent("undo");
  });
}

if (redoButton) {
  redoButton.addEventListener("click", () => {
    sendCompensationEvent("redo");
  });
}

if (playButton) {
  playButton.addEventListener("click", handlePlay);
}

if (pauseButton) {
  pauseButton.addEventListener("click", handlePause);
}

if (replayButton) {
  replayButton.addEventListener("click", handleReplay);
}

if (partitionLeaderButton) {
  partitionLeaderButton.addEventListener("click", isolateLeaderPartition);
}

if (healNetworkButton) {
  healNetworkButton.addEventListener("click", healNetworkPartition);
}

if (stopReplicaButton) {
  stopReplicaButton.addEventListener("click", () => {
    controlReplica("stop");
  });
}

setStatus("Connecting to gateway...", "normal");
connect();
startDashboardPolling();
