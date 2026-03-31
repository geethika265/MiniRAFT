const WebSocket = require("ws");
const axios = require("axios");
const express = require("express");

const WS_PORT = 8080;
const HTTP_PORT = 3000;

// -------------------- WebSocket Server --------------------
const wss = new WebSocket.Server({ port: WS_PORT });

let clients = [];

wss.on("connection", (ws) => {
    console.log("Client connected");
    clients.push(ws);

    ws.on("message", async (message) => {
        const data = JSON.parse(message);
        console.log("Received from client:", data);

        await forwardToLeader(data);
    });

    ws.on("close", () => {
        clients = clients.filter(c => c !== ws);
        console.log("Client disconnected");
    });
});

console.log(`WebSocket running on ws://localhost:${WS_PORT}`);

// -------------------- Leader Routing --------------------
const replicas = [
    "http://localhost:5000",
    "http://localhost:5001",
    "http://localhost:5002"
];

let currentLeader = replicas[0];

async function forwardToLeader(data) {
    try {
        await axios.post(`${currentLeader}/append-entries`, data);
        console.log("Sent to leader:", currentLeader);
    } catch (err) {
        console.log("Leader failed. Finding new leader...");
        await findLeader();

        try {
            await axios.post(`${currentLeader}/append-entries`, data);
            console.log("Retried success:", currentLeader);
        } catch {
            console.log("Retry failed");
        }
    }
}

async function findLeader() {
    for (let replica of replicas) {
        try {
            const res = await axios.get(`${replica}/who-is-leader`);
            if (res.data.isLeader) {
                currentLeader = replica;
                console.log("New leader found:", replica);
                return;
            }
        } catch {}
    }
    console.log("No leader found");
}

// -------------------- HTTP Server (FIXED ROOT ROUTE) --------------------
const app = express();
app.use(express.json());

// ✅ FIX: No more "Cannot GET /"
app.get("/", (req, res) => {
    res.send("🚀 Gateway is running successfully!");
});

// Commit endpoint (called by leader)
app.post("/commit", (req, res) => {
    const stroke = req.body;
    console.log("Commit received:", stroke);

    broadcast(stroke);

    res.send({ status: "ok" });
});

app.listen(HTTP_PORT, () => {
    console.log(`HTTP server running on http://localhost:${HTTP_PORT}`);
});

// -------------------- Broadcast --------------------
function broadcast(data) {
    clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}