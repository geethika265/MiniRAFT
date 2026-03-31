const WebSocket = require("ws");

const ws = new WebSocket("ws://localhost:8080");

ws.on("open", () => {
    console.log("Connected to gateway");

    ws.send(JSON.stringify({
        type: "stroke",
        x1: 10,
        y1: 20,
        x2: 100,
        y2: 200,
        color: "black"
    }));
});

ws.on("message", (msg) => {
    console.log("Broadcast received:", msg.toString());
});