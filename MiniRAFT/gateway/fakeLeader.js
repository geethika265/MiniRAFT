const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

let isLeader = true;

// simulate append
app.post("/append-entries", async (req, res) => {
    console.log("Leader received:", req.body);

    // simulate commit after delay
    setTimeout(async () => {
        await axios.post("http://localhost:3000/commit", req.body);
    }, 500);

    res.send({ status: "received" });
});

// leader check
app.get("/who-is-leader", (req, res) => {
    res.send({ isLeader });
});

app.listen(5000, () => {
    console.log("Fake Leader running on port 5000");
});