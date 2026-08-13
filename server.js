const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

const rooms = new Map();

app.get("/", (req, res) => {
  res.send("CoListen server je v provozu!");
});

server.on("upgrade", (request, socket, head) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const parts = url.pathname.split("/").filter(Boolean);

  if (parts[0] !== "room" || !parts[1]) {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request, parts[1], url.searchParams.get("name") || "Anonymous");
  });
});

wss.on("connection", (ws, request, roomId, name) => {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Map());
  }

  const room = rooms.get(roomId);
  const sessionId = crypto.randomUUID();

  room.set(sessionId, { ws, name });

  function broadcast(msg, excludeId = null) {
    const data = JSON.stringify(msg);
    for (const [id, session] of room) {
      if (id !== excludeId && session.ws.readyState === 1) {
        try { session.ws.send(data); } catch {}
      }
    }
  }

  broadcast({ type: "joined", user: name }, sessionId);
  broadcast({
    type: "members",
    members: [...room.values()].map(s => s.name)
  });

  ws.on("message", (message) => {
    try {
      const msg = JSON.parse(message);
      broadcast(msg, sessionId);
    } catch {}
  });

  ws.on("close", () => {
    room.delete(sessionId);
    if (room.size === 0) {
      rooms.delete(roomId);
    } else {
      broadcast({ type: "left", user: name }, sessionId);
      broadcast({
        type: "members",
        members: [...room.values()].map(s => s.name)
      });
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server běží na portu ${PORT}`);
});
