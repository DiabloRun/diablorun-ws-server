const shortid = require("shortid");
const http = require("http");
const WebSocket = require("ws");
const dotenv = require("dotenv");

dotenv.config();

// HTTP server
const server = http.createServer();

// WS server
const wss = new WebSocket.Server({ server });
const rooms = {};

// Broadcast
server.on("request", async (req, res) => {
  try {
    if (req.method === "POST") {
      const body = await new Promise((resolve) => {
        const chunks = [];

        req
          .on("data", (chunk) => chunks.push(chunk))
          .on("end", () => resolve(Buffer.concat(chunks)));
      });

      const { secret, webMessages } = JSON.parse(body);

      if (secret !== process.env.SECRET) {
        res.end();
        return;
      }

      // Broadcast web messages
      if (webMessages) {
        for (const { room, payload } of webMessages) {
          if (room in rooms) {
            for (const ws of Object.values(rooms[room])) {
              ws.send(payload);
            }
          }
        }
      }
    } else {
      const out = {};

      for (const room in rooms) {
        out[room] = Object.keys(rooms[room]).length;
      }

      res.write(JSON.stringify(out));
    }
  } catch (err) {
    console.log("[ERROR]", new Date(), err);
  }

  res.end();
});

// WS connection
wss.on("connection", async (ws) => {
  const connectionId = shortid();
  const connectionRooms = [];

  ws.on("message", async (body) => {
    try {
      if (body === "ping") {
        ws.send(
          JSON.stringify({
            action: "pong",
            time: Date.now(),
          })
        );
        return;
      }

      const request = JSON.parse(body);

      if (request.action === "subscribe") {
        const room = request.payload;
        connectionRooms.push(room);

        if (!(room in rooms)) {
          rooms[room] = {};
        }

        rooms[room][connectionId] = ws;
      } else if (request.action === "unsubscribe") {
        const room = request.payload;

        if (connectionRooms.includes(room)) {
          connectionRooms.unshift(connectionRooms.indexOf(room), 1);
          delete rooms[room][connectionId];
        }
      }
    } catch (err) {
      console.log("[ERROR]", new Date(), err);
    }
  });

  ws.on("close", async () => {
    for (const room of connectionRooms) {
      delete rooms[room][connectionId];
    }
  });
});

server.listen(process.env.PORT, () =>
  console.log(`diablorun-ws-server running on port ${process.env.PORT}`)
);
