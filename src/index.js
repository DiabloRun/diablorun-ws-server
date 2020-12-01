const shortid = require('shortid');
const http = require('http');
const WebSocket = require('ws');
const dotenv = require('dotenv');
const { sendTwitchMessages, runTwitchBot } = require('./twitch');

dotenv.config();

// HTTP server
const server = http.createServer();

// WS server
const wss = new WebSocket.Server({ server });
const rooms = {};

// Broadcast
server.on('request', async (req, res) => {
  try {
    const body = await new Promise(resolve => {
      const chunks = [];

      req
        .on('data', chunk => chunks.push(chunk))
        .on('end', () => resolve(Buffer.concat(chunks)));
    });

    const { action, room, payload, secret, twitchMessages } = JSON.parse(body);

    if (secret !== process.env.SECRET) {
      res.end();
      return;
    }

    if (action === 'broadcast' && room in rooms) {
      for (const ws of Object.values(rooms[room])) {
        ws.send(payload);
      }
    }

    if (twitchMessages) {
      sendTwitchMessages(twitchMessages);
    }
  } catch (err) {
    const out = {};

    for (const room in rooms) {
      out[room] = Object.keys(rooms[room]).length;
    }

    //console.log(new Date(), err);
    res.write(JSON.stringify(out));
  }

  res.end();
});

// WS connection
wss.on('connection', async ws => {
  const connectionId = shortid();
  let room;

  ws.on('message', async body => {
    try {
      if (body === 'ping') {
        ws.send('pong');
        return;
      }

      const request = JSON.parse(body);

      if (request.action === 'subscribe') {
        if (room) {
          delete rooms[room][connectionId];
        }

        room = request.payload;

        if (!(room in rooms)) {
          rooms[room] = {};
        }

        rooms[room][connectionId] = ws;
      }
    } catch (err) {
      console.log(err);
    }
  });

  ws.on('close', async () => {
    if (room) {
      delete rooms[room][connectionId];
    }
  });
});

runTwitchBot();
setInterval(async () => await runTwitchBot(), 600000); // reload channels every 10 mins

server.listen(process.env.PORT);