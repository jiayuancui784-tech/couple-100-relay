const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// Store room state: { roomId: { clients: Set<ws>, lastState: object } }
const rooms = {};

const server = http.createServer((req, res) => {
  // Health check endpoint
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', rooms: Object.keys(rooms).length }));
    return;
  }
  res.writeHead(200);
  res.end('couple100-relay is running');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  let roomId = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      switch (msg.type) {
        case 'join': {
          roomId = String(msg.room || '').toUpperCase();
          if (!roomId || roomId.length < 3) {
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid room' }));
            return;
          }

          // Initialize room if needed
          if (!rooms[roomId]) {
            rooms[roomId] = { clients: new Set(), lastState: null };
          }

          rooms[roomId].clients.add(ws);
          console.log(`[${roomId}] client joined (total: ${rooms[roomId].clients.size})`);

          // Send the last known state to the new client immediately
          if (rooms[roomId].lastState) {
            ws.send(JSON.stringify({ type: 'update', data: rooms[roomId].lastState }));
          }

          ws.send(JSON.stringify({ type: 'joined', room: roomId, peers: rooms[roomId].clients.size }));
          break;
        }

        case 'update': {
          if (!roomId || !rooms[roomId]) return;

          const data = msg.data;
          if (!data || !Array.isArray(data.items)) return;

          // Store last state
          rooms[roomId].lastState = data;

          // Broadcast to all OTHER clients in the room
          const payload = JSON.stringify({ type: 'update', data: data });
          for (const client of rooms[roomId].clients) {
            if (client !== ws && client.readyState === 1) {
              client.send(payload);
            }
          }
          console.log(`[${roomId}] update broadcast to ${rooms[roomId].clients.size - 1} peers`);
          break;
        }

        case 'ping':
          ws.send(JSON.stringify({ type: 'pong' }));
          break;
      }
    } catch (e) {
      console.error('Message parse error:', e.message);
    }
  });

  ws.on('close', () => {
    if (roomId && rooms[roomId]) {
      rooms[roomId].clients.delete(ws);
      console.log(`[${roomId}] client left (remaining: ${rooms[roomId].clients.size})`);
      // Clean up empty rooms
      if (rooms[roomId].clients.size === 0) {
        // Keep lastState for 10 minutes in case clients reconnect
        setTimeout(() => {
          if (rooms[roomId] && rooms[roomId].clients.size === 0) {
            delete rooms[roomId];
            console.log(`[${roomId}] room cleaned up`);
          }
        }, 600000);
      }
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
  });
});

// Periodic cleanup of stale rooms
setInterval(() => {
  for (const [roomId, room] of Object.entries(rooms)) {
    if (room.clients.size === 0) {
      delete rooms[roomId];
      console.log(`[${roomId}] stale room cleaned up`);
    }
  }
}, 3600000); // Every hour

server.listen(PORT, () => {
  console.log(`couple100-relay running on port ${PORT}`);
});
