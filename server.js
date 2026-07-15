const express = require('express');
const http = require('http');
const WebSocket = require('ws');

// Safely extract the client constructor however the module exports it
const blitz = require('@simonschick/blitzortungapi');
const BlitzClient = blitz.Client || blitz.default || blitz;

const app = express();
app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Initialize Blitzortung client
const bClient = new BlitzClient();

// Proxy live strikes to any connected frontend clients
bClient.on('strike', (strike) => {
    const payload = JSON.stringify({ lat: strike.lat, lon: strike.lon });
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
});

server.listen(3000, () => {
    console.log('Weather dashboard serving on port 3000');
});