const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const blitz = require('@simonschick/blitzortungapi');
const BlitzClient = blitz.Client || blitz.default || blitz;

const app = express();
app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const bClient = new BlitzClient();

let strikeCount = 0;

bClient.on('strike', (strike) => {
    strikeCount++;
    const payload = JSON.stringify({ lat: strike.lat, lon: strike.lon });
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
});

// Prints a health check to your Dockge terminal every 60 seconds
setInterval(() => {
    console.log(`[Status] Server online. Global strikes received in the last minute: ${strikeCount}`);
    strikeCount = 0;
}, 60000);

server.listen(3000, () => {
    console.log('Weather dashboard serving on port 3000');
});