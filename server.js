const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let strikeCount = 0;

// Reverse-engineered LZW Decompression Algorithm
function decodeBlitzortung(b) {
    let a, e = {}, d = b.split(""), c = d[0], f = c, g = [c], h = 256, o = h;
    for (let i = 1; i < d.length; i++) {
        a = d[i].charCodeAt(0);
        a = h > a ? d[i] : e[a] ? e[a] : f + c;
        g.push(a);
        c = a.charAt(0);
        e[o] = f + c;
        o++;
        f = a;
    }
    return g.join("");
}

function connectToBlitzortung() {
    // Connect directly to the official websocket
    const wsBlitz = new WebSocket('wss://ws8.blitzortung.org/');

    wsBlitz.on('open', () => {
        console.log('[Blitzortung] Connected to live websocket.');
        // Send the secret initialization payload to start the data stream
        wsBlitz.send(JSON.stringify({ a: 111 }));
    });

    wsBlitz.on('message', (data) => {
        try {
            // Decrypt the scrambled payload
            const decodedText = decodeBlitzortung(data.toString('utf8'));
            const strike = JSON.parse(decodedText);
            
            // If it's a valid strike coordinate, send it to our web dashboard
            if (strike && strike.lat && strike.lon) {
                strikeCount++;
                const payload = JSON.stringify({ lat: strike.lat, lon: strike.lon });
                
                wss.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(payload);
                    }
                });
            }
        } catch (err) {
            // Ignore non-JSON heartbeat messages
        }
    });

    wsBlitz.on('close', () => {
        console.log('[Blitzortung] Connection lost. Reconnecting in 5 seconds...');
        setTimeout(connectToBlitzortung, 5000);
    });

    wsBlitz.on('error', (err) => {
        console.error('[Blitzortung] WebSocket Error:', err.message);
        wsBlitz.close();
    });
}

// Boot up the connection
connectToBlitzortung();

// Print a health check to your Dockge terminal every 60 seconds
setInterval(() => {
    console.log(`[Status] Server online. Global strikes received in the last minute: ${strikeCount}`);
    strikeCount = 0;
}, 60000);

server.listen(3000, () => {
    console.log('Weather dashboard serving on port 3000');
});