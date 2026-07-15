const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- 1. Lightning Core & History Cache ---
let strikeCache = [];
// Increased backend cache to 30 minutes
const MAX_STRIKE_AGE_MS = 30 * 60 * 1000; 

function addStrikeToCache(lat, lon) {
    const now = Date.now();
    strikeCache.push({ lat, lon, timestamp: now });
    strikeCache = strikeCache.filter(s => now - s.timestamp <= MAX_STRIKE_AGE_MS);
}

// --- 2. RainViewer Reverse Proxy & Memory Cache ---
let metadataCache = null;
let metadataTimestamp = 0;
const METADATA_TTL = 2 * 60 * 1000;
const tileCache = new Map();

async function getMetadata() {
    const now = Date.now();
    if (!metadataCache || (now - metadataTimestamp > METADATA_TTL)) {
        try {
            const response = await fetch('https://api.rainviewer.com/public/weather-maps.json');
            metadataCache = await response.json();
            metadataTimestamp = now;
            garbageCollectTiles(metadataCache);
        } catch (err) {
            console.error('[Radar Cache] Error fetching metadata:', err.message);
            if (!metadataCache) throw err;
        }
    }
    return metadataCache;
}

function garbageCollectTiles(newMetadata) {
    const activePaths = new Set();
    if (newMetadata.radar && newMetadata.radar.past) {
        newMetadata.radar.past.forEach(frame => activePaths.add(frame.path));
    }
    
    let deletedCount = 0;
    for (const key of tileCache.keys()) {
        const isActive = Array.from(activePaths).some(path => key.includes(path));
        if (!isActive) {
            tileCache.delete(key);
            deletedCount++;
        }
    }
    if (deletedCount > 0) {
        console.log(`[Radar Cache] Garbage collected ${deletedCount} expired tiles from RAM.`);
    }
}

app.get('/api/radar/metadata', async (req, res) => {
    try {
        const data = await getMetadata();
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch radar metadata' });
    }
});

app.get('/api/radar/tile/*', async (req, res) => {
    const tilePath = '/' + req.params[0];
    
    if (tileCache.has(tilePath)) {
        const cached = tileCache.get(tilePath);
        res.set('Content-Type', cached.contentType);
        return res.send(cached.buffer);
    }

    const targetUrl = `https://tilecache.rainviewer.com${tilePath}`;
    try {
        const response = await fetch(targetUrl);
        if (!response.ok) return res.status(response.status).end();
        
        const contentType = response.headers.get('content-type') || 'image/png';
        const arrayBuffer = await response.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        tileCache.set(tilePath, { buffer, contentType });
        res.set('Content-Type', contentType);
        res.send(buffer);
    } catch (err) {
        res.status(500).end();
    }
});

// --- 3. WebSocket Broker ---
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
    const wsBlitz = new WebSocket('wss://ws8.blitzortung.org/');

    wsBlitz.on('open', () => {
        console.log('[Blitzortung] Connected to live websocket feed.');
        wsBlitz.send(JSON.stringify({ a: 111 }));
    });

    wsBlitz.on('message', (data) => {
        try {
            const decodedText = decodeBlitzortung(data.toString('utf8'));
            const strike = JSON.parse(decodedText);
            
            if (strike && strike.lat && strike.lon) {
                const now = Date.now();
                addStrikeToCache(strike.lat, strike.lon);
                
                const payload = JSON.stringify({ type: 'strike', lat: strike.lat, lon: strike.lon, timestamp: now });
                wss.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN) client.send(payload);
                });
            }
        } catch (err) {}
    });

    wsBlitz.on('close', () => setTimeout(connectToBlitzortung, 5000));
    wsBlitz.on('error', () => wsBlitz.close());
}
connectToBlitzortung();

wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'history', data: strikeCache }));
});

setInterval(() => {
    console.log(`[Status] Active memory cache: ${tileCache.size} map tiles. Retained historical strikes: ${strikeCache.length}`);
}, 60000);

server.listen(3000, () => {
    console.log('Weather dashboard serving on port 3000');
});