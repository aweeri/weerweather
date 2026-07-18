const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- 0. Persistent Disk Storage Setup ---
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR);
}
const CACHE_FILE = path.join(DATA_DIR, 'strikes.json');

// --- 1. Lightning Core & History Cache ---
let strikeCache = [];
const MAX_STRIKE_AGE_MS = 6 * 60 * 60 * 1000; // 6 hours to cover radar span + 3h history

// Load saved strikes from hard drive on boot
if (fs.existsSync(CACHE_FILE)) {
    try {
        const rawData = fs.readFileSync(CACHE_FILE, 'utf8');
        const loaded = JSON.parse(rawData);
        const now = Date.now();
        // Only keep strikes that are still under 30 minutes old
        strikeCache = loaded.filter(s => now - s.timestamp <= MAX_STRIKE_AGE_MS);
        console.log(`[Storage] Successfully restored ${strikeCache.length} historical strikes from disk.`);
    } catch (err) {
        console.error('[Storage] Error reading save file:', err.message);
    }
}

function saveStrikesToDisk() {
    try {
        fs.writeFileSync(CACHE_FILE, JSON.stringify(strikeCache));
    } catch (err) {
        console.error('[Storage] Error writing save file:', err.message);
    }
}

// Auto-save every 10 seconds just in case of a crash
setInterval(saveStrikesToDisk, 10000);

// Panic-save right before Docker kills the container during an Update
process.on('SIGTERM', () => {
    console.log('[Storage] Container updating... Saving lightning history to disk.');
    saveStrikesToDisk();
    process.exit(0);
});

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

let prefetchQueue = [];
let isPrefetching = false;

async function processPrefetchQueue() {
    if (isPrefetching || prefetchQueue.length === 0) return;
    isPrefetching = true;
    
    while (prefetchQueue.length > 0) {
        const tilePath = prefetchQueue.shift();
        if (!tileCache.has(tilePath)) {
            let targetUrl = '';
            try {
                targetUrl = `https://tilecache.rainviewer.com${tilePath}`;
                const response = await fetch(targetUrl);
                if (response.ok) {
                    const contentType = response.headers.get('content-type') || 'image/png';
                    const arrayBuffer = await response.arrayBuffer();
                    const buffer = Buffer.from(arrayBuffer);
                    tileCache.set(tilePath, { buffer, contentType });
                } else if (response.status === 429) {
                    prefetchQueue.unshift(tilePath);
                    await new Promise(r => setTimeout(r, 5000));
                    continue;
                } else {
                    console.error(`[Radar Cache] Prefetch failed: ${response.status} ${response.statusText} -> ${targetUrl}`);
                }
            } catch (err) {
                console.error(`[Radar Cache] Prefetch error for ${targetUrl}:`, err.message);
            }
            await new Promise(r => setTimeout(r, 150));
        }
    }
    isPrefetching = false;
}

function queuePrefetch(metadata) {
    if (!metadata) return;
    
    function enqueueTiles(path, size, colorScheme, options) {
        for (let z = 0; z <= 3; z++) {
            const maxCoord = Math.pow(2, z);
            for (let x = 0; x < maxCoord; x++) {
                for (let y = 0; y < maxCoord; y++) {
                    const tilePath = `${path}/${size}/${z}/${x}/${y}/${colorScheme}/${options}.png`;
                    if (!tileCache.has(tilePath) && !prefetchQueue.includes(tilePath)) {
                        prefetchQueue.push(tilePath);
                    }
                }
            }
        }
    }

    if (metadata.radar && metadata.radar.past) {
        metadata.radar.past.forEach(frame => enqueueTiles(frame.path, 512, 2, '1_1'));
    }
    if (metadata.satellite && metadata.satellite.infrared) {
        metadata.satellite.infrared.forEach(frame => enqueueTiles(frame.path, 256, 0, '0_0'));
    }

    processPrefetchQueue();
}

async function getMetadata() {
    const now = Date.now();
    if (!metadataCache || (now - metadataTimestamp > METADATA_TTL)) {
        try {
            const response = await fetch('https://api.rainviewer.com/public/weather-maps.json');
            metadataCache = await response.json();
            metadataTimestamp = now;
            garbageCollectTiles(metadataCache);
            queuePrefetch(metadataCache);
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
    if (newMetadata.satellite && newMetadata.satellite.infrared) {
        newMetadata.satellite.infrared.forEach(frame => activePaths.add(frame.path));
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
        if (!response.ok) {
            console.error(`[Radar Proxy] Tile fetch failed: ${response.status} ${response.statusText} -> ${targetUrl}`);
            return res.status(response.status).end();
        }
        
        const contentType = response.headers.get('content-type') || 'image/png';
        const arrayBuffer = await response.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        tileCache.set(tilePath, { buffer, contentType });
        res.set('Content-Type', contentType);
        res.send(buffer);
    } catch (err) {
        console.error(`[Radar Proxy] Network/Server error fetching ${targetUrl}:`, err.message);
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