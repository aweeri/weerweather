const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.static('public'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const CLIENT_PING_INTERVAL_MS = 30000;
const BLITZ_STALE_MS = 45000;
const BLITZ_BASE_RECONNECT_MS = 2000;
const BLITZ_MAX_RECONNECT_MS = 60000;
const BLITZ_DECODE_ERROR_LOG_WINDOW_MS = 60000;
const BLITZ_DECODE_ERROR_LOG_LIMIT = 8;

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

function addStrikeToCache(lat, lon) {
    const now = Date.now();
    strikeCache.push({ lat, lon, timestamp: now });
    strikeCache = strikeCache.filter(s => now - s.timestamp <= MAX_STRIKE_AGE_MS);
}

function addStrikeToCacheWithTimestamp(lat, lon, timestamp) {
    const safeTimestamp = Number.isFinite(timestamp) ? timestamp : Date.now();
    strikeCache.push({ lat, lon, timestamp: safeTimestamp });
    strikeCache = strikeCache.filter(s => safeTimestamp - s.timestamp <= MAX_STRIKE_AGE_MS);
}

// --- 2. RainViewer Reverse Proxy & Memory Cache ---
let metadataCache = null;
let metadataTimestamp = 0;
const METADATA_TTL = 2 * 60 * 1000;
const tileCache = new Map();

function logInfo(message) {
    console.log(`[Info] ${message}`);
}

function logWarn(message) {
    console.warn(`[Warn] ${message}`);
}

function logError(message, err) {
    if (err && err.stack) {
        console.error(`[Error] ${message}\n${err.stack}`);
        return;
    }
    if (err && err.message) {
        console.error(`[Error] ${message}: ${err.message}`);
        return;
    }
    console.error(`[Error] ${message}`);
}

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

    if (metadata.radar) {
        if (metadata.radar.past) {
            metadata.radar.past.forEach(frame => enqueueTiles(frame.path, 512, 2, '1_1'));
        }
        if (metadata.radar.nowcast) {
            metadata.radar.nowcast.forEach(frame => enqueueTiles(frame.path, 512, 2, '1_1'));
        }
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
    if (newMetadata.radar) {
        if (newMetadata.radar.past) {
            newMetadata.radar.past.forEach(frame => activePaths.add(frame.path));
        }
        if (newMetadata.radar.nowcast) {
            newMetadata.radar.nowcast.forEach(frame => activePaths.add(frame.path));
        }
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

const BLITZ_ENDPOINTS = [
    'wss://ws1.blitzortung.org/',
    'wss://ws2.blitzortung.org/',
    'wss://ws3.blitzortung.org/',
    'wss://ws4.blitzortung.org/',
    'wss://ws5.blitzortung.org/',
    'wss://ws6.blitzortung.org/',
    'wss://ws7.blitzortung.org/',
    'wss://ws8.blitzortung.org/'
];

const recentStrikeKeys = new Map();
const upstreamState = new Map();
let lastBroadcastAt = 0;
let isShuttingDown = false;
let decodeErrorsInWindow = 0;
let decodeWindowStartedAt = Date.now();

function shouldLogDecodeError() {
    const now = Date.now();
    if (now - decodeWindowStartedAt >= BLITZ_DECODE_ERROR_LOG_WINDOW_MS) {
        decodeWindowStartedAt = now;
        decodeErrorsInWindow = 0;
    }
    decodeErrorsInWindow += 1;
    return decodeErrorsInWindow <= BLITZ_DECODE_ERROR_LOG_LIMIT;
}

function extractStrikeTimestamp(strike) {
    const candidates = [strike.timestamp, strike.time, strike.ts, strike.utc];
    for (const candidate of candidates) {
        if (!Number.isFinite(candidate)) continue;
        if (candidate > 1e12) return candidate;
        if (candidate > 1e9) return candidate * 1000;
    }
    return Date.now();
}

function strikeDedupeKey(lat, lon, timestamp) {
    const sec = Math.floor(timestamp / 1000);
    return `${Math.round(lat * 1000)}:${Math.round(lon * 1000)}:${sec}`;
}

function cleanupRecentStrikeKeys(now) {
    const maxAge = 30000;
    for (const [key, seenAt] of recentStrikeKeys.entries()) {
        if (now - seenAt > maxAge) {
            recentStrikeKeys.delete(key);
        }
    }
}

function broadcastStrike(lat, lon, timestamp) {
    const key = strikeDedupeKey(lat, lon, timestamp);
    const now = Date.now();
    cleanupRecentStrikeKeys(now);

    if (recentStrikeKeys.has(key)) {
        return;
    }
    recentStrikeKeys.set(key, now);

    addStrikeToCacheWithTimestamp(lat, lon, timestamp);
    lastBroadcastAt = now;

    const payload = JSON.stringify({ type: 'strike', lat, lon, timestamp });
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

function scheduleReconnect(state, reason) {
    if (isShuttingDown) return;

    const jitterMs = Math.floor(Math.random() * 1000);
    const waitMs = Math.min(state.backoffMs, BLITZ_MAX_RECONNECT_MS) + jitterMs;
    state.backoffMs = Math.min(state.backoffMs * 2, BLITZ_MAX_RECONNECT_MS);

    setTimeout(() => {
        connectToBlitzortungEndpoint(state.url);
    }, waitMs);

    logWarn(`[Blitzortung] Reconnecting ${state.url} in ${waitMs}ms (${reason}).`);
}

function connectToBlitzortungEndpoint(url) {
    let state = upstreamState.get(url);
    if (!state) {
        state = {
            url,
            ws: null,
            backoffMs: BLITZ_BASE_RECONNECT_MS,
            connectedAt: 0,
            lastMessageAt: 0
        };
        upstreamState.set(url, state);
    }

    if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
        return;
    }

    const wsBlitz = new WebSocket(url);
    state.ws = wsBlitz;

    wsBlitz.on('open', () => {
        state.backoffMs = BLITZ_BASE_RECONNECT_MS;
        state.connectedAt = Date.now();
        state.lastMessageAt = Date.now();
        logInfo(`[Blitzortung] Connected to ${url}.`);
        wsBlitz.send(JSON.stringify({ a: 111 }));
    });

    wsBlitz.on('message', (data) => {
        state.lastMessageAt = Date.now();
        try {
            const decodedText = decodeBlitzortung(data.toString('utf8'));
            const strike = JSON.parse(decodedText);

            if (!strike || !Number.isFinite(strike.lat) || !Number.isFinite(strike.lon)) {
                return;
            }

            const timestamp = extractStrikeTimestamp(strike);
            broadcastStrike(strike.lat, strike.lon, timestamp);
        } catch (err) {
            if (shouldLogDecodeError()) {
                logError(`[Blitzortung] Decode/parse error from ${url}`, err);
            }
        }
    });

    wsBlitz.on('close', () => {
        if (state.ws === wsBlitz) {
            state.ws = null;
        }
        scheduleReconnect(state, 'socket closed');
    });

    wsBlitz.on('error', (err) => {
        logError(`[Blitzortung] Socket error on ${url}`, err);
        wsBlitz.close();
    });
}

BLITZ_ENDPOINTS.forEach(connectToBlitzortungEndpoint);

setInterval(() => {
    const now = Date.now();
    upstreamState.forEach((state) => {
        if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
        if (now - state.lastMessageAt > BLITZ_STALE_MS) {
            logWarn(`[Blitzortung] Stale feed detected on ${state.url}; forcing reconnect.`);
            state.ws.terminate();
            state.ws = null;
            scheduleReconnect(state, 'stale feed watchdog');
        }
    });
}, 10000);

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => {
        ws.isAlive = true;
    });
    ws.send(JSON.stringify({ type: 'history', data: strikeCache }));
});

const clientHeartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            ws.terminate();
            return;
        }

        ws.isAlive = false;
        ws.ping();
    });
}, CLIENT_PING_INTERVAL_MS);

wss.on('close', () => {
    clearInterval(clientHeartbeat);
});

setInterval(() => {
    const connectedUpstreams = Array.from(upstreamState.values()).filter(
        state => state.ws && state.ws.readyState === WebSocket.OPEN
    ).length;
    const secondsSinceLastStrike = lastBroadcastAt ? Math.floor((Date.now() - lastBroadcastAt) / 1000) : 'n/a';
    logInfo(
        `[Status] Upstreams: ${connectedUpstreams}/${BLITZ_ENDPOINTS.length}. Active clients: ${wss.clients.size}. ` +
        `Tiles: ${tileCache.size}. Strikes: ${strikeCache.length}. Last strike: ${secondsSinceLastStrike}s ago.`
    );

    if (connectedUpstreams === 0) {
        logError('[Status] All Blitzortung upstream sockets are disconnected.');
    }
    if (typeof secondsSinceLastStrike === 'number' && secondsSinceLastStrike > 300) {
        logWarn(`[Status] No strikes broadcast in ${secondsSinceLastStrike}s.`);
    }
}, 60000);

function shutdownGracefully(signalName) {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log(`[Shutdown] Received ${signalName}. Saving state and closing sockets.`);
    saveStrikesToDisk();

    upstreamState.forEach((state) => {
        if (state.ws) {
            state.ws.terminate();
            state.ws = null;
        }
    });

    wss.clients.forEach((client) => {
        try {
            client.close();
        } catch (err) {}
    });

    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 2000);
}

process.on('SIGINT', () => shutdownGracefully('SIGINT'));
process.on('SIGTERM', () => shutdownGracefully('SIGTERM'));

process.on('uncaughtException', (err) => {
    logError('Uncaught exception', err);
});

process.on('unhandledRejection', (reason) => {
    if (reason instanceof Error) {
        logError('Unhandled rejection', reason);
        return;
    }
    logError(`Unhandled rejection: ${JSON.stringify(reason)}`);
});

server.listen(3000, () => {
    console.log('Weather dashboard serving on port 3000');
});