const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Broker Metrics Tracker
const metrics = {
    totalConnections: 0,
    messagesPublished: 0,
    activeTopics: 0
};

const subscriptions = new Map();

// Serve basic dashboard metrics via REST API
app.get('/api/metrics', (req, res) => {
    metrics.totalConnections = wss.clients.size;
    metrics.activeTopics = subscriptions.size;
    res.json(metrics);
});

// Embedded Dashboard HTML UI
app.get('/', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>Broker Admin Dashboard</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body { font-family: -apple-system, sans-serif; background: #121212; color: #fff; padding: 20px; }
                .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 15px; margin-top: 20px; }
                .card { background: #1e1e1e; padding: 20px; border-radius: 8px; text-align: center; border: 1px solid #333; }
                h1 { color: #00ff88; margin-bottom: 5px; }
                .num { font-size: 2rem; font-weight: bold; margin-top: 10px; color: #00ff88; }
            </style>
        </head>
        <body>
            <h1>Broker Analytics</h1>
            <p>Status: Live (Termux Backend)</p>
            <div class="grid">
                <div class="card">Active Connections<div class="num" id="conns">0</div></div>
                <div class="card">Topics Maintained<div class="num" id="topics">0</div></div>
                <div class="card">Total Messages Routed<div class="num" id="msgs">0</div></div>
            </div>
            <script>
                async function updateMetrics() {
                    try {
                        const res = await fetch('/api/metrics');
                        const data = await res.json();
                        document.getElementById('conns').innerText = data.totalConnections;
                        document.getElementById('topics').innerText = data.activeTopics;
                        document.getElementById('msgs').innerText = data.messagesPublished;
                    } catch (e) {}
                }
                setInterval(updateMetrics, 1000);
                updateMetrics();
            </script>
        </body>
        </html>
    `);
});

wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (messageBuffer) => {
        try {
            const payload = JSON.parse(messageBuffer.toString());
            const { action, topic, message } = payload;

            if (action === 'subscribe' && topic) {
                if (!subscriptions.has(topic)) subscriptions.set(topic, new Set());
                subscriptions.get(topic).add(ws);
                ws.send(JSON.stringify({ event: 'subscribed', topic }));
            }
            else if (action === 'publish' && topic && message) {
                metrics.messagesPublished++; // Increment UI metric count
                const subscribers = subscriptions.get(topic);
                let deliveryCount = 0;
                if (subscribers) {
                    subscribers.forEach((client) => {
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify({ event: 'message', topic, data: message }));
                            deliveryCount++;
                        }
                    });
                }
                ws.send(JSON.stringify({ event: 'published', topic, deliveredTo: deliveryCount }));
            }
        } catch (err) {}
    });

    ws.on('close', () => {
        subscriptions.forEach((clients, topic) => {
            clients.delete(ws);
            if (clients.size === 0) subscriptions.delete(topic);
        });
    });
});

const PORT = 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(\`🚀 Broker & Dashboard Server running on http://localhost:\${PORT}\`);
});

