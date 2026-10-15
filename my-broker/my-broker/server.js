const { WebSocketServer, WebSocket } = require('ws');

// Initialize WebSocket server on port 8080
const wss = new WebSocketServer({ port: 8080 }, () => {
    console.log('🚀 Production WebSocket Broker running on ws://localhost:8080');
});

// Map to track which clients are subscribed to which topics
// Structure: { "topic_name": Set(WebSocket_Clients) }
const subscriptions = new Map();

wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    console.log(`🔌 New client connected from: ${clientIp}`);

    // Setup heartbeat property to track connection health
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (messageBuffer) => {
        try {
            // Parse incoming JSON packet safely
            const payload = JSON.parse(messageBuffer.toString());
            const { action, topic, message } = payload;

            if (action === 'subscribe' && topic) {
                if (!subscriptions.has(topic)) {
                    subscriptions.set(topic, new Set());
                }
                subscriptions.get(topic).add(ws);

                ws.send(JSON.stringify({ event: 'subscribed', topic }));
                console.log(`📥 Client subscribed to topic: [${topic}]`);
            }

            else if (action === 'publish' && topic && message) {
                const subscribers = subscriptions.get(topic);
                let deliveryCount = 0;

                if (subscribers) {
                    subscribers.forEach((client) => {
                        // Deliver message only to open connections, excluding the sender
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify({ event: 'message', topic, data: message }));
                            deliveryCount++;
                        }
                    });
                }

                ws.send(JSON.stringify({ event: 'published', topic, deliveredTo: deliveryCount }));
                console.log(`📤 Broadcasted to [${topic}]: "${message}" (${deliveryCount} peers)`);
            }

            else {
                ws.send(JSON.stringify({ error: 'Unknown action or missing parameters' }));
            }
        } catch (err) {
            ws.send(JSON.stringify({ error: 'Invalid JSON payload protocol' }));
        }
    });

    // Clean up tracking mappings when client disconnects
    ws.on('close', () => {
        console.log(`❌ Client disconnected`);
        subscriptions.forEach((clients, topic) => {
            clients.delete(ws);
            if (clients.size === 0) subscriptions.delete(topic); // Memory optimization
        });
    });
});

// Heartbeat Interval: Guard against ghost connections every 30 seconds
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.log('⚠️ Terminating unresponsive client connection');
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => {
    clearInterval(interval);
});

