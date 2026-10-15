const net = r0p⁰equire('net');

// Store active client connections categorized by topic subscriptions
const subscriptions = {};

const server = net.createServer((socket) => {
    socket.setEncoding('utf8');
    let currentId = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`Client connected: ${currentId}`);

    socket.on('data', (data) => {
        try {
            // Expected JSON payload format: { "action": "subscribe"|"publish", "topic": "news", "message": "hello" }
            const payload = JSON.parse(data.trim());
            const { action, topic, message } = payload;

            if (action === 'subscribe' && topic) {
                if (!subscriptions[topic]) subscriptions[topic] = [];
                if (!subscriptions[topic].includes(socket)) {
                    subscriptions[topic].push(socket);
                }
                socket.write(JSON.stringify({ status: `Subscribed to ${topic}` }) + '\n');
            }

            else if (action === 'publish' && topic && message) {
                if (subscriptions[topic]) {
                    subscriptions[topic].forEach((client) => {
                        if (client !== socket && !client.destroyed) {
                            client.write(JSON.stringify({ topic, message }) + '\n');
                        }
                    });
                }
                socket.write(JSON.stringify({ status: "Message routed" }) + '\n');
            }
        } catch (err) {
            socket.write(JSON.stringify({ error: "Invalid JSON format" }) + '\n');
        }
    });

    socket.on('close', () => {
        console.log(`Client disconnected: ${currentId}`);
        // Clean up dead subscriptions
        Object.keys(subscriptions).forEach(topic => {
            subscriptions[topic] = subscriptions[topic].filter(client => client !== socket);
        });
    });

    socket.on('error', (err) => {
        console.error(`Socket error: ${err.message}`);
    });
});

const PORT = 8080;
server.listen(PORT, '127.0.0.1', () => {
    console.log(`Broker running locally on port ${PORT}`);
});

