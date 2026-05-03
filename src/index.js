import http from 'node:http';
import express from 'express';
import { Server } from 'socket.io';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { kafkaClient, checkKafkaConnection } from './kafka-client.js';
import jwt from 'jsonwebtoken';
import 'dotenv/config';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Auth configuration
const AUTH_SERVER_URL = process.env.AUTH_SERVER_URL || 'http://localhost:8000';
const JWT_PUBLIC_KEY = process.env.JWT_PUBLIC_KEY || null;

// async function verifyToken(token) {
//     try {
//         // For development, you can use a simple verification
//         // In production, properly verify with JWKS
//         const decoded = jwt.decode(token);
//         if (!decoded) return null;
        
//         // For now, just return the decoded token
//         // You should implement proper verification
//         return decoded;
//     } catch (error) {
//         console.error('Token verification failed:', error);
//         return null;
//     }
// }
async function verifyToken(token) {
    try {
        // First decode the token
        const decoded = jwt.decode(token);
        if (!decoded) return null;
        
        // If token already has email and name, use it
        if (decoded.email && decoded.name) {
            console.log('Token contains user info:', { email: decoded.email, name: decoded.name });
            return decoded;
        }
        
        // Otherwise, fetch user info from OIDC server
        try {
            const response = await fetch(`${AUTH_SERVER_URL}/o/userinfo`, {
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });
            
            if (response.ok) {
                const userInfo = await response.json();
                console.log('Fetched user info from OIDC:', userInfo);
                
                // Merge token and userinfo
                return {
                    ...decoded,
                    email: userInfo.email,
                    name: userInfo.name || userInfo.given_name,
                    given_name: userInfo.given_name,
                    family_name: userInfo.family_name
                };
            }
        } catch (userInfoError) {
            console.error('Failed to fetch userinfo:', userInfoError);
        }
        
        return decoded;
    } catch (error) {
        console.error('Token verification failed:', error);
        return null;
    }
}
async function main() {
    const PORT = process.env.PORT ?? 8001;
    const app = express();
    const server = http.createServer(app);
    const io = new Server(server, {
        cors: {
            origin: "*",
            methods: ["GET", "POST"]
        }
    });

    // Check Kafka connection
    const kafkaReady = await checkKafkaConnection();
    if (!kafkaReady) {
        console.warn('⚠️  Kafka not available, running without Kafka');
    }

    let kafkaProducer = null;
    let kafkaConsumer = null;

    if (kafkaReady) {
        kafkaProducer = kafkaClient.producer();
        await kafkaProducer.connect();
        console.log('✅ Kafka producer connected');

        kafkaConsumer = kafkaClient.consumer({
            groupId: `socket-server-${PORT}`
        });
        await kafkaConsumer.connect();
        await kafkaConsumer.subscribe({
            topics: ['location-update'],
            fromBeginning: false
        });
        console.log('✅ Kafka consumer connected');

        kafkaConsumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat }) => {
                try {
                    const data = JSON.parse(message.value.toString());
                    console.log(`Broadcasting location update for user:`, data.email);
                    io.emit('server:location:update', {
                        userId: data.userId,
                        email: data.email,
                        name: data.name,
                        latitude: data.latitude,
                        longitude: data.longitude,
                        timestamp: data.timestamp
                    });
                    await heartbeat();
                } catch (error) {
                    console.error('Error processing kafka message:', error);
                }
            }
        });
    }

    // Store user info for each socket
    const userSockets = new Map();
    const connectedUsers = new Map();

    // Authentication middleware for Socket.IO
    // io.use(async (socket, next) => {
    //     try {
    //         const token = socket.handshake.auth.token || socket.handshake.headers.authorization?.split(' ')[1];
            
    //         if (!token) {
    //             return next(new Error('Authentication required'));
    //         }
            
    //         const userData = await verifyToken(token);
            
    //         if (!userData) {
    //             return next(new Error('Invalid token'));
    //         }
            
    //         socket.userData = {
    //             userId: userData.sub || userData.id,
    //             email: userData.email,
    //             name: userData.name || userData.given_name,
    //             picture: userData.picture
    //         };
            
    //         next();
    //     } catch (error) {
    //         console.error('Socket auth error:', error);
    //         next(new Error('Authentication failed'));
    //     }
    // });

    // Authentication middleware for Socket.IO
io.use(async (socket, next) => {
    try {
        const token = socket.handshake.auth.token || socket.handshake.headers.authorization?.split(' ')[1];
        
        if (!token) {
            return next(new Error('Authentication required'));
        }
        
        // Debug: Decode and log token
        const decoded = jwt.decode(token);
        console.log('=== Socket Auth Debug ===');
        console.log('Raw token (first 100 chars):', token.substring(0, 100));
        console.log('Decoded token payload:', JSON.stringify(decoded, null, 2));
        console.log('Available fields:', Object.keys(decoded || {}));
        
        const userData = await verifyToken(token);
        
        if (!userData) {
            return next(new Error('Invalid token'));
        }
        
        // Extract user info from token - try multiple field names
        socket.userData = {
            userId: userData.sub || userData.id || userData.userId,
            email: userData.email || userData.email_verified || userData.preferred_username,
            name: userData.name || userData.given_name || userData.preferred_username || userData.email,
            picture: userData.picture
        };
        
        console.log('Extracted user data:', socket.userData);
        console.log('===========================');
        
        next();
    } catch (error) {
        console.error('Socket auth error:', error);
        next(new Error('Authentication failed'));
    }
});

    io.on('connection', async (socket) => {
        const { userId, email, name } = socket.userData;
        
        console.log(`User ${email} (${userId}) connected with socket ${socket.id}`);
        
        userSockets.set(socket.id, { userId, email, name });
        connectedUsers.set(userId, { socketId: socket.id, userInfo: { userId, email, name } });
        
        const usersList = Array.from(connectedUsers.values()).map(u => ({
            userId: u.userInfo.userId,
            email: u.userInfo.email,
            name: u.userInfo.name
        }));
        socket.emit('server:users:list', usersList);
        
        socket.broadcast.emit('server:user:connected', {
            userId,
            email,
            name,
            socketId: socket.id
        });

        socket.on('client:location:update', async (locationData) => {
    const { latitude, longitude } = locationData;
    const { userId, email, name } = socket.userData;
    
    if (!latitude || !longitude) {
        console.error(`Invalid location data`);
        return;
    }
    
    // Skip if no email (client-only users)
    if (!email) {
        console.log(`Skipping location update for client-only user: ${userId}`);
        return;
    }
    
    const userEmail = email;
    const userName = name || email;
    
    console.log(`User ${userEmail} (${userId}) location update:`, { latitude, longitude });
    
    if (kafkaProducer) {
        try {
            await kafkaProducer.send({
                topic: 'location-update',
                messages: [{
                    key: userId,
                    value: JSON.stringify({
                        userId: userId,
                        email: userEmail,
                        name: userName,
                        latitude: latitude,
                        longitude: longitude,
                        timestamp: new Date().toISOString(),
                        socketId: socket.id
                    }),
                }],
            });
        } catch (error) {
            console.error('Failed to send to Kafka:', error);
        }
    } else {
        io.emit('server:location:update', {
            userId: userId,
            email: userEmail,
            name: userName,
            latitude: latitude,
            longitude: longitude,
            timestamp: new Date().toISOString()
        });
    }
});

//         socket.on('client:location:update', async (locationData) => {
//     const { latitude, longitude, email, name } = locationData;
//     const { userId } = socket.userData; // Get from authenticated socket
    
//     if (!latitude || !longitude) {
//         console.error(`Invalid location data from ${email || userId}`);
//         return;
//     }
    
//     // Use email from socket userData or from locationData
//     const userEmail = email || socket.userData.email;
//     const userName = name || socket.userData.name;
    
//     console.log(`User ${userEmail} (${userId}) location update:`, { latitude, longitude });
    
//     if (kafkaProducer) {
//         try {
//             await kafkaProducer.send({
//                 topic: 'location-update',
//                 messages: [{
//                     key: userId,
//                     value: JSON.stringify({
//                         userId: userId,
//                         email: userEmail,
//                         name: userName,
//                         latitude: latitude,
//                         longitude: longitude,
//                         timestamp: new Date().toISOString(),
//                         socketId: socket.id
//                     }),
//                 }],
//             });
//         } catch (error) {
//             console.error('Failed to send to Kafka:', error);
//         }
//     } else {
//         // If no Kafka, broadcast directly
//         io.emit('server:location:update', {
//             userId: userId,
//             email: userEmail,
//             name: userName,
//             latitude: latitude,
//             longitude: longitude,
//             timestamp: new Date().toISOString()
//         });
//     }
// });

        // socket.on('client:location:update', async (locationData) => {
        //     const { latitude, longitude } = locationData;
            
        //     if (!latitude || !longitude) {
        //         console.error(`Invalid location data from ${email}`);
        //         return;
        //     }
            
        //     console.log(`User ${email} (${userId}) location update:`, { latitude, longitude });
            
        //     if (kafkaProducer) {
        //         try {
        //             await kafkaProducer.send({
        //                 topic: 'location-update',
        //                 messages: [{
        //                     key: userId,
        //                     value: JSON.stringify({
        //                         userId: userId,
        //                         email: email,
        //                         name: name,
        //                         latitude: latitude,
        //                         longitude: longitude,
        //                         timestamp: new Date().toISOString(),
        //                         socketId: socket.id
        //                     }),
        //                 }],
        //             });
        //         } catch (error) {
        //             console.error('Failed to send to Kafka:', error);
        //         }
        //     } else {
        //         // If no Kafka, broadcast directly
        //         io.emit('server:location:update', {
        //             userId: userId,
        //             email: email,
        //             name: name,
        //             latitude: latitude,
        //             longitude: longitude,
        //             timestamp: new Date().toISOString()
        //         });
        //     }
        // });
        
        socket.on('disconnect', () => {
            console.log(`User ${email} (${userId}) disconnected`);
            userSockets.delete(socket.id);
            connectedUsers.delete(userId);
            
            socket.broadcast.emit('server:user:disconnected', {
                userId,
                email,
                name
            });
        });
    });

    server.listen(PORT, () => {
        console.log(`✅ Location Tracker Server running on http://localhost:${PORT}`);
        console.log(`📊 Health check: http://localhost:${PORT}/health`);
        console.log(`🗺️  Frontend: http://localhost:${PORT}`);
    });

    // REST endpoints
    app.get('/health', (req, res) => {
        res.json({
            message: 'OK',
            success: true,
            connectedUsers: connectedUsers.size,
            kafkaConnected: kafkaProducer ? kafkaProducer.connected : false
        });
    });
    
    app.get('/api/users', async (req, res) => {
        const authHeader = req.headers.authorization;
        
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return res.status(401).json({ error: 'Unauthorized' });
        }
        
        const token = authHeader.split(' ')[1];
        const userData = await verifyToken(token);
        
        if (!userData) {
            return res.status(401).json({ error: 'Invalid token' });
        }
        
        const users = Array.from(connectedUsers.values()).map(u => ({
            userId: u.userInfo.userId,
            email: u.userInfo.email,
            name: u.userInfo.name
        }));
        
        res.json({ users });
    });

    // Serve static files - FIXED PATH
    const publicPath = path.join(__dirname, '../public');
    console.log(`Serving static files from: ${publicPath}`);
    app.use(express.static(publicPath));
    
    // Fallback route for SPA
    app.get('*', (req, res) => {
        res.sendFile(path.join(publicPath, 'index.html'));
    });
}

main().catch(console.error);