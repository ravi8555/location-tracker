# Architecture
This is a real-time location sharing system built with a microservices architecture. Users can authenticate via OIDC, share their GPS location in real-time, and see other online users on a map.

Step 1: Start Infrastructure Services

# Start PostgreSQL (both instances) and Kafka
docker-compose up -d

# Verify services are running
docker-compose ps

This starts:

PostgreSQL OIDC on port 5432

PostgreSQL Tracker on port 5433

Kafka on port 9092

PGAdmin on port 5050

Step 2: Configure Environment Variables
Create .env file:

# Server Configuration
PORT=8001
AUTH_SERVER_URL=http://localhost:8000

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=location-tracker

# Database Configuration (Tracker DB)
DB_HOST=localhost
DB_PORT=5433
DB_USER=admin
DB_PASSWORD=admin
DB_NAME=location_tracker
DB_POOL_MAX=20
DB_POOL_MIN=2

Step 3: Initialize Kafka Topics
npm run kafka-admin

Step 4: Initialize Database Schema
npm run db:init

This creates all necessary tables in the location tracker database.

Step 5: Start the Tracker Server

# Terminal 1 - Main WebSocket server
npm start

# Terminal 2 - Database processor (Kafka consumer)
npm run db-processor

#Step 7: Access the Application
Open browser to: http://localhost:8001


# Authentication Flow
User visits http://localhost:8001

No token found → Redirects to Auth Server: http://localhost:8000/o/authenticate?client_id=test_client_123&redirect_uri=...&response_type=code

User logs in/signs up on Auth Server

Auth Server redirects back with ?code=...

Frontend exchanges code for token via /o/token endpoint

Token stored in localStorage, used for WebSocket authentication

# WebSocket Events
Client → Server
Event	Payload	Description
client:location:update	{ latitude, longitude, email, name }	Send current GPS location
Server → Client
Event	Payload	Description
server:users:list	[{ userId, email, name }]	Initial list of online users
server:user:connected	{ userId, email, name, socketId }	New user came online
server:user:disconnected	{ userId, email, name }	User went offline
server:location:update	{ userId, email, name, latitude, longitude, timestamp }	User location update
Database Schema


tracking.location_history
Column	Type	Description
id	BIGSERIAL PRIMARY KEY	Auto-increment
user_id	VARCHAR(255) FK	References users
latitude	DOUBLE PRECISION	GPS latitude
longitude	DOUBLE PRECISION	GPS longitude
accuracy	DOUBLE PRECISION	GPS accuracy (meters)
altitude	DOUBLE PRECISION	Altitude (meters)
speed	DOUBLE PRECISION	Speed (m/s)
heading	DOUBLE PRECISION	Direction (degrees)
location_timestamp	TIMESTAMP	When location was captured
tracking.user_sessions
Column	Type	Description
id	UUID PRIMARY KEY	Session identifier
user_id	VARCHAR(255) FK	References users
socket_id	VARCHAR(255)	Socket.IO connection ID
started_at	TIMESTAMP	Session start
ended_at	TIMESTAMP	Session end
is_active	BOOLEAN	Currently active
API Endpoints (Tracker Server)
Endpoint	Method	Description
/health	GET	Health check with connected user count
/api/users	GET	Get list of online users (requires Bearer token)
/*	GET	Serves frontend HTML
Development Commands
bash
# Run tracker server with auto-restart
npm run dev

# Run database processor only
npm run db-processor

# Initialize Kafka topics
npm run kafka-admin

# Initialize database schema
npm run db:init

# Run migrations (if schema changes)
npm run db:migrate

Database Connection Issues
bash
# Test connection to tracker DB
psql -h localhost -p 5433 -U admin -d location_tracker

# Test connection to OIDC DB
psql -h localhost -p 5432 -U admin -d oidc_auth
Socket Connection Issues
Check browser console for errors

Verify token is being sent in auth handshake

Ensure CORS is configured correctly

JWT Token Format Expected
The tracker server expects a JWT containing at minimum:

json
{
  "sub": "user-id",
  "email": "user@example.com",
  "name": "User Name"
}
Or it will fetch user info from ${AUTH_SERVER_URL}/o/userinfo endpoint.
