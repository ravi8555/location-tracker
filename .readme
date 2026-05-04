# 📍 Location Tracker App (OIDC Secured)

A real-time location tracking application built with **Node.js, Socket.IO, Kafka, and PostgreSQL**, secured using **OIDC (OpenID Connect) with PKCE and HTTP-only cookies**.

---

## 🚀 Overview

This application allows users to share their live location — **only after authenticating via an OIDC Auth Server**.

🔐 **Authentication is mandatory**
📡 **Real-time updates using WebSockets + Kafka**
🗺️ **Live map visualization**

---

## 🔐 Authentication Flow (OIDC + PKCE)

Before accessing the app, every user must authenticate via the OIDC server.

### Flow:

```
User → Location Tracker → OIDC Auth Server
     → Login → Authorization Code → Token Exchange
     → Secure Cookie Set → Access Granted
```

### Key Features:

* ✅ PKCE (Proof Key for Code Exchange)
* ✅ Authorization Code Flow
* ✅ HTTP-only secure cookies (no token exposure in frontend)
* ✅ `/userinfo` endpoint for user identity
* ✅ Refresh token support

---

## 🧠 Architecture

```
Frontend (Browser)
   ↓
OIDC Auth Server (Vercel / Local)
   ↓
Location Tracker Backend (Node.js + Socket.IO)
   ↓
Kafka → Database (PostgreSQL)
```

---

## 📁 Project Structure

```
location-tracker/
├── public/                # Frontend (HTML, JS, Map UI)
├── src/
│   ├── index.js           # Main server (Express + Socket.IO)
│   ├── kafka-client.js
│   ├── database/
│   ├── services/
│   └── processors/
├── .env
├── package.json
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository

```bash
git clone <your-repo-url>
cd location-tracker
```

---

### 2️⃣ Install Dependencies

```bash
npm install
```

---

### 3️⃣ Setup Environment Variables

Create a `.env` file:

```env
PORT=8001

# OIDC
AUTH_SERVER=https://oidc-auth-module.vercel.app

# Kafka
KAFKA_BROKERS=localhost:9092

# Database
DATABASE_URL=postgresql://admin:admin@localhost:5433/location_tracker
```

---

### 4️⃣ Start Services

#### Start Kafka + DB (Docker)

```bash
docker-compose up -d
```

#### Start App

```bash
npm run dev
```

---

## 🌐 Access App

```
http://localhost:8001
```

---

## 🔐 Important: Authentication Required

🚫 Users **CANNOT** access the app directly.

When user opens the app:

1. Redirected to OIDC Auth Server
2. Login / Signup required
3. After success → redirected back with authorization code
4. Token exchanged → cookie set
5. Access granted

---

## 🔑 OIDC Configuration

The app uses:

* Authorization Endpoint:
  `/o/authenticate`

* Token Endpoint:
  `/o/token`

* User Info Endpoint:
  `/o/userinfo`

* JWKS:
  `/.well-known/jwks.json`

---

## 🍪 Cookie Security

* `httpOnly: true`
* `secure: true`
* `sameSite: "none"` (required for cross-origin)

---

## 📡 Real-Time Features

* Live user location tracking
* WebSocket communication via Socket.IO
* Kafka-based event streaming
* Multi-user tracking support

---

## 🗺️ Features

* 📍 Share real-time location
* 👥 Track multiple users
* 🔐 Secure authentication (OIDC)
* ⚡ Fast streaming with Kafka
* 📊 Scalable architecture

---

## 🧪 Tech Stack

* **Frontend:** HTML, JS, Leaflet.js
* **Backend:** Node.js, Express
* **Realtime:** Socket.IO
* **Streaming:** Kafka
* **Database:** PostgreSQL
* **Auth:** OIDC (Custom Auth Server)

---

## 🚨 Security Notes

* No tokens stored in localStorage
* Uses HTTP-only cookies
* PKCE prevents code interception
* JWT verified via JWKS

---

## 🧠 Future Improvements

* 🔄 Refresh token rotation UI
* 📊 Admin dashboard
* 🧑‍💼 Multi-tenant support
* 📜 Consent screen (like Google login)
* 🔑 Key rotation (JWKS multiple keys)

---

## 👨‍💻 Author

**Ravindra Dhadave**

---

## ⭐ Final Note

> This app enforces strict authentication:
> **No login → No location tracking**

---
