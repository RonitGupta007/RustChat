# 🦀 RustChat — Real-time Chat Application

A full-stack real-time chat app built for a **Rust minor project**.

**Backend**: Pure Rust using `tokio` (async runtime) + `tokio-tungstenite` (WebSockets)  
**Frontend**: Vanilla HTML/CSS/JS — no npm, no framework, just open the file

---

## 🚀 Quick Start

### 1. Prerequisites
- [Rust + Cargo](https://rustup.rs/) installed

### 2. Run the server
```bash
cargo run
```
Server starts on `ws://localhost:8080`

### 3. Open the frontend
Open `index.html` in **two or more browser tabs**.  
No web server needed — it's a static file.

### 4. Chat!
- Enter a username + room name → **Connect**
- Open multiple tabs to simulate multiple users

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Browser Clients                       │
│   Tab 1 (Alice)    Tab 2 (Bob)    Tab 3 (Charlie)       │
│       │                │                │               │
│       └────────────────┴────────────────┘               │
│                         │ WebSocket                      │
└─────────────────────────┼───────────────────────────────┘
                          │
┌─────────────────────────┼───────────────────────────────┐
│              Rust Server (tokio async)                   │
│                         │                               │
│    ┌────────────────────▼──────────────────────┐        │
│    │            TcpListener :8080               │        │
│    └────────────────────┬──────────────────────┘        │
│                         │ accept_async()                 │
│    ┌────────────────────▼──────────────────────┐        │
│    │         Per-Connection Handler             │        │
│    │   (tokio::spawn → independent task)        │        │
│    └───┬──────────────────┬────────────────────┘        │
│        │                  │                             │
│   ┌────▼────┐       ┌─────▼────┐                        │
│   │ Inbound │       │ Outbound │                        │
│   │ Task    │       │ Task     │                        │
│   └────┬────┘       └─────▲────┘                        │
│        │                  │                             │
│   ┌────▼──────────────────┴────┐                        │
│   │   Shared State (RwLock)    │                        │
│   │   HashMap<UserId, User>    │                        │
│   ├────────────────────────────┤                        │
│   │  broadcast::Sender         │  (room messages)       │
│   │  dm_tx::Sender             │  (private messages)    │
│   └────────────────────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

---

## ✨ Features

| Feature | Description |
|---|---|
| **Multi-room** | Join any room by name; users only see messages in their room |
| **Private DMs** | Click a user OR type `/dm <user> <message>` |
| **Typing indicators** | Live "Alice is typing…" with animated dots |
| **User list** | Real-time online users sidebar |
| **Join/leave events** | System notifications when users connect/disconnect |
| **Online count** | Live member count per room |

---

## 📦 Dependencies

```toml
tokio            # Async runtime
tokio-tungstenite # WebSocket server
futures-util     # Stream/Sink combinators
serde + serde_json # JSON serialization
uuid             # Unique message/user IDs
chrono           # Timestamps
tracing          # Structured logging
```

---

## 🔌 Message Protocol

All messages are JSON over WebSocket.

### Client → Server
```json
{ "type": "join",    "username": "alice", "room": "general" }
{ "type": "chat",    "content": "Hello!" }
{ "type": "private_message", "to": "bob", "content": "Hey" }
{ "type": "typing_start" }
{ "type": "typing_stop" }
{ "type": "ping" }
```

### Server → Client
```json
{ "type": "welcome",       "user_id": "...", "username": "alice", "room": "general" }
{ "type": "chat",          "message_id": "...", "username": "bob", "content": "...", "timestamp": "..." }
{ "type": "user_joined",   "username": "...", "online_count": 3, ... }
{ "type": "user_left",     "username": "...", "online_count": 2, ... }
{ "type": "user_list",     "users": [...] }
{ "type": "typing_update", "username": "bob", "is_typing": true }
{ "type": "private_message", "from_username": "bob", "content": "...", ... }
{ "type": "error",         "message": "..." }
```

---

## 🔬 Key Rust Concepts Used

- **`tokio::spawn`** — Spawn independent async tasks per connection
- **`broadcast::channel`** — Fan-out room messages to all room members
- **`mpsc::unbounded_channel`** — Per-connection outbound message queue
- **`Arc<RwLock<HashMap>>`** — Shared mutable state across async tasks
- **`futures_util::StreamExt / SinkExt`** — Async WebSocket read/write
- **`serde` + `#[serde(tag="type")]`** — Enum-based JSON message dispatch
