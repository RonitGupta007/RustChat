use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────
//  Data types
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Join { username: String, room: String },
    Chat { content: String },
    PrivateMessage { to: String, content: String },
    TypingStart,
    TypingStop,
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Welcome {
        user_id: String,
        username: String,
        room: String,
    },
    UserJoined {
        user_id: String,
        username: String,
        room: String,
        timestamp: String,
        online_count: usize,
    },
    UserLeft {
        user_id: String,
        username: String,
        room: String,
        timestamp: String,
        online_count: usize,
    },
    Chat {
        message_id: String,
        user_id: String,
        username: String,
        content: String,
        room: String,
        timestamp: String,
    },
    PrivateMessage {
        message_id: String,
        from_id: String,
        from_username: String,
        content: String,
        timestamp: String,
    },
    UserList {
        users: Vec<UserInfo>,
    },
    TypingUpdate {
        user_id: String,
        username: String,
        room: String,
        is_typing: bool,
    },
    Error {
        message: String,
    },
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserInfo {
    user_id: String,
    username: String,
    room: String,
}

#[derive(Debug, Clone)]
struct ConnectedUser {
    user_id: String,
    username: String,
    room: String,
    addr: SocketAddr,
}

// ─────────────────────────────────────────────────────────────
//  Shared state
// ─────────────────────────────────────────────────────────────

type Users = Arc<RwLock<HashMap<String, ConnectedUser>>>;

struct AppState {
    users: Users,
    broadcast_tx: broadcast::Sender<(String, ServerMessage)>, // (room, msg)
    dm_tx: broadcast::Sender<(String, ServerMessage)>,        // (user_id, msg)
}

// ─────────────────────────────────────────────────────────────
//  Entry point
// ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind");

    info!("🦀 Rust Chat Server running on ws://{}", addr);
    info!("   Open index.html in your browser to connect.");

    let (broadcast_tx, _) = broadcast::channel::<(String, ServerMessage)>(512);
    let (dm_tx, _) = broadcast::channel::<(String, ServerMessage)>(512);

    let state = Arc::new(AppState {
        users: Arc::new(RwLock::new(HashMap::new())),
        broadcast_tx,
        dm_tx,
    });

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("New TCP connection from {}", addr);
                let state = Arc::clone(&state);
                tokio::spawn(handle_connection(stream, addr, state));
            }
            Err(e) => error!("Accept error: {}", e),
        }
    }
}

// ─────────────────────────────────────────────────────────────
//  Per-connection handler
// ─────────────────────────────────────────────────────────────

async fn handle_connection(stream: TcpStream, addr: SocketAddr, state: Arc<AppState>) {
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("WebSocket handshake failed for {}: {}", addr, e);
            return;
        }
    };

    info!("WebSocket connected: {}", addr);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let user_id = Uuid::new_v4().to_string();
    let mut current_user: Option<ConnectedUser> = None;

    // Subscribe to broadcast and DM channels
    let mut broadcast_rx = state.broadcast_tx.subscribe();
    let mut dm_rx = state.dm_tx.subscribe();
    let uid_for_tasks = user_id.clone();

    // ── Outbound task: forward broadcast/DM messages to this WS ──
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
    let outbound_tx_bc = outbound_tx.clone();
    let outbound_tx_dm = outbound_tx.clone();

    // Broadcast listener
    let uid_bc = uid_for_tasks.clone();
    let users_bc = Arc::clone(&state.users);
    tokio::spawn(async move {
        while let Ok((room, msg)) = broadcast_rx.recv().await {
            // Only forward if this user is in that room
            let users = users_bc.read().await;
            if let Some(u) = users.get(&uid_bc) {
                if u.room == room {
                    let _ = outbound_tx_bc.send(msg);
                }
            }
        }
    });

    // DM listener
    let uid_dm = uid_for_tasks.clone();
    tokio::spawn(async move {
        while let Ok((target_id, msg)) = dm_rx.recv().await {
            if target_id == uid_dm {
                let _ = outbound_tx_dm.send(msg);
            }
        }
    });

    // Outbound writer
    tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if ws_sender.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    // ── Inbound: process messages from client ──
    while let Some(raw) = ws_receiver.next().await {
        match raw {
            Ok(Message::Text(text)) => {
                let parsed: Result<ClientMessage, _> = serde_json::from_str(&text);
                match parsed {
                    Ok(client_msg) => {
                        handle_client_message(
                            client_msg,
                            &user_id,
                            &mut current_user,
                            &state,
                            &outbound_tx,
                            addr,
                        )
                        .await;
                    }
                    Err(e) => {
                        let _ = outbound_tx.send(ServerMessage::Error {
                            message: format!("Invalid message format: {}", e),
                        });
                    }
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    // ── Cleanup on disconnect ──
    if let Some(user) = current_user {
        let mut users = state.users.write().await;
        users.remove(&user.user_id);
        let count = users.values().filter(|u| u.room == user.room).count();
        drop(users);

        info!("User '{}' left room '{}'", user.username, user.room);

        let _ = state.broadcast_tx.send((
            user.room.clone(),
            ServerMessage::UserLeft {
                user_id: user.user_id.clone(),
                username: user.username.clone(),
                room: user.room.clone(),
                timestamp: Utc::now().to_rfc3339(),
                online_count: count,
            },
        ));

        broadcast_user_list(&state, &user.room).await;
    }

    info!("Connection closed: {}", addr);
}

// ─────────────────────────────────────────────────────────────
//  Message dispatch
// ─────────────────────────────────────────────────────────────

async fn handle_client_message(
    msg: ClientMessage,
    user_id: &str,
    current_user: &mut Option<ConnectedUser>,
    state: &Arc<AppState>,
    tx: &tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    addr: SocketAddr,
) {
    match msg {
        // ── Join ──────────────────────────────────────────────
        ClientMessage::Join { username, room } => {
            if username.trim().is_empty() || room.trim().is_empty() {
                let _ = tx.send(ServerMessage::Error {
                    message: "Username and room cannot be empty".into(),
                });
                return;
            }

            let user = ConnectedUser {
                user_id: user_id.to_string(),
                username: username.clone(),
                room: room.clone(),
                addr,
            };

            let mut users = state.users.write().await;
            users.insert(user_id.to_string(), user.clone());
            let count = users.values().filter(|u| u.room == room).count();
            drop(users);

            *current_user = Some(user.clone());

            info!("'{}' joined room '{}' ({})", username, room, user_id);

            // Welcome this user
            let _ = tx.send(ServerMessage::Welcome {
                user_id: user_id.to_string(),
                username: username.clone(),
                room: room.clone(),
            });

            // Announce to room
            let _ = state.broadcast_tx.send((
                room.clone(),
                ServerMessage::UserJoined {
                    user_id: user_id.to_string(),
                    username: username.clone(),
                    room: room.clone(),
                    timestamp: Utc::now().to_rfc3339(),
                    online_count: count,
                },
            ));

            broadcast_user_list(state, &room).await;
        }

        // ── Chat ──────────────────────────────────────────────
        ClientMessage::Chat { content } => {
            if let Some(user) = current_user {
                if content.trim().is_empty() {
                    return;
                }
                let _ = state.broadcast_tx.send((
                    user.room.clone(),
                    ServerMessage::Chat {
                        message_id: Uuid::new_v4().to_string(),
                        user_id: user.user_id.clone(),
                        username: user.username.clone(),
                        content,
                        room: user.room.clone(),
                        timestamp: Utc::now().to_rfc3339(),
                    },
                ));
            } else {
                let _ = tx.send(ServerMessage::Error {
                    message: "You must join a room first".into(),
                });
            }
        }

        // ── Private Message ───────────────────────────────────
        ClientMessage::PrivateMessage { to, content } => {
            if let Some(user) = current_user {
                let users = state.users.read().await;
                let target = users.values().find(|u| u.username == to).cloned();
                drop(users);

                if let Some(target) = target {
                    let pm = ServerMessage::PrivateMessage {
                        message_id: Uuid::new_v4().to_string(),
                        from_id: user.user_id.clone(),
                        from_username: user.username.clone(),
                        content,
                        timestamp: Utc::now().to_rfc3339(),
                    };
                    let _ = state.dm_tx.send((target.user_id.clone(), pm));
                } else {
                    let _ = tx.send(ServerMessage::Error {
                        message: format!("User '{}' not found", to),
                    });
                }
            }
        }

        // ── Typing ────────────────────────────────────────────
        ClientMessage::TypingStart => {
            if let Some(user) = current_user {
                let _ = state.broadcast_tx.send((
                    user.room.clone(),
                    ServerMessage::TypingUpdate {
                        user_id: user.user_id.clone(),
                        username: user.username.clone(),
                        room: user.room.clone(),
                        is_typing: true,
                    },
                ));
            }
        }
        ClientMessage::TypingStop => {
            if let Some(user) = current_user {
                let _ = state.broadcast_tx.send((
                    user.room.clone(),
                    ServerMessage::TypingUpdate {
                        user_id: user.user_id.clone(),
                        username: user.username.clone(),
                        room: user.room.clone(),
                        is_typing: false,
                    },
                ));
            }
        }

        ClientMessage::Ping => {
            let _ = tx.send(ServerMessage::Pong);
        }
    }
}

// ─────────────────────────────────────────────────────────────
//  Helpers
// ─────────────────────────────────────────────────────────────

async fn broadcast_user_list(state: &Arc<AppState>, room: &str) {
    let users = state.users.read().await;
    let list: Vec<UserInfo> = users
        .values()
        .filter(|u| u.room == room)
        .map(|u| UserInfo {
            user_id: u.user_id.clone(),
            username: u.username.clone(),
            room: u.room.clone(),
        })
        .collect();
    drop(users);

    let _ = state
        .broadcast_tx
        .send((room.to_string(), ServerMessage::UserList { users: list }));
}
