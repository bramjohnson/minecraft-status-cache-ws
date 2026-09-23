use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    routing::get,
    Router,
};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{watch, Notify};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct Status {
    pub motd: String,
    pub max_players: u32,
    pub online: bool,
    pub players_online: Vec<ServerStatusPlayer>,
    pub players_history: Vec<StatusPlayerHistory>, // Sorted by last_online
    pub is_allowlist_enforced: bool,
    pub allowlist: Vec<ServerStatusPlayer>,
    pub version: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct StatusPlayerHistory {
    pub id: String,
    pub name: String,
    pub last_online: u64,
}

#[derive(Deserialize)]
struct ServerStatusVersionResponse {
    name: String,
    protocol: u32,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ServerStatusPlayer {
    pub id: String,
    pub name: String,
}

#[derive(Deserialize)]
struct ServerStatusResponse {
    players: Option<Vec<ServerStatusPlayer>>,
    started: bool,
    version: ServerStatusVersionResponse,
}

// Axum requires shared state to implement `Clone`.
// Cloned instances of `watch::Receiver` point to the same underlying channel buffer.
#[derive(Clone)]
struct AppState {
    status_rx: watch::Receiver<Status>,
    client_connected: Arc<Notify>,
}

// `#[tokio::main]` transforms `async fn main()` into a synchronous main function
// that initializes the Tokio multi-threaded async runtime and drives the top-level future.
#[tokio::main]
async fn main() {
    let _ = dotenv();

    // `tokio::sync::watch` is a single-producer, multi-consumer channel designed for state updates.
    // Unlike an `mpsc` (queue) channel, `watch` retains ONLY the latest value.
    // Consumers don't get flooded with missed historical messages—they only care about the freshest state.
    let (status_tx, status_rx) = watch::channel(Status::default());
    let client_connected = Arc::new(Notify::new());

    // `tokio::spawn` submits a lightweight async task (green thread) to the Tokio runtime.
    // It runs concurrently with the rest of `main` without blocking the main execution path.
    tokio::spawn(upstream_poller_task(status_tx, client_connected.clone()));

    let state = AppState {
        status_rx,
        client_connected,
    };

    // Axum routes map paths to async handler functions. `.with_state()` injects thread-safe
    // state into the handler pipeline via Axum's type-safe `State` extractor.
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let port_str = std::env::var("SERVER_PORT").unwrap_or("3001".to_owned());
    let port = port_str.parse::<u16>().expect("Invalid SERVER_PORT");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("WebSocket server listening on ws://{}", addr);

    // Tokio's async TCP listener doesn't block the OS thread while waiting for connections.
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

struct MinecraftRestCacheAPIClient {
    http_client: reqwest::Client,
    host: String,
}

impl MinecraftRestCacheAPIClient {
    fn get(&self, endpoint: &str) -> RequestBuilder {
        return self.http_client.get(format!("{}{}", self.host, endpoint));
    }
}

// Background poller running endlessly in its spawned Tokio task.
async fn upstream_poller_task(status_tx: watch::Sender<Status>, notify: Arc<Notify>) {
    // `reqwest::Client` uses an `Arc` internally, so standard HTTP connection pooling
    // is preserved across loop iterations without needing `&mut self`.
    let http_client = reqwest::Client::new();
    let upstream_host = std::env::var("MINECRAFT_SERVER_MANAGEMENT_API_HOST")
        .expect("Expected MINECRAFT_SERVER_MANAGEMENT_API_HOST");
    let client = MinecraftRestCacheAPIClient {
        http_client,
        host: upstream_host,
    };

    // `tokio::time::interval` non-blockingly yields back to the executor during waiting periods,
    // freeing up the OS thread to do other work.
    let mut interval = tokio::time::interval(Duration::from_secs(35));

    loop {
        // Sleep / wake protocol when no clients
        if status_tx.receiver_count() <= 1 {
            println!("No active clients connected. Poller going to sleep...");

            notify.notified().await;

            println!("Client connected, waking back up!");
            fetch_and_update(&client, &status_tx).await;
            interval.reset();
        }

        // `.tick().await` waits until the next interval elapsed without blocking OS threads.
        interval.tick().await;
        if status_tx.receiver_count() > 1 {
            fetch_and_update(&client, &status_tx).await;
        }
    }
}

async fn fetch_and_update(client: &MinecraftRestCacheAPIClient, status_tx: &watch::Sender<Status>) {
    // Create Future primitives for each HTTP request without `.await`ing them immediately.
    let motd_req = client.get("/motd").send();
    let server_status_req = client.get("/server").send();
    let max_players_req = client.get("/max_players").send();
    let is_allowlist_enforced_req = client.get("/isAllowlistEnforced").send();
    let allowlist_req = client.get("/allowlist").send();

    // `tokio::join!` drives multiple Futures concurrently on the same task thread
    // until all complete, returning a tuple of their results once done.
    if let (
        Ok(motd_res),
        Ok(server_status_res),
        Ok(max_players_res),
        Ok(is_allowlist_enforced_res),
        Ok(allowlist_res),
    ) = tokio::join!(
        motd_req,
        server_status_req,
        max_players_req,
        is_allowlist_enforced_req,
        allowlist_req
    ) {
        let motd: String = motd_res.json().await.unwrap_or("".to_string());
        let server_status: ServerStatusResponse =
            server_status_res
                .json()
                .await
                .unwrap_or(ServerStatusResponse {
                    players: Some(Vec::new()),
                    started: false,
                    version: ServerStatusVersionResponse {
                        name: "".to_string(),
                        protocol: 0,
                    },
                });
        let max_players: u32 = max_players_res.json().await.unwrap_or(20);
        let is_allowlist_enforced = is_allowlist_enforced_res.json().await.unwrap_or(false);
        let allowlist: Vec<ServerStatusPlayer> = allowlist_res.json().await.unwrap_or(Vec::new());

        // TODO: This could be done better
        let existing_player_history = status_tx.borrow().players_history.clone();
        let mut new_player_history_map: HashMap<String, StatusPlayerHistory> = HashMap::new();
        // Load the existing history
        for existing_player in existing_player_history {
            new_player_history_map.insert(existing_player.id.clone(), existing_player);
        }
        // Overwrite with current player data
        let current_players = server_status.players.unwrap_or(Vec::new());
        for current_player in &current_players {
            new_player_history_map.insert(
                current_player.id.clone(),
                StatusPlayerHistory {
                    id: current_player.id.clone(),
                    name: current_player.name.clone(),
                    last_online: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                },
            );
        }

        let mut new_player_history: Vec<StatusPlayerHistory> =
            new_player_history_map.into_values().collect();
        new_player_history.sort_by_key(|player| player.last_online);
        let new_status = Status {
            motd: motd.to_string(),
            max_players: max_players,
            online: server_status.started,
            players_online: current_players,
            players_history: new_player_history,
            is_allowlist_enforced: is_allowlist_enforced,
            allowlist: allowlist,
            version: server_status.version.name,
            // last_updated: std::time::SystemTime::now()
            //     .duration_since(std::time::UNIX_EPOCH)
            //     .unwrap_or_default()
            //     .as_secs(),
        };

        // `status_tx.send(...)` notifies all active `status_rx` receivers.
        // If it returns `Err`, all receivers were dropped (e.g., server shutting down).
        if *status_tx.borrow() != new_status {
            let _ = status_tx.send(new_status).is_err();
        }
    }
}

// Axum extractors (`WebSocketUpgrade`, `State`) pull data directly from the incoming HTTP request.
// Handlers declare what they need as arguments, and Axum validates and extracts them automatically.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> axum::http::Response<axum::body::Body> {
    // `ws.on_upgrade` converts the standard HTTP GET request into a persistent TCP WebSocket stream.
    state.client_connected.notify_one();
    ws.on_upgrade(|socket| handle_socket(socket, state.status_rx))
}

// Per-connection worker handling incoming & outgoing frames for a single connected client.
async fn handle_socket(socket: WebSocket, mut status_rx: watch::Receiver<Status>) {
    // `socket.split()` breaks the WebSocket into two independent halves:
    // a `Stream` (reader) and a `Sink` (writer), allowing concurrent send and receive operations.
    let (mut sender, mut receiver) = socket.split();

    // Reader task: Listens for incoming client messages (e.g., PING/PONG, client close requests).
    // `StreamExt::next` yields `Some(item)` as frames arrive, yielding `None` on connection termination.
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Close(_) = msg {
                break;
            }
        }
    });

    // Send the current cached state immediately to newly connected clients.
    // `status_rx.borrow()` returns a temporary read guard (`Ref<T>`) to the latest value.
    let initial_status = status_rx.borrow().clone();
    if let Ok(json_str) = serde_json::to_string(&initial_status) {
        if sender.send(Message::Text(json_str)).await.is_err() {
            return;
        }
    }

    // Writer task: Awaits updates from the watch channel and writes them out over the socket.
    let mut send_task = tokio::spawn(async move {
        // `status_rx.changed()` pauses the task until `status_tx.send()` is called with a new value.
        while status_rx.changed().await.is_ok() {
            let updated_status = status_rx.borrow().clone();
            if let Ok(json_str) = serde_json::to_string(&updated_status) {
                // `SinkExt::send` sends a frame over the socket. If it fails, the client disconnected.
                if sender.send(Message::Text(json_str)).await.is_err() {
                    break;
                }
            }
        }
    });

    // `tokio::select!` executes both tasks concurrently.
    // As soon as ONE of the tasks finishes (e.g., client drops or write fails),
    // `select!` returns and triggers the cancellation of the remaining task via `.abort()`.
    tokio::select! {
        _ = (&mut recv_task) => send_task.abort(),
        _ = (&mut send_task) => recv_task.abort(),
    };
}
