use core::error;
use std::borrow::Borrow;

use super::kucoin::KucoinConnector;
use crate::datastore::{DataStore, MessageCounter};
use crate::TickerData;
use futures::{SinkExt, StreamExt};

use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub async fn kucoin_connection(
    data_store: DataStore,
    mut rx: mpsc::Receiver<String>,
    message_counter: MessageCounter,
) {
    let connector = KucoinConnector::new();

    // Get WebSocket URL
    let response = connector.get_websocket_url().await;
    let data = response.unwrap().data;
    let servers = &data.borrow().instance_servers;
    let token = &data.borrow().token;

    if servers.is_empty() {
        println!("No WebSocket servers available for KuCoin");
    }

    let server_url = &servers[0].endpoint;
    let full_url = format!("{}?token={}", server_url, token);

    // Establish WebSocket connection
    let (ws_stream, _) = connect_async(full_url)
        .await
        .expect("Failed to connect to KuKoin");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to symbols
    while let Some(subscription_message) = rx.recv().await {
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscription_message,
            ))
            .await;
    }

    // Read messages from WebSocket
    while let Some(msg) = read.next().await {
        if let Ok(tokio_tungstenite::tungstenite::Message::Text(text)) = msg {
            {
                let mut counter = message_counter.write().await;

                let count = counter.entry("KuKoin".to_string()).or_insert(0);

                *count += 1;
            }
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(topic) = parsed.get("topic").and_then(|t| t.as_str()) {
                    if let Some(data) = parsed.get("data") {
                        let symbol = topic.split(':').nth(1).unwrap_or_default();
                        let ticker_data = TickerData {
                            best_ask_price: data["bestAsk"]
                                .as_str()
                                .unwrap_or_default()
                                .to_string(),
                            best_bid_price: data["bestBid"]
                                .as_str()
                                .unwrap_or_default()
                                .to_string(),
                            best_ask_qty: data["bestAskSize"]
                                .as_str()
                                .unwrap_or_default()
                                .to_string(),
                            best_bid_qty: data["bestBidSize"]
                                .as_str()
                                .unwrap_or_default()
                                .to_string(),
                            event_time: chrono::Utc::now().timestamp_millis(),
                        };
                        let mut store = data_store.write().await;
                        store
                            .entry("KuCoin".to_string())
                            .or_default()
                            .insert(symbol.to_string(), ticker_data);
                    }
                }
            }
        }
    }
}
