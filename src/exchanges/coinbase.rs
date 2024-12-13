use crate::datastore::{update_ticker_data, DataStore, MessageCounter};

use futures::{SinkExt, StreamExt};

use serde_json::{json, Value};

use tokio::sync::mpsc;

use crate::datastore::ExchangeFormat;

use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub async fn coinbase_connection(
    data_store: DataStore,
    mut subscription_rx: mpsc::Receiver<String>,
    message_counter: MessageCounter,
) {
    let url = "wss://ws-feed.exchange.coinbase.com";
    let (ws_stream, _) = connect_async(url)
        .await
        .expect("Failed to connect to Coinbase");
    let (mut write, mut read) = ws_stream.split();

    // Send initial message (e.g., subscribe to channels)
    tokio::spawn(async move {
        while let Some(subscription) = subscription_rx.recv().await {
            let msg = Message::Text(subscription);
            if write.send(msg).await.is_err() {
                break; // Connection closed
            }
        }
    });

    // Process incoming messages
    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            {
                let mut counter = message_counter.write().await;

                let count = counter.entry("CoinBase".to_string()).or_insert(0);

                *count += 1;
            }
            // Attempt to deserialize the incoming message
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                let symbol = parsed["product_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                if symbol.is_empty() {
                    eprintln!("Received empty symbol for Coinbase.");
                    continue; // Skip processing this message
                }

                update_ticker_data(
                    data_store.clone(),
                    "Coinbase",
                    &symbol,
                    &parsed,
                    ExchangeFormat::Coinbase,
                )
                .await;
            }
        }
    }
}
