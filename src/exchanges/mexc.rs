use crate::datastore::{update_ticker_data, DataStore};
use crate::datastore::{ExchangeFormat, MessageCounter};
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub async fn mexc_connection(
    data_store: DataStore,
    mut subscription_rx: mpsc::Receiver<String>,
    message_counter: MessageCounter,
) {
    let url = "wss://wbs.mexc.com/ws";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect to Mexc");
    let (mut write, mut read) = ws_stream.split();

    // Subscription task
    tokio::spawn(async move {
        while let Some(subscription) = subscription_rx.recv().await {
            let msg = Message::Text(subscription);
            if write.send(msg).await.is_err() {
                eprintln!("Failed to send subscription message.");
                break; // Exit if the connection is closed
            }
        }
    });

    // Message processing
    while let Some(Ok(msg)) = read.next().await {
        if let Message::Text(text) = msg {
            {
                let mut counter = message_counter.write().await;
                let count = counter.entry("Mexc".to_string()).or_insert(0);
                *count += 1;
            }
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(data) = parsed.get("d") {
                    let symbol = parsed["s"].as_str().unwrap_or_default().to_string();
                    update_ticker_data(
                        data_store.clone(),
                        "Mexc",
                        &symbol,
                        data,
                        ExchangeFormat::Mexc,
                    )
                    .await;
                }
            }
        }
    }
}
