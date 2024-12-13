use crate::datastore::{update_ticker_data, DataStore};
use crate::datastore::{ExchangeFormat, MessageCounter};
use flate2::read::GzDecoder;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use std::io::prelude::*;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
pub async fn htx_connection(
    data_store: DataStore,
    mut subscription_rx: mpsc::Receiver<String>,
    message_counter: MessageCounter,
) {
    let url = "wss://api.huobi.pro/ws";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect to HTX");
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

    // Process incoming messages
    while let Some(Ok(msg)) = read.next().await {
        match msg {
            Message::Text(text) => {
                process_message(&text, &data_store).await;
            }

            Message::Binary(binary_data) => {
                {
                    let mut counter = message_counter.write().await;

                    let count = counter.entry("HTX".to_string()).or_insert(0);

                    *count += 1;
                }
                if let Ok(decompressed) = decompress_gzip(&binary_data) {
                    let decompresed_x = decompressed.clone();

                    process_message(&decompressed, &data_store).await;
                }
            }
            _ => {
                // Handle other message types if needed
            }
        }
    }
}

async fn process_message(message: &str, data_store: &DataStore) {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(message) {
        if let Some(data) = parsed.get("tick") {
            let symbol = parsed["ch"].as_str().unwrap_or_default().to_string();

            if symbol.is_empty() {
                eprintln!("Received empty symbol from HTX");
                return;
            }

            update_ticker_data(
                data_store.clone(),
                "HTX",
                &symbol,
                data,
                ExchangeFormat::Htx,
            )
            .await;
        }
    }
}

fn decompress_gzip(data: &[u8]) -> Result<String, std::io::Error> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed_data = String::new();
    decoder.read_to_string(&mut decompressed_data)?;
    Ok(decompressed_data)
}
