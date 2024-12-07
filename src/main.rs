use std::collections::HashMap;
use std::sync::Arc;

use crate::datastore::DataStore;
use crate::exchanges::{coinbase::coinbase_connection, htx::htx_connection, mexc::mexc_connection};
use models::load_config;
use tokio::sync::{mpsc, RwLock};

mod datastore;
mod exchanges;
mod models;

#[tokio::main]
async fn main() {
    let config = load_config("config.json");
    println!("Loaded config: {:?}", config);
    let data_store: DataStore = Arc::new(RwLock::new(HashMap::new()));

    let mut tasks = Vec::new();

    for (exchange_name, exchange_config) in config.exchanges {
        if exchange_config.enabled {
            println!("Connecting to exchange: {}", exchange_name);

            let data_store_clone = data_store.clone();
            let symbols = exchange_config.symbols.clone();

            let task = match exchange_name.as_str() {
                "Mexc" => spawn_exchange_connection(data_store_clone, mexc_connection, symbols),
                "Coinbase" => {
                    spawn_exchange_connection(data_store_clone, coinbase_connection, symbols)
                }
                "HTX" => spawn_exchange_connection(data_store_clone, htx_connection, symbols),
                _ => {
                    eprintln!("Unknown exchange: {}", exchange_name);
                    continue;
                }
            };

            tasks.push(task);
        }
    }

    // Wait for all tasks to complete (keeps the program running)
    futures::future::join_all(tasks).await;

    // Periodically print the data store
    loop {
        let store = data_store.read().await;
        println!("{:?}", store);
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

/// Spawns a WebSocket connection task for a given exchange.
fn spawn_exchange_connection<F>(
    data_store: DataStore,
    connection_fn: fn(DataStore, mpsc::Receiver<String>) -> F,
    symbols: Vec<String>,
) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        // Start the WebSocket connection
        tokio::spawn(connection_fn(data_store.clone(), rx));

        // Send subscriptions for each symbol
        for symbol in symbols {
            let subscription_message = build_subscription_message(&symbol);
            if tx.send(subscription_message).await.is_err() {
                eprintln!("Failed to send subscription for symbol: {}", symbol);
                break;
            }
        }
    })
}

/// Builds the subscription message for a given symbol.
/// Builds the subscription message for a given symbol.
fn build_subscription_message(symbol: &str) -> String {
    // Coinbase format (e.g., BTC-USD)
    if symbol.contains("-") {
        return serde_json::json!({
            "type": "subscribe",
            "product_ids": [symbol],
            "channels": ["ticker"]
        })
        .to_string();
    }

    // HTX format (e.g., market.btcusdt.bbo)
    if symbol.contains(".") {
        return serde_json::json!({
            "sub": [format!("market.{}.bbo", symbol)],
            "id": "id1"
        })
        .to_string();
    }

    // Mexc format (e.g., BTCUSDT)
    if symbol.len() == 6 || symbol.len() == 7 {
        // assuming all valid symbols have 6 or 7 characters
        return serde_json::json!({
            "method": "SUBSCRIPTION",
            "params": [format!("spot@public.bookTicker.v3.api@{}", symbol)]
        })
        .to_string();
    }

    // Unsupported symbol format
    eprintln!("Unsupported symbol format: {}", symbol);
    String::new() // Return an empty string to skip unsupported symbols
}
