use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::datastore::DataStore;
use crate::exchanges::{coinbase::coinbase_connection, htx::htx_connection, mexc::mexc_connection};
use crate::message_metrics::spawn_metrics_printer;
use datastore::{spawn_printer_task, MessageCounter, TickerData};
use exchanges::binance::binance_connection;
use exchanges::kukoinx::kucoin_connection;
use futures::future::join_all;
use metrics::message_metrics;
use models::load_config;
use tokio::sync::{mpsc, watch, RwLock};

mod datastore;
mod exchanges;
mod metrics;
mod models;

#[tokio::main]
async fn main() {
    let config = load_config("config.json");
    println!("Loaded config: {:?}", config);

    let data_store: DataStore = Arc::new(RwLock::new(HashMap::new()));
    let message_counter: MessageCounter = Arc::new(RwLock::new(HashMap::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut tasks = vec![];

    for (exchange_name, exchange_config) in config.exchanges {
        if exchange_config.enabled {
            println!("Connecting to exchange: {}", exchange_name);

            let data_store_clone = data_store.clone();

            // Format symbols based on the exchange
            let formatted_symbols: Vec<String> = exchange_config
                .symbols
                .iter()
                .map(|symbol| format_symbol(&exchange_name, symbol))
                .collect();

            let task = match exchange_name.as_str() {
                "Mexc" => spawn_exchange_connection(
                    data_store_clone,
                    mexc_connection,
                    formatted_symbols,
                    "Mexc".to_string(),
                    message_counter.clone(),
                ),
                "Coinbase" => spawn_exchange_connection(
                    data_store_clone,
                    coinbase_connection,
                    formatted_symbols,
                    "Coinbase".to_string(),
                    message_counter.clone(),
                ),
                "HTX" => spawn_exchange_connection(
                    data_store_clone,
                    htx_connection,
                    formatted_symbols,
                    "HTX".to_string(),
                    message_counter.clone(),
                ),

                "KuKoin" => spawn_exchange_connection(
                    data_store_clone,
                    kucoin_connection,
                    formatted_symbols,
                    "KuKoin".to_string(),
                    message_counter.clone(),
                ),

                "Binance" => spawn_exchange_connection(
                    data_store_clone,
                    binance_connection,
                    formatted_symbols,
                    "Binance".to_string(),
                    message_counter.clone(),
                ),
                _ => {
                    eprintln!("Unknown exchange: {}", exchange_name);
                    continue;
                }
            };

            tasks.push(task);
        }
    }

    // Wait for all tasks to complete (keeps the program running)
    let task_joiner = tokio::spawn(async move {
        join_all(tasks).await;
    });
    // Periodically print the data store
    let printer_task = spawn_printer_task(data_store.clone(), 5, shutdown_rx);
    let metrics_task = tokio::spawn(spawn_metrics_printer(message_counter, 1));

    // tokio::spawn(async move {
    //     tokio::time::sleep(Duration::from_secs(20)).await;
    //     println!("initiating shutdown...");
    //     shutdown_tx.send(true).unwrap();
    // });
    let _ = tokio::try_join!(task_joiner, printer_task, metrics_task);
}

/// Spawns a WebSocket connection task for a given exchange.
fn spawn_exchange_connection<F>(
    data_store: DataStore,
    connection_fn: fn(DataStore, mpsc::Receiver<String>, MessageCounter) -> F,
    symbols: Vec<String>,
    exchange_name: String,
    message_counter: MessageCounter,
) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = mpsc::channel(32);

    let connection_task = tokio::spawn(async move {
        connection_fn(data_store, rx, message_counter).await;
    });

    let subscription_task = tokio::spawn(async move {
        for symbol in symbols {
            let subscription_message = build_subscription_message(&symbol, &exchange_name);
            if subscription_message.is_empty() {
                eprintln!("Skipping invalid symbol format: {}", symbol);
                continue;
            }
            if tx.send(subscription_message).await.is_err() {
                eprintln!("Failed to send subscription for symbol: {}", symbol);
                break;
            }
        }
    });

    tokio::spawn(async move {
        let _ = tokio::try_join!(connection_task, subscription_task);
    })
}

/// Builds the subscription message for a given symbol.
fn build_subscription_message(symbol: &str, exchange: &str) -> String {
    match exchange {
        // Coinbase format (e.g., BTC-USD)
        "Coinbase" => {
            if symbol.contains('-') {
                return serde_json::json!({
                    "type": "subscribe",
                    "product_ids": [symbol],
                    "channels": ["ticker"]
                })
                .to_string();
            }
        }

        // HTX format (e.g., market.btcusdt.bbo)
        "HTX" => {
            if symbol.contains('.') {
                return serde_json::json!({
                    "sub": [symbol],
                    "id": "id1"
                })
                .to_string();
            }
        }

        // Mexc format (e.g., BTCUSDT)
        "Mexc" => {
            if symbol.len() == 6 || symbol.len() == 7 {
                return serde_json::json!({
                    "method": "SUBSCRIPTION",
                    "params": [format!("spot@public.bookTicker.v3.api@{}", symbol)]
                })
                .to_string();
            }
        }

        // KuCoin format (e.g., BTC-USDT)
        "KuKoin" => {
            if symbol.contains('-') {
                return serde_json::json!({
                    "id": 1,
                    "type": "subscribe",
                    "topic": format!("/market/ticker:{}", symbol),
                    "privateChannel": false,
                    "response": true,
                })
                .to_string();
            }
        }

        _ => {
            eprintln!("Unsupported exchange: {}", exchange);
        }
    }

    // Unsupported symbol format
    eprintln!(
        "Unsupported symbol format for exchange '{}': {}",
        exchange, symbol
    );
    String::new()
}

/// Formats symbols based on the exchange requirements.
fn format_symbol(exchange: &str, symbol: &str) -> String {
    match exchange {
        "Mexc" => symbol.to_string(),
        "Coinbase" => format!("{}-{}", &symbol[0..3], &symbol[3..]), // BTCUSDT -> BTC-USD
        "KuKoin" => format!("{}-{}", &symbol[0..3], &symbol[3..]),   // BTCUSDT -> BTC-USD

        "HTX" => format!("market.{}.bbo", symbol.to_lowercase()), // BTCUSDT -> market.btcusdt.bbo
        "Binance" => return symbol.to_lowercase(),

        _ => {
            eprintln!("Unknown exchange: {}", exchange);
            symbol.to_string()
        }
    }
}
