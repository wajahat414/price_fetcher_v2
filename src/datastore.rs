use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

pub type DataStore = Arc<RwLock<HashMap<String, HashMap<String, TickerData>>>>;
pub type MessageCounter = Arc<RwLock<HashMap<String, u64>>>;
pub enum ExchangeFormat {
    Mexc,
    Coinbase,
    Htx, // Add more exchanges as needed
    KuKoin,
    Binance,
}

#[derive(Debug, Clone)]
pub struct TickerData {
    pub best_ask_price: String,
    pub best_bid_price: String,
    pub best_ask_qty: String,
    pub best_bid_qty: String,
    pub event_time: i64,
}

pub async fn update_ticker_data(
    data_store: DataStore,
    exchange: &str,
    symbol: &str,
    data: &Value,
    format: ExchangeFormat,
) {
    let ticker_data = match format {
        ExchangeFormat::Mexc => TickerData {
            best_ask_price: data["a"].as_str().unwrap_or_default().to_string(),
            best_bid_price: data["b"].as_str().unwrap_or_default().to_string(),
            best_ask_qty: data["A"].as_str().unwrap_or_default().to_string(),
            best_bid_qty: data["B"].as_str().unwrap_or_default().to_string(),
            event_time: data["t"].as_i64().unwrap_or_default(),
        },
        ExchangeFormat::Coinbase => TickerData {
            best_ask_price: data["best_ask"].as_str().unwrap_or_default().to_string(),
            best_bid_price: data["best_bid"].as_str().unwrap_or_default().to_string(),
            best_ask_qty: data["best_ask_size"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            best_bid_qty: data["best_bid_size"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            event_time: chrono::Utc::now().timestamp_millis(), // Coinbase format might not include event time
        },
        ExchangeFormat::Htx => TickerData {
            best_ask_price: data["ask"].as_f64().unwrap_or_default().to_string(),
            best_bid_price: data["bid"].as_f64().unwrap_or_default().to_string(),
            best_ask_qty: data["askSize"].as_f64().unwrap_or_default().to_string(),
            best_bid_qty: data["bidSize"].as_f64().unwrap_or_default().to_string(),
            event_time: data["quoteTime"].as_i64().unwrap_or_default(),
        },
        ExchangeFormat::KuKoin => todo!(),
        ExchangeFormat::Binance => todo!(),
    };

    let mut store = data_store.write().await;
    let exchange_store = store.entry(exchange.to_string()).or_default();
    exchange_store.insert(symbol.to_string(), ticker_data);
}

pub fn spawn_printer_task(
    data_store: DataStore,
    interval_secs: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Read and print the data store snapshot
                    let store_snapshot = {
                        let read_lock = data_store.read().await;
                        normalize_data_store(&*read_lock)
                    };

                    print_data_store(&store_snapshot);
                }
                // Optional shutdown logic could be added here if needed

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow(){
                        println!("Printer Task Shutting Down");
                        break;
                    }
                }

            }
        }
    })
}

fn normalize_data_store(
    store: &HashMap<String, HashMap<String, TickerData>>,
) -> HashMap<String, HashMap<String, TickerData>> {
    let mut normalized_store = HashMap::new();

    for (exchange, symbols) in store {
        let mut normalized_symbols = HashMap::new();

        for (symbol, data) in symbols {
            let normalized_symbol = match exchange.as_str() {
                "Mexc" => symbol.to_uppercase(),
                "Coinbase" => symbol.replace('-', "").to_uppercase(), // BTC-USD -> BTCUSDT
                "KuKoin" => symbol.replace('-', "").to_uppercase(),
                "HTX" => symbol
                    .strip_prefix("market.")
                    .and_then(|s| s.strip_suffix(".bbo"))
                    .unwrap_or(symbol)
                    .to_uppercase(), // market.btcusdt.bbo -> BTCUSDT
                _ => symbol.to_uppercase(),
            };

            normalized_symbols.insert(normalized_symbol, data.clone());
        }

        normalized_store.insert(exchange.clone(), normalized_symbols);
    }

    normalized_store
}

fn print_data_store(data_store: &HashMap<String, HashMap<String, TickerData>>) {
    for (exchange, pairs) in data_store {
        println!("Exchange: {}", exchange);
        for (pair, data) in pairs {
            println!(
                "  Pair: {} | Ask: {}@{} | Bid: {}@{}",
                pair,
                data.best_ask_price,
                data.best_ask_qty,
                data.best_bid_price,
                data.best_bid_qty
            );
        }
    }
    println!("---------------------------------------");
}
