use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type DataStore = Arc<RwLock<HashMap<String, HashMap<String, TickerData>>>>;
pub enum ExchangeFormat {
    Mexc,
    Coinbase,
    Htx, // Add more exchanges as needed
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
    };

    let mut store = data_store.write().await;
    let exchange_store = store.entry(exchange.to_string()).or_default();
    exchange_store.insert(symbol.to_string(), ticker_data);
}
