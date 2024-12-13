use crate::datastore::{update_ticker_data, DataStore, ExchangeFormat, MessageCounter};
use binance::model::BookTickerEvent;
use binance::websockets::*;
use serde_json::json;
use std::{
    sync::{atomic::AtomicBool, Arc},
    time::SystemTime,
};
use tokio::sync::mpsc;

pub async fn binance_connection(
    data_store: DataStore,
    mut subscription_rx: mpsc::Receiver<String>,
    message_counter: MessageCounter,
) {
    let mut ws_client = WebSockets::new(move |event: WebsocketEvent| {
        if let WebsocketEvent::BookTicker(ref book_ticker) = event {
            handle_book_ticker_event(data_store.clone(), book_ticker).await;
        }
        Ok(())
    });

    // Manage subscriptions dynamically
    tokio::spawn(async move {
        while let Some(subscription) = subscription_rx.recv().await {
            let symbol = subscription.to_lowercase();
            if let Err(e) = ws_client.connect_multiple_streams(&[format!("{}@bookTicker", symbol)])
            {
                eprintln!("Failed to subscribe to symbol {}: {:?}", symbol, e);
            }
        }
    });

    // Start the WebSocket event loop
    let keep_running = Arc::new(AtomicBool::new(true));
    if let Err(e) = ws_client.event_loop(&keep_running) {
        eprintln!("Error in Binance WebSocket event loop: {:?}", e);
    }
}

async fn handle_book_ticker_event(data_store: DataStore, book_ticker: &BookTickerEvent) {
    let symbol = book_ticker.symbol.clone();
    let now = SystemTime::now();
    let data = json!({
        "best_ask_price": book_ticker.best_ask,
        "best_bid_price": book_ticker.best_bid,
        "best_ask_qty": book_ticker.best_ask_qty,
        "best_bid_qty": book_ticker.best_bid_qty,
        "event_time":  now,
    });

    // Update the shared data store
    update_ticker_data(
        data_store,
        "Binance",
        &symbol,
        &data,
        ExchangeFormat::Binance,
    )
    .await;
}
