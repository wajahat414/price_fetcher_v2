use core::f64;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;

use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::debug;

#[derive(thiserror::Error, Debug)]
pub enum KucoinError {
    #[error("WebSocket connection failed: {0}")]
    ConnectionFailed(String),
    #[error("Invalid response data: {0}")]
    InvalidResponse(String),
}

#[derive(Debug, Deserialize)]
pub struct KucoinWebSocketResponse {
    pub code: String,
    pub data: WebSocketData,
}
#[derive(Debug, Deserialize)]
pub struct WebSocketData {
    pub token: String,
    #[serde(rename = "instanceServers")]
    pub instance_servers: Vec<InstanceServer>,
}

#[derive(Debug, Deserialize)]
pub struct InstanceServer {
    pub endpoint: String,
    protocol: String,
    encrypt: bool,
    #[serde(rename = "pingInterval")]
    ping_interval: u64,
    #[serde(rename = "pingTimeout")]
    ping_timeout: u64,
}

pub struct KucoinConnector {
    base_url: String,
    client: Client,
}
impl KucoinConnector {
    pub fn new() -> Self {
        Self {
            base_url: "https://api.kucoin.com".to_string(),
            client: Client::new(),
        }
    }
    pub async fn get_websocket_url(
        &self,
    ) -> Result<KucoinWebSocketResponse, Box<dyn std::error::Error>> {
        let url = format!("{}/api/v1/bullet-public", self.base_url);

        let response = self.client.post(&url).send().await?;

        // Print the raw response status and body for debugging
        debug!("Response Status: {}", response.status());
        let raw_body = response.text().await?;

        // Attempt to deserialize if the response is successful
        match serde_json::from_str::<KucoinWebSocketResponse>(&raw_body) {
            Ok(parsed) => {
                println!("Parsed Response: {:?}", parsed);
                Ok(parsed)
            }
            Err(e) => {
                eprintln!("Deserialization Error: {}", e);
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to deserialize response: {}", e),
                )))
            }
        }
    }

    pub async fn connect_and_subscribe(
        &self,
        servers: Vec<InstanceServer>,
        token: &str,
        symbols: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if servers.is_empty() {
            return Err("No WebSocket servers available".into());
        }

        let server_url = &servers[0].endpoint;
        let full_url = format!("{}?token={}", server_url, token);
        print!("Connecting to web socket: {}", full_url);

        let (ws_stream, _) = connect_async(&full_url).await?;
        print!("Connected to web scoket");

        let (mut write, mut read) = ws_stream.split();

        let subscribe_message = json!({

            "id" : 1 ,
            "type": "subscribe",
            "topic": "/market/ticker:".to_owned() + &symbols.join(","),
            "privateChannel": false,
            "response": true

        });

        write
            .send(Message::Text(subscribe_message.to_string()))
            .await?;

        println!("subscribe to book ticker for symbols : {:?}", symbols);

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let parsed: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|e| format!("Failed to parse message: {}", e))?;

                    if let Some(topic) = parsed.get("topic").and_then(|t| t.as_str()) {
                        if let Some(data) = parsed.get("data") {
                            let symbol = topic.split(':').nth(1).unwrap_or_default();

                            if let (
                                Some(best_bid),
                                Some(best_bid_qty),
                                Some(best_ask),
                                Some(best_ask_qty),
                            ) = (
                                data.get("bestBid").and_then(|v| v.as_str()),
                                data.get("bestBidSize").and_then(|v| v.as_str()),
                                data.get("bestAsk").and_then(|v| v.as_str()),
                                data.get("bestAskSize").and_then(|v| v.as_str()),
                            ) {}
                        }
                    }
                }
                Ok(Message::Ping(ping)) => {
                    write.send(Message::Pong(ping)).await?;
                }
                Err(e) => {
                    eprintln!("WebSocket error: {:?}", e);
                    break;
                }
                _ => {}
            }
        }

        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Web Socket Closed un expectedly",
        )));
    }

    pub async fn subscribe(
        &self,
        write: &mut futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
            Message,
        >,
        symbols: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let subscribe_message = json!({
            "id": 1,
            "type": "subscribe",
            "topic": "/market/ticker:".to_owned() + &symbols.join(","),
            "privateChannel": false,
            "response": true,
        });

        write
            .send(Message::Text(subscribe_message.to_string()))
            .await?;
        println!("Subscribed to book ticker for symbols: {:?}", symbols);

        Ok(())
    }
}
