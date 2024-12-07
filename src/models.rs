use serde::Deserialize;
use serde_json;
use std::collections::HashMap;
use std::fs;

#[derive(Deserialize, Debug)]
pub struct ExchangeConfig {
    pub enabled: bool,
    pub symbols: Vec<String>,
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub exchanges: HashMap<String, ExchangeConfig>,
}

pub fn load_config(path: &str) -> Config {
    let config_data = fs::read_to_string(path).expect("Failed to read config file");
    println!("Raw config data: {}", config_data); // Debug log
    serde_json::from_str(&config_data).unwrap_or_else(|e| {
        panic!("Failed to parse config: {}. Content: {}", e, config_data);
    })
}
