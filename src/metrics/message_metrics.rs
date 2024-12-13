use crate::datastore::MessageCounter;

pub fn spawn_metrics_printer(
    message_counter: MessageCounter,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            // Print the message counters
            {
                let counters = message_counter.read().await;
                println!("--- Message Metrics ---");
                for (exchange, count) in counters.iter() {
                    println!("Exchange: {} | Messages: {}", exchange, count);
                }
                println!("------------------------");
            }
        }
    })
}
