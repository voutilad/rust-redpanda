use std::env;

use log::{debug, info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::message::Message;

struct DummyContext;
impl ClientContext for DummyContext {}
impl ConsumerContext for DummyContext {}

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    /*
     * We'll just use commandline args for topic names. Just skip the
     * program name.
     */
    let mut topics: Vec<String> = env::args().skip(1).collect();
    if topics.len() == 0 {
        topics = vec![String::from("rust")];
    }

    debug!("Starting consumer for topics: {:?}", topics);
    let ctx = DummyContext {};

    let username = env::var("REDPANDA_SASL_USERNAME").unwrap_or(String::from("redpanda"));
    let password = env::var("REDPANDA_SASL_PASSWORD").unwrap_or(String::from("password"));
    let mechanism = env::var("REDPANDA_SASL_MECHANISM").unwrap_or(String::from("SCRAM-SHA-256"));
    let bootstrap = env::var("REDPANDA_BROKERS").unwrap_or(String::from("localhost:9092"));

    let consumer: BaseConsumer<DummyContext> = ClientConfig::new()
        .set("group.id", "rust-group")
        .set("group.instance.id", "muh-rusty-boi")
        .set("bootstrap.servers", bootstrap)
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanism", mechanism)
        .set("sasl.username", username)
        .set("sasl.password", password)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(ctx)
        .expect("failed to create consumer");

    debug!("Created consumer.");

    consumer
        .subscribe(
            topics
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>()
                .as_slice(),
        )
        .expect("failed to subscribe to topics!");
    debug!("subscribed to topics: {:?}", topics);

    let tpl = consumer.subscription().unwrap();
    debug!("Assignment: {:?}", tpl);

    debug!("Consuming from topic 'rust'...");
    let timeout = std::time::Duration::from_millis(3000);
    loop {
        let result = match consumer.poll(timeout) {
            Some(r) => r,
            None => {
                debug!("no events for {:?}", timeout);
                continue;
            },
        };
        result.map_or_else(
            |err| warn!("{:?}", err),
            |msg| {
                let key = String::from_utf8_lossy(msg.key().unwrap_or(&[]));
                let payload = String::from_utf8_lossy(msg.payload().unwrap_or(&[]));
                info!(
                    "offset: {}, key: {:?}, data: {:?}",
                    msg.offset(),
                    key,
                    payload
                );
            },
        );
    }
}
