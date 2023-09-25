use std::env;
use std::thread;

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
    /* Install a signal handler to help kill this beast. */
    ctrlc::set_handler(|| std::process::exit(1)).expect("failed to register sigint handler");

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    /*
     * We'll just use commandline args for topic names. Just skip the
     * program name.
     */
    let topics: Vec<String>;
    if env::args().len() < 2 {
        topics = vec![String::from("rust")];
    } else {
        topics = env::args().skip(1).collect();
    }

    let username = env::var("REDPANDA_SASL_USERNAME").unwrap_or(String::from("redpanda"));
    let password = env::var("REDPANDA_SASL_PASSWORD").unwrap_or(String::from("password"));
    let mechanism = env::var("REDPANDA_SASL_MECHANISM").unwrap_or(String::from("SCRAM-SHA-256"));
    let protocol = env::var("REDPANDA_SECURITY_PROTOCOL").unwrap_or(String::from("plaintext"));
    let bootstrap = env::var("REDPANDA_BROKERS").unwrap_or(String::from("localhost:9092"));
    let cnt = env::var("REDPANDA_CONSUMERS").unwrap_or(String::from("1")).parse::<usize>().expect("invalid consumer count");

    let base_config: ClientConfig = ClientConfig::new()
        .set("group.id", "rust-group")
        .set("bootstrap.servers", &bootstrap)
        .set("security.protocol", &protocol)
        .set("sasl.mechanism", &mechanism)
        .set("sasl.username", &username)
        .set("sasl.password", &password)
        .set("enable.ssl.certificate.verification", "false")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .clone();

    let consumers: Vec<thread::JoinHandle<()>> = (0..cnt)
        .map(|i| {
            // I don't know enough Rust to avoid the double clone() :(
            let name = format!("rusty-boi-{i}");
            debug!("creating consumer {}", name);

            let consumer: BaseConsumer<DummyContext> = base_config
                .clone()
                .set("group.instance.id", format!("rusty-boi-{i}"))
                .create_with_context(DummyContext {})
                .expect("failed to create consumer");

            consumer
                .subscribe(
                    topics
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<&str>>()
                        .as_slice(),
                )
                .expect("failed to subscribe to topics");

            let builder = thread::Builder::new().name(name);
            builder
                .spawn(move || {
                    let me = thread::current();
                    let timeout = std::time::Duration::from_millis(5000);
                    let name = match me.name() {
                        Some(n) => String::from(n),
                        None => format!("thread-{i}"),
                    };
                    loop {
                        let result = match consumer.poll(timeout) {
                            Some(r) => r,
                            None => {
                                debug!("no events for {:?}", timeout);
                                continue;
                            }
                        };
                        result.map_or_else(
                            |err| warn!("{:?}", err),
                            |msg| {
                                let key = String::from_utf8_lossy(msg.key().unwrap_or(&[]));
                                let payload = String::from_utf8_lossy(msg.payload().unwrap_or(&[]));
                                info!(
                                    "({:?}) partition: {}, offset: {}, key: {:?}, data: {:?}",
                                    name.to_owned(),
                                    msg.partition(),
                                    msg.offset(),
                                    key,
                                    payload
                                );
                            },
                        );
                    }
                })
                .expect("failed to spawn thread")
        })
        .collect();

    consumers
        .into_iter()
        .for_each(|c| c.join().expect("failed to wait on thread"));
}
