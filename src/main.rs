use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use log::{debug, info, warn};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use tracing::error;

static TIMEOUT_5_SECONDS: Timeout = Timeout::After(Duration::new(5, 0));

struct DummyContext;

impl ClientContext for DummyContext {}

impl ConsumerContext for DummyContext {}

type PartitionQueue = Arc<Mutex<Vec<(String, i32)>>>;

fn worker(
    num_partitions: usize,
    config: ClientConfig,
    work: PartitionQueue,
) -> Result<(), KafkaError> {
    let me = thread::current();
    let name = me
        .name()
        .expect("expected to get thread name...what platform am I on?");

    // Grab some work.
    let tpl: TopicPartitionList = {
        let mut q = work.lock().unwrap();
        let mut tpl = TopicPartitionList::new();

        for _ in 0..num_partitions {
            let (topic, id) = match q.pop() {
                None => {
                    warn!("no work left");
                    break;
                }
                Some((t, p)) => {
                    info!("taking {}/{}", t, p);
                    (t, p)
                }
            };
            tpl.add_partition(topic.as_str(), id);
        }
        tpl.set_all_offsets(Offset::Beginning)?;

        Ok::<TopicPartitionList, KafkaError>(tpl)
    }?;

    // Create a consumer.
    let consumer: BaseConsumer<DummyContext> = config
        .clone()
        .set("group.instance.id", name)
        .create_with_context(DummyContext {})
        .expect("failed to create consumer");

    // Assign TopicPartitions.
    consumer.assign(&tpl)?;

    // Start consuming.
    loop {
        let result = match consumer.poll(TIMEOUT_5_SECONDS) {
            Some(r) => r,
            None => {
                debug!("no events for {:?}", TIMEOUT_5_SECONDS);
                continue;
            }
        };

        result.map_or_else(
            |err| warn!("{:?}", err),
            |msg| {
                // Manually commit this message's offset.
                match consumer.commit_message(&msg, CommitMode::Sync) {
                    Ok(_) => info!(
                        "committed partition {} @ offset {}",
                        msg.partition(),
                        msg.offset(),
                    ),
                    Err(e) => error!(
                        "error committing partition {} @ offset {}: {}",
                        msg.partition(),
                        msg.offset(),
                        e,
                    ),
                };
            },
        );
    }
}

fn main() -> Result<(), KafkaError> {
    /* Install a signal handler to help kill this beast. */
    ctrlc::set_handler(|| std::process::exit(1)).expect("failed to register sigint handler");

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_names(true)
        .init();

    /*
     * We'll just use commandline args for topic names. Just skip the program name.
     */
    let topics: Vec<String>;
    if env::args().len() < 2 {
        topics = vec![String::from("rust")];
    } else {
        topics = env::args().skip(1).collect();
    }

    let username = env::var("REDPANDA_SASL_USERNAME");
    let password = env::var("REDPANDA_SASL_PASSWORD").unwrap_or(String::from("password"));
    let mechanism = env::var("REDPANDA_SASL_MECHANISM").unwrap_or(String::from("SCRAM-SHA-256"));
    let protocol = env::var("REDPANDA_SECURITY_PROTOCOL").unwrap_or(String::from("sasl_plaintext"));
    let bootstrap = env::var("REDPANDA_BROKERS").unwrap_or(String::from("localhost:9092"));
    let consumer_cnt = env::var("REDPANDA_CONSUMERS")
        .unwrap_or(String::from("1"))
        .parse::<usize>()
        .expect("invalid consumer count");
    let group_id = env::var("REDPANDA_GROUP_ID").unwrap_or(String::from("rust-group"));

    let mut base_config: ClientConfig = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &bootstrap)
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .clone();

    if username.is_ok() {
        info!("using authentication");
        base_config = base_config
            .set("security.protocol", &protocol)
            .set("sasl.mechanism", &mechanism)
            .set("sasl.username", &username.unwrap())
            .set("sasl.password", &password)
            .clone();
    }

    // Manually assemble a list of TopicPartitions to use for the .assign() call.
    let metadata_consumer: BaseConsumer<DummyContext> =
        base_config.clone().create_with_context(DummyContext {})?;
    let topic_metadata = metadata_consumer.fetch_metadata(None, TIMEOUT_5_SECONDS)?;

    let topic_partitions: PartitionQueue = Arc::new(Mutex::new(Vec::new()));
    let tp_cnt: usize = {
        // Populate our global work queue.
        let mut v = topic_partitions.lock().unwrap();
        topic_metadata
            .topics()
            .iter()
            .filter(|&mt| topics.contains(&String::from(mt.name())))
            .for_each(|mt| {
                mt.partitions()
                    .iter()
                    .for_each(|mp| v.push((String::from(mt.name()), mp.id())))
            });
        info!("Prepared {} topic partitions", v.len());
        v.len()
    };

    // Figure out how many each Consumer should try to take.
    let per_consumer = tp_cnt / consumer_cnt;
    let bonus = tp_cnt % consumer_cnt;

    // Spin up our consumers.
    let consumers: Vec<thread::JoinHandle<Result<(), KafkaError>>> = (0..consumer_cnt)
        .map(|i| {
            let name = format!("rusty-boi-{i}");
            let config = base_config.clone();
            let work_queue = topic_partitions.clone();
            let mut num_partitions = per_consumer;
            if i == 0 {
                num_partitions = num_partitions + bonus;
            }
            debug!(
                "creating consumer thread {} for {} partitions",
                name, num_partitions
            );

            thread::Builder::new()
                .name(name)
                .spawn(move || worker(num_partitions, config, work_queue))
                .expect("failed to spawn thread")
        })
        .collect();

    consumers
        .into_iter()
        .for_each(|h| h.join().unwrap().unwrap());
    Ok(())
}
