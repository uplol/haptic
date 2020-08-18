mod discovery;
mod model;
mod prometheus_scrape;
mod scraper;

use std::collections::{HashMap, HashSet, VecDeque};

use clickhouse_rs::{row, types::Value, Block, Client, Options, Pool};
use structopt::StructOpt;
use tokio::sync::broadcast;
use tracing_subscriber;

use discovery::kube::KubeDiscovery;

#[macro_use]
extern crate lazy_static;

#[derive(StructOpt, Debug)]
pub struct Command {
    #[structopt(short, long)]
    pub ch_uri: clickhouse_rs::Options,
}

const QUEUE_CAPACITY: usize = 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // a builder for `FmtSubscriber`.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // parse the command line args
    let opt: Command = Command::from_args();

    let discovery = KubeDiscovery::new();

    let (tx, mut rx) = broadcast::channel(QUEUE_CAPACITY);

    tokio::spawn(async move {
        let scraper = scraper::Scraper::new(discovery, tx);
        scraper.run().await;
    });

    let mut interval = tokio::time::interval_at(
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(5),
        tokio::time::Duration::from_secs(5),
    );

    let pool = Pool::new(opt.ch_uri);

    let mut client = pool.get_handle().await?;

    // todo: this is all bad and this will grow forever
    let mut metric_labels: HashMap<String, HashSet<String>> = Default::default();

    let mut buf = VecDeque::<model::Sample>::new();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if buf.is_empty() {
                    continue;
                }

                let samples: Vec<_> = buf.drain(..).collect();

                let mut metric_samples: HashMap<String, Vec<Vec<(String, Value)>>> = HashMap::new();

                for sample in samples {
                    match metric_labels.entry(sample.key.clone()) {
                        std::collections::hash_map::Entry::Occupied(e) => {
                            let mut set = e.remove();
                            let mut label_lines = vec![];
                            for label in sample.labels.keys() {
                                if set.insert(label.to_owned()) {
                                    label_lines.push(format!("`{}` String", label));
                                }
                            }

                            if !label_lines.is_empty() {
                                let alter_query = format!(r"ALTER TABLE `{}`
                                    ADD COLUMN IF NOT EXISTS
                                    {}
                                ", sample.key, label_lines.join(",\n\tADD COLUMN IF NOT EXISTS\n\t"));

                                tracing::info!("{}", alter_query);

                                if let Err(e) = client.execute(alter_query).await {
                                    tracing::error!("alter error {:?}", e);
                                }
                            }

                            metric_labels.insert(sample.key.clone(), set.clone());
                        },
                        std::collections::hash_map::Entry::Vacant(v) => {
                            let label_lines: Vec<_> = sample.labels.keys().map(|l| format!("`{}` String", l)).collect();

                            let create_query = format!(r"CREATE TABLE IF NOT EXISTS `{}` (
                                    timestamp DateTime,
                                    metric String,
                                    value Float64,
                                    {}
                                ) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(timestamp) ORDER BY (timestamp)
                            ", sample.key, label_lines.join(",\n"));

                            tracing::info!("{}", create_query);

                            if let Err(e) = client.execute(create_query).await {
                                tracing::error!("create error {:?}", e);
                            }

                            v.insert(sample.labels.keys().map(|s| s.to_owned()).collect());
                        }
                    }


                    let mut row = Vec::new();
                    row.push(("timestamp".to_string(), Value::DateTime(sample.timestamp.timestamp() as u32, chrono_tz::UTC)));
                    row.push(("metric".to_string(), Value::String(std::sync::Arc::new(sample.key.clone().into_bytes()))));
                    row.push(("value".to_string(), Value::Float64(sample.value)));
                    for (key, value) in sample.labels {
                        row.push((key, Value::String(std::sync::Arc::new(value.into()))));
                    }

                    if let std::collections::hash_map::Entry::Occupied(mut e) = metric_samples.entry(sample.key.clone()) {
                        e.get_mut().push(row);
                    } else {
                        metric_samples.insert(sample.key.clone(), vec![row]);
                    }
                }


                // insert to blocks
                let (mut c_tables, mut c_rows) = (0, 0);
                for (metric, rows) in metric_samples {
                    c_tables += 1;
                    let mut block = Block::with_capacity(rows.len());
                    for row in rows {
                        c_rows += 1;
                        block.push(row)?;
                    }
                    client.insert(metric, block).await?;
                }

                tracing::info!("write {} rows to {} tables", c_rows, c_tables);
            }
            Ok(sample) = rx.recv() => {
                buf.push_back(sample);
            }
        }
    }

    Ok(())
}
