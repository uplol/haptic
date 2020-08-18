use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use futures::future::FutureExt;
use futures_util::future::RemoteHandle;
use http::uri::Uri;

use tokio::sync::broadcast;
use tracing;

use crate::discovery::DiscoveryEngine;
use crate::model::{Endpoint, Sample};
use crate::prometheus_scrape::Scrape;

pub struct EndpointWorkerTask {
    endpoint: Endpoint,
    sender: broadcast::Sender<Sample>,
}

impl EndpointWorkerTask {
    pub fn new(endpoint: Endpoint, sender: broadcast::Sender<Sample>) -> Self {
        Self { endpoint, sender }
    }

    pub async fn run(mut self) {
        static SPLAY_COUNTER: AtomicU64 = AtomicU64::new(0);
        let splay = SPLAY_COUNTER.fetch_add(250, std::sync::atomic::Ordering::Relaxed)
            % self.endpoint.scrape_interval.as_millis() as u64;
        tokio::time::delay_for(tokio::time::Duration::from_millis(splay)).await;

        loop {
            tracing::info!("scraping {}", self.endpoint.hostname);
            if let Err(e) = self.scrape().await {
                tracing::error!("{:?}", e);
            }
            tokio::time::delay_for(self.endpoint.scrape_interval).await;
        }
    }

    pub async fn scrape(&mut self) -> Result<(), anyhow::Error> {
        let client = reqwest::Client::builder()
            .timeout(self.endpoint.scrape_interval * 2)
            .connect_timeout(self.endpoint.scrape_interval * 2)
            .build()?;

        let body = client
            .get(&self.endpoint.scrape_uri.to_string())
            .send()
            .await?
            .text()
            .await?;

        let scrape = Scrape::parse(
            body.lines()
                .map(|line| std::io::Result::Ok(String::from(line).into())),
        )?;

        for prom_sample in scrape.samples {
            let mut labels = self.endpoint.base_labels.clone();
            labels.extend(prom_sample.labels.0);

            match prom_sample.value {
                crate::prometheus_scrape::Value::Counter(value)
                | crate::prometheus_scrape::Value::Gauge(value)
                | crate::prometheus_scrape::Value::Untyped(value) => {
                    self.sender
                        .send(Sample {
                            timestamp: prom_sample.timestamp,
                            key: prom_sample.metric,
                            value,
                            labels: labels.clone(),
                        })
                        .map_err(|_| anyhow::anyhow!("send error"))?;
                }
                crate::prometheus_scrape::Value::Histogram(v) => {
                    // TODO: How do we do these for real???
                    for value in v {
                        let mut histogram_labels = labels.clone();
                        histogram_labels.insert("le".into(), value.less_than.to_string());
                        self.sender
                            .send(Sample {
                                timestamp: prom_sample.timestamp,
                                key: format!("{}_bucket", prom_sample.metric),
                                value: value.count,
                                labels: histogram_labels,
                            })
                            .map_err(|_| anyhow::anyhow!("send error"))?;
                    }
                }
                crate::prometheus_scrape::Value::Summary(v) => {

                    // TODO
                }
            }
        }

        Ok(())
    }
}

pub struct EndpointWorker {
    _remote_handle: RemoteHandle<()>,
}

impl EndpointWorker {
    pub fn new(endpoint: Endpoint, sender: broadcast::Sender<Sample>) -> Self {
        // hang onto a remote handle for this future so we can cancel it when dropped
        let (remote, remote_handle) = Self::scrape(endpoint.clone(), sender).remote_handle();

        // run the future in a new task
        tokio::spawn(async move {
            remote.await;
        });

        Self {
            _remote_handle: remote_handle,
        }
    }

    async fn scrape(endpoint: Endpoint, sender: broadcast::Sender<Sample>) {
        let task = EndpointWorkerTask::new(endpoint, sender);
        task.run().await;
    }
}

pub struct Scraper<D: DiscoveryEngine> {
    discovery: D,
    endpoints: HashMap<Uri, EndpointWorker>,
    sender: broadcast::Sender<Sample>,
}

impl<D: DiscoveryEngine> Scraper<D> {
    pub fn new(discovery: D, sender: broadcast::Sender<Sample>) -> Self {
        Self {
            discovery,
            endpoints: Default::default(),
            sender,
        }
    }

    pub async fn run(mut self) {
        loop {
            if let Err(e) = self.update_endpoints().await {
                tracing::error!("discovery error: {:?}", e);
            }
            tokio::time::delay_for(tokio::time::Duration::from_secs(15)).await;
        }
    }

    pub async fn update_endpoints(&mut self) -> Result<(), anyhow::Error> {
        tracing::info!("updating endpoints");
        let endpoints = self
            .discovery
            .find_endpoints()
            .await
            .map_err(|e| anyhow::anyhow!("{:?}", e))?;

        let endpoints_mapped: HashMap<Uri, Endpoint> = endpoints
            .into_iter()
            .map(|e| (e.scrape_uri.clone(), e))
            .collect();

        let to_remove: Vec<_> = self
            .endpoints
            .iter()
            .filter(|(uri, _)| !endpoints_mapped.contains_key(&uri))
            .map(|(uri, _)| uri.clone())
            .collect();

        let to_add: Vec<_> = endpoints_mapped
            .iter()
            .filter(|(uri, _)| !self.endpoints.contains_key(&uri))
            .collect();

        tracing::info!(
            "discovered {} endpoints: {} new, {} stale",
            endpoints_mapped.len(),
            to_add.len(),
            to_remove.len()
        );

        for uri in to_remove {
            // drop existing endpoint workers
            self.endpoints.remove(&uri);
        }

        for (uri, endpoint) in to_add {
            self.endpoints.insert(
                uri.clone(),
                EndpointWorker::new(endpoint.clone(), self.sender.clone()),
            );
        }

        Ok(())
    }
}
