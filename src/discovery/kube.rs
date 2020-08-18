use std::collections::HashMap;

use async_trait::async_trait;
use http::uri;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{ListParams, Meta},
    Api, Client,
};
use thiserror::Error;
use tracing;

use super::{DiscoveryEngine, DiscoveryError};
use crate::model::Endpoint;

static ANNOTATION_SCRAPE: &str = "haptic.up.lol/scrape";
static ANNOTATION_PATH: &str = "haptic.up.lol/path";
static ANNOTATION_PORT: &str = "haptic.up.lol/port";

static DEFAULT_SCRAPE_SCHEME: &str = "http";
static DEFAULT_METRICS_PORT: u16 = 80;
static DEFAULT_METRICS_PATH: &str = "/metrics";

#[derive(Debug)]
pub struct KubeDiscovery;

impl KubeDiscovery {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Error, Debug)]
pub enum KubeDiscoveryError {
    #[error("kube-api error: {0}")]
    KubeError(#[from] kube::Error),
    #[error(transparent)]
    DiscoveryError(#[from] DiscoveryError),
}

#[async_trait]
impl DiscoveryEngine for KubeDiscovery {
    type Error = KubeDiscoveryError;

    async fn find_endpoints(&self) -> Result<Vec<Endpoint>, KubeDiscoveryError> {
        let client = Client::try_default().await?;

        let mut endpoints = Vec::new();

        //

        // discover all pods with endpoints
        let pods: Api<Pod> = Api::all(client.clone());
        let all_pods = pods.list(&ListParams::default()).await?;
        let pods_with_networking = all_pods.iter().filter_map(|pod| {
            pod.status
                .as_ref()
                .and_then(|status| status.pod_ip.as_ref().and_then(|ip| Some((ip, pod))))
        });

        for (ip_addr, pod) in pods_with_networking {
            let meta = pod.meta();
            if let Some(v) = meta
                .annotations
                .as_ref()
                .and_then(|map| map.get(ANNOTATION_SCRAPE))
            {
                if v != "true" {
                    // if prometheus scraping isn't specifically enabled, bail out now
                    continue;
                }

                // get path and port for scraping
                let path = meta
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(ANNOTATION_PATH).map(|s| s.as_str()))
                    .unwrap_or(DEFAULT_METRICS_PATH);

                let port: u16 = meta
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(ANNOTATION_PORT).and_then(|s| s.parse().ok()))
                    .unwrap_or(DEFAULT_METRICS_PORT);

                // define some base labels, collect labels from pod
                let mut base_labels = HashMap::new();

                base_labels.insert("pod".into(), pod.name());
                if let Some(node_name) = pod.spec.as_ref().and_then(|s| s.node_name.clone()) {
                    base_labels.insert("node".into(), node_name);
                }
                if let Some(namespace) = pod.namespace() {
                    base_labels.insert("namespace".into(), namespace);
                }

                // extend the base labels w/ the pod's labels
                if let Some(labels) = meta.labels.clone() {
                    base_labels.extend(labels);
                }

                // build the uri, and if that works,
                match uri::Builder::new()
                    .scheme(DEFAULT_SCRAPE_SCHEME)
                    .authority(format!("{}:{}", ip_addr, port).as_str())
                    .path_and_query(path)
                    .build()
                {
                    Ok(uri) => endpoints.push(Endpoint {
                        hostname: pod.name(),
                        scrape_uri: uri,
                        base_labels,
                        scrape_interval: tokio::time::Duration::from_secs(15),
                    }),
                    Err(_) => tracing::warn!("incoherent scrape config for pod {:?}", meta.name),
                }
            }
        }

        Ok(endpoints)
    }
}
