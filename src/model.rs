use std::collections::HashMap;

use chrono::{DateTime, Utc};
use chrono_tz::UTC;
use http::uri::Uri;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub hostname: String,
    pub scrape_uri: Uri,
    pub base_labels: HashMap<String, String>,
    pub scrape_interval: Duration,
}

// impl From<(String, String)> for MetricLabel {
//     fn from((key, value): (String, String)) -> Self {
//         Self(key.into(), value.into())
//     }
// }

#[derive(Debug, Clone)]
pub struct Sample {
    pub timestamp: DateTime<Utc>,
    pub key: String,
    pub value: f64,
    pub labels: HashMap<String, String>,
}
