pub mod kube;

//use anyhow::Result;
use async_trait::async_trait;
use thiserror::Error;

use crate::model::Endpoint;

#[derive(Error, Debug)]
pub enum DiscoveryError {
    // we can add more generic errors discovery implementations may use here
    #[error("error")]
    Error(#[from] anyhow::Error),
}

#[async_trait]
pub trait DiscoveryEngine: std::fmt::Debug {
    type Error: From<DiscoveryError> + Send + Sync + std::fmt::Debug + std::error::Error;

    async fn find_endpoints(&self) -> Result<Vec<Endpoint>, Self::Error>;
}
