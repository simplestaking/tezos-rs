// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use async_trait::async_trait;
use shiplift::{
    rep::{ContainerDetails, ImageDetails},
    Docker,
};

#[async_trait]
pub trait DeployMonitoringContainer {
    const NAME: &'static str;

    async fn image() -> Result<String, anyhow::Error> {
        let docker = Docker::new();

        let ContainerDetails { config, .. } = docker.containers().get(Self::NAME).inspect().await?;
        Ok(config.image)
    }
}

pub async fn remote_hash<T: DeployMonitoringContainer + Sync + Send>(
) -> Result<String, anyhow::Error> {
    let image = T::image().await?;
    let image_split: Vec<&str> = image.split(':').collect();
    let repo = image_split.get(0).unwrap_or(&"");
    let url = format!(
        "https://hub.docker.com/v2/repositories/{}/tags/{}/?page_size=100",
        repo,
        image_split.get(1).unwrap_or(&""),
    );
    match reqwest::get(&url).await {
        Ok(result) => {
            let res_json: serde_json::Value = match result.json().await {
                Ok(json) => json,
                Err(e) => anyhow::bail!("Error converting result to json: {:?}", e),
            };
            let digest = res_json["images"][0]["digest"].to_string();
            let digest = digest.trim_matches('"');
            Ok(format!("{}@{}", repo, digest))
        }
        Err(e) => anyhow::bail!("Error getting latest image: {:?}", e),
    }
}

pub async fn local_hash<T: DeployMonitoringContainer + Sync + Send>(
    docker: &Docker,
) -> Result<String, anyhow::Error> {
    let image = T::image().await?;
    let ImageDetails { repo_digests, .. } = docker.images().get(&image).inspect().await?;
    repo_digests
        .and_then(|v| v.first().cloned())
        .ok_or_else(|| anyhow::format_err!("no such image {}", image))
}

pub struct TezedgeDebugger;

impl DeployMonitoringContainer for TezedgeDebugger {
    const NAME: &'static str = "deploy-monitoring-tezedge-debugger";
}

pub struct OcamlDebugger;

impl DeployMonitoringContainer for OcamlDebugger {
    const NAME: &'static str = "deploy-monitoring-ocaml-debugger";
}

pub struct Explorer;

impl DeployMonitoringContainer for Explorer {
    const NAME: &'static str = "deploy-monitoring-explorer";
}

pub struct Sandbox;

impl DeployMonitoringContainer for Sandbox {
    const NAME: &'static str = "deploy-monitoring-tezedge-sandbox-launcher";
}

pub struct TezedgeMemprof;

impl DeployMonitoringContainer for TezedgeMemprof {
    const NAME: &'static str = "deploy-monitoring-tezedge-memprof";
}
