// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;

use reqwest::header::AUTHORIZATION;
use slog::{error, info, Logger};

#[derive(Clone)]
pub struct SlackServer {
    monitor_channel_url: String,
    auth_token: String,
    channel: String,
    log: Logger,
}

impl SlackServer {
    pub fn new(
        monitor_channel_url: String,
        auth_token: String,
        channel: String,
        log: Logger,
    ) -> Self {
        Self {
            monitor_channel_url,
            auth_token,
            channel,
            log,
        }
    }

    pub async fn send_message(&self, text: &str) -> Result<(), anyhow::Error> {
        let client = reqwest::Client::new();

        let mut map = HashMap::new();
        map.insert("text", text);

        let res = client
            .post(&self.monitor_channel_url)
            .json(&map)
            .send()
            .await;

        match res {
            Ok(_) => info!(self.log, "Slack message sent: {}", text),
            Err(e) => error!(self.log, "Slack message error: {:?}", e),
        }

        Ok(())
    }

    pub async fn upload_file(&self, text: &str, file_content: &str) -> Result<(), anyhow::Error> {
        let params = [
            ("initial_comment", text),
            ("content", file_content),
            ("channels", &self.channel),
        ];

        let client = reqwest::Client::new();

        let res = client
            .post("https://slack.com/api/files.upload")
            .header(AUTHORIZATION, self.auth_token.clone())
            .form(&params)
            .send()
            .await?;

        info!(
            self.log,
            "Slack file upload response: {}",
            res.text().await?
        );
        Ok(())
    }
}
