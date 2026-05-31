use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use ask_core::models::EmbeddingModel;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

use crate::config::EmbeddingProvider;

/// Embeds one or more strings for a specific configured model.
pub trait EmbeddingClient: Send + Sync {
    /// Produce one embedding vector per input string.
    ///
    /// # Arguments
    ///
    /// * `model` - Persisted model metadata, including the expected dimensions.
    /// * `inputs` - Strings to embed in request order.
    ///
    /// # Returns
    ///
    /// One vector per input, in the same order.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider request fails, the response cannot be
    /// decoded, or the returned vector count or dimensions do not match the
    /// requested model.
    fn embed(&self, model: &EmbeddingModel, inputs: &[String]) -> Result<Vec<Vec<f32>>>;
}

/// Shared trait object used by the worker.
pub type SharedEmbeddingClient = Arc<dyn EmbeddingClient>;

/// HTTP implementation for OpenAI-compatible embedding providers.
#[derive(Debug, Clone)]
pub struct HttpEmbeddingClient {
    provider: EmbeddingProvider,
    client: Client,
}

impl HttpEmbeddingClient {
    /// Build an HTTP embedding client from the configured provider.
    ///
    /// # Arguments
    ///
    /// * `provider` - Provider mode and connection details.
    ///
    /// # Returns
    ///
    /// A ready-to-use client.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created.
    pub fn new(provider: EmbeddingProvider) -> Result<Self> {
        let client = Client::builder()
            .build()
            .context("failed to build embedding HTTP client")?;

        Ok(Self { provider, client })
    }
}

impl EmbeddingClient for HttpEmbeddingClient {
    fn embed(&self, model: &EmbeddingModel, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let url = format!(
            "{}/embeddings",
            self.provider.base_url().trim_end_matches('/')
        );
        let request = EmbeddingRequest {
            model: &model.name,
            input: inputs,
        };

        let mut http_request = self.client.post(url).json(&request);
        if let EmbeddingProvider::OpenAi { auth_token, .. } = &self.provider {
            http_request = http_request.bearer_auth(auth_token);
        }

        // `embed` runs on a `spawn_blocking` worker thread. Re-entering the
        // outer Tokio runtime avoids `reqwest::blocking`, which would create
        // and later drop its own runtime from async startup/shutdown paths.
        let response = Handle::current()
            .block_on(async { http_request.send().await })
            .with_context(|| {
                format!(
                    "embedding provider request failed for model {} ({})",
                    model.name,
                    self.provider.mode_name()
                )
            })?;
        let status = response.status();
        let body = Handle::current()
            .block_on(async { response.text().await })
            .context("failed to read embedding provider response body")?;

        if !status.is_success() {
            return Err(anyhow!(
                "embedding provider returned {}: {}",
                status,
                body.trim()
            ));
        }

        let decoded: EmbeddingResponse =
            serde_json::from_str(&body).context("failed to decode embedding provider response")?;

        let mut items = decoded.data;
        items.sort_by_key(|item| item.index);

        ensure!(
            items.len() == inputs.len(),
            "embedding provider returned {} vectors for {} inputs",
            items.len(),
            inputs.len()
        );

        let mut vectors = Vec::with_capacity(items.len());
        for item in items {
            ensure!(
                item.embedding.len() == model.dimensions as usize,
                "embedding provider returned {} dimensions for model {} (expected {})",
                item.embedding.len(),
                model.name,
                model.dimensions
            );
            vectors.push(item.embedding);
        }

        Ok(vectors)
    }
}

/// Deterministic embedding client for tests.
#[derive(Debug, Clone, Default)]
pub struct DeterministicEmbeddingClient {
    failure_trigger: Option<String>,
}

impl DeterministicEmbeddingClient {
    /// Create a deterministic client that always succeeds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a deterministic client that fails when any input contains `needle`.
    #[must_use]
    pub fn fail_on_input(needle: impl Into<String>) -> Self {
        Self {
            failure_trigger: Some(needle.into()),
        }
    }
}

impl EmbeddingClient for DeterministicEmbeddingClient {
    fn embed(&self, model: &EmbeddingModel, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        if let Some(needle) = &self.failure_trigger {
            if inputs.iter().any(|input| input.contains(needle)) {
                return Err(anyhow!(
                    "deterministic embedding failure triggered by input containing '{}'",
                    needle
                ));
            }
        }

        let dimensions = usize::try_from(model.dimensions)
            .context("embedding dimensions must fit into usize")?;
        let mut vectors = Vec::with_capacity(inputs.len());

        for input in inputs {
            let mut state = 0xcbf29ce484222325u64;
            for byte in model.name.bytes().chain(input.bytes()) {
                state ^= u64::from(byte);
                state = state.wrapping_mul(0x100000001b3);
            }

            let mut vector = Vec::with_capacity(dimensions);
            for index in 0..dimensions {
                state ^= index as u64;
                state = state.wrapping_mul(0x100000001b3);
                let value = (state % 10_000) as f32 / 10_000.0;
                vector.push(value);
            }
            vectors.push(vector);
        }

        Ok(vectors)
    }
}

#[derive(Debug, Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [String],
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingResponseItem>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingResponseItem {
    index: usize,
    embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use ask_core::models::EmbeddingModel;

    use crate::config::EmbeddingProvider;

    use super::{DeterministicEmbeddingClient, EmbeddingClient};

    fn model() -> EmbeddingModel {
        EmbeddingModel {
            id: 1,
            name: "test".to_string(),
            dimensions: 4,
            chunk_size: 16,
            chunk_overlap: 0,
            created_at: 1,
        }
    }

    #[test]
    fn deterministic_client_is_stable() {
        let client = DeterministicEmbeddingClient::new();
        let inputs = vec!["alpha".to_string(), "beta".to_string()];

        let first = client.embed(&model(), &inputs).unwrap();
        let second = client.embed(&model(), &inputs).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].len(), 4);
    }

    #[test]
    fn deterministic_client_can_fail_on_matching_input() {
        let client = DeterministicEmbeddingClient::fail_on_input("fail-me");
        let err = client
            .embed(
                &model(),
                &["ok".to_string(), "please fail-me now".to_string()],
            )
            .unwrap_err();

        assert!(err.to_string().contains("deterministic embedding failure"));
    }

    #[test]
    fn http_client_can_be_constructed_inside_tokio_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime must build");

        let result = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                let provider = EmbeddingProvider::Tei {
                    base_url: String::from("http://127.0.0.1:18080"),
                };

                super::HttpEmbeddingClient::new(provider)
                    .expect("client construction inside Tokio must succeed");
            });
        }));

        assert!(
            result.is_ok(),
            "constructing the HTTP embedding client inside Tokio must not panic"
        );
    }
}
