use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use ask_core::models::EmbeddingModel;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

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
    max_batch_size: usize,
}

impl HttpEmbeddingClient {
    /// Build an HTTP embedding client from the configured provider.
    ///
    /// # Arguments
    ///
    /// * `provider` - Provider mode and connection details.
    /// * `max_batch_size` - Maximum number of inputs in one provider request.
    ///
    /// # Returns
    ///
    /// A ready-to-use client.
    ///
    /// # Errors
    ///
    /// Returns an error if provider configuration is invalid.
    pub fn new(provider: EmbeddingProvider, max_batch_size: usize) -> Result<Self> {
        ensure!(max_batch_size > 0, "max_batch_size must be greater than 0");
        Ok(Self {
            provider,
            max_batch_size,
        })
    }
}

impl EmbeddingClient for HttpEmbeddingClient {
    fn embed(&self, model: &EmbeddingModel, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let client = Client::builder()
            .build()
            .context("failed to build embedding HTTP client")?;
        let limit = self.max_batch_size;
        embed_inputs_in_batches(inputs, limit, |batch, batch_index, start, end| {
            debug!(
                batch_index = batch_index + 1,
                input_start = start,
                input_end = end,
                limit,
                "sending batched embedding request"
            );
            self.embed_http_batch(&client, model, batch)
        })
    }
}

impl HttpEmbeddingClient {
    fn embed_http_batch(
        &self,
        client: &Client,
        model: &EmbeddingModel,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>> {
        let url = format!(
            "{}/embeddings",
            self.provider.base_url().trim_end_matches('/')
        );
        let request = EmbeddingRequest {
            model: &model.name,
            input: inputs,
        };

        let mut http_request = client.post(url).json(&request);
        if let EmbeddingProvider::OpenAi { auth_token, .. } = &self.provider {
            http_request = http_request.bearer_auth(auth_token);
        }

        let response = http_request.send().with_context(|| {
            format!(
                "embedding provider request failed for model {} ({})",
                model.name,
                self.provider.mode_name()
            )
        })?;
        let status = response.status();
        let body = response
            .text()
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
        parse_response_vectors(decoded, model, inputs.len())
    }
}

fn parse_response_vectors(
    decoded: EmbeddingResponse,
    model: &EmbeddingModel,
    expected_vectors: usize,
) -> Result<Vec<Vec<f32>>> {
    let mut items = decoded.data;
    items.sort_by_key(|item| item.index);

    ensure!(
        items.len() == expected_vectors,
        "embedding provider returned {} vectors for {} inputs",
        items.len(),
        expected_vectors
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

fn embed_inputs_in_batches<F>(
    inputs: &[String],
    max_batch_size: usize,
    mut embed_batch: F,
) -> Result<Vec<Vec<f32>>>
where
    F: FnMut(&[String], usize, usize, usize) -> Result<Vec<Vec<f32>>>,
{
    if inputs.is_empty() {
        return Ok(Vec::new());
    }

    let batch_size = max_batch_size;
    let mut vectors = Vec::with_capacity(inputs.len());

    for (batch_index, batch_inputs) in inputs.chunks(batch_size).enumerate() {
        let start = batch_index * batch_size;
        let end = start + batch_inputs.len();
        let mut batch_vectors =
            embed_batch(batch_inputs, batch_index, start, end).with_context(|| {
                format!(
                    "embedding batch {} failed for input range [{}..{})",
                    batch_index + 1,
                    start,
                    end
                )
            })?;

        ensure!(
            batch_vectors.len() == batch_inputs.len(),
            "embedding batch {} returned {} vectors for {} inputs",
            batch_index + 1,
            batch_vectors.len(),
            batch_inputs.len()
        );

        vectors.append(&mut batch_vectors);
    }

    Ok(vectors)
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
    use std::sync::{Arc, Mutex};

    use anyhow::anyhow;
    use ask_core::models::EmbeddingModel;

    use super::{DeterministicEmbeddingClient, EmbeddingClient, embed_inputs_in_batches};

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
    fn batch_helper_keeps_exact_limit_in_single_batch() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let inputs = (0..32).map(|i| format!("input-{i}")).collect::<Vec<_>>();
        let calls_for_closure = Arc::clone(&calls);

        let vectors = embed_inputs_in_batches(&inputs, 32, move |batch, _, start, end| {
            calls_for_closure
                .lock()
                .unwrap()
                .push((batch.len(), start, end));
            Ok(batch
                .iter()
                .map(|value| {
                    vec![
                        value
                            .strip_prefix("input-")
                            .unwrap()
                            .parse::<f32>()
                            .unwrap(),
                    ]
                })
                .collect())
        })
        .unwrap();

        assert_eq!(*calls.lock().unwrap(), vec![(32, 0, 32)]);
        assert_eq!(vectors.len(), 32);
        assert_eq!(vectors[0], vec![0.0]);
        assert_eq!(vectors[31], vec![31.0]);
    }

    #[test]
    fn batch_helper_splits_over_limit_and_preserves_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let inputs = (0..53).map(|i| format!("input-{i}")).collect::<Vec<_>>();
        let calls_for_closure = Arc::clone(&calls);

        let vectors = embed_inputs_in_batches(&inputs, 32, move |batch, _, start, end| {
            calls_for_closure
                .lock()
                .unwrap()
                .push((batch.len(), start, end));
            Ok(batch
                .iter()
                .map(|value| {
                    vec![
                        value
                            .strip_prefix("input-")
                            .unwrap()
                            .parse::<f32>()
                            .unwrap(),
                    ]
                })
                .collect())
        })
        .unwrap();

        assert_eq!(*calls.lock().unwrap(), vec![(32, 0, 32), (21, 32, 53)]);
        assert_eq!(vectors.len(), 53);
        assert_eq!(vectors[0], vec![0.0]);
        assert_eq!(vectors[31], vec![31.0]);
        assert_eq!(vectors[32], vec![32.0]);
        assert_eq!(vectors[52], vec![52.0]);
    }

    #[test]
    fn batch_helper_adds_context_on_subrequest_failure() {
        let inputs = (0..33).map(|i| format!("input-{i}")).collect::<Vec<_>>();

        let err = embed_inputs_in_batches(&inputs, 32, |batch, batch_index, _, _| {
            if batch_index == 1 {
                Err(anyhow!("synthetic provider failure"))
            } else {
                Ok(batch.iter().map(|_| vec![1.0]).collect())
            }
        })
        .unwrap_err();

        let err_text = format!("{err:#}");
        assert!(err_text.contains("embedding batch 2 failed for input range [32..33)"));
        assert!(err_text.contains("synthetic provider failure"));
    }
}
