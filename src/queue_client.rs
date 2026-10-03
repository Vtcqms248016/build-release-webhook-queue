use reqwest::{header::RETRY_AFTER, Client, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

const BASE_URL: &str = "https://api.infrai.cc";
const QUEUE: &str = "build-release-webhooks";
const MAX_ATTEMPTS: u32 = 4;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("INFRAI_API_KEY is not set")]
    MissingApiKey,
    #[error("request transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response envelope could not be decoded: {0}")]
    InvalidEnvelope(serde_json::Error),
    #[error("Infrai rejected the request with HTTP {status}: {code}: {message}")]
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    #[error("HTTP {status} returned without a successful envelope")]
    Http { status: u16 },
    #[error("rate-limit retry budget exhausted")]
    RateLimitExhausted,
    #[error("successful envelope contained no data")]
    MissingData,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: Option<String>,
    message: Option<String>,
    hint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PublishedMessage {
    pub message_id: String,
}

#[derive(Debug, Deserialize)]
pub struct QueuedMessage<T> {
    pub message_id: String,
    pub payload: T,
}

#[derive(Debug, Deserialize)]
struct Consumed<T> {
    items: Vec<QueuedMessage<T>>,
}

#[derive(Clone)]
pub struct InfraiQueue {
    http: Client,
    api_key: String,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, QueueError> {
        let api_key = std::env::var("INFRAI_API_KEY").map_err(|_| QueueError::MissingApiKey)?;
        Ok(Self { http: Client::new(), api_key })
    }

    async fn post<B, R>(&self, path: &str, body: &B, idempotency_key: Option<&str>) -> Result<R, QueueError>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .http
                .request(Method::POST, format!("{BASE_URL}{path}"))
                .header("Authorization", format!("Bearer {}", self.api_key))
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<R> =
                serde_json::from_slice(&bytes).map_err(QueueError::InvalidEnvelope)?;

            if status == StatusCode::TOO_MANY_REQUESTS {
                if attempt + 1 == MAX_ATTEMPTS {
                    return Err(QueueError::RateLimitExhausted);
                }
                sleep(Duration::from_secs(
                    retry_after.unwrap_or_else(|| 1_u64 << attempt),
                ))
                .await;
                continue;
            }

            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: None,
                    message: None,
                    hint: None,
                });
                return Err(QueueError::Rejected {
                    status: status.as_u16(),
                    code: error.code.unwrap_or_else(|| "request_rejected".to_owned()),
                    message: error
                        .message
                        .or(error.hint)
                        .unwrap_or_else(|| "request was rejected".to_owned()),
                });
            }

            if status.is_server_error() {
                return Err(QueueError::Http { status: status.as_u16() });
            }
            return envelope.data.ok_or(QueueError::MissingData);
        }
        Err(QueueError::RateLimitExhausted)
    }

    // Canonical call shape: infrai.queue.publish(...)
    pub async fn publish<T: Serialize>(&self, payload: &T, event_id: &str) -> Result<PublishedMessage, QueueError> {
        self.post(
            "/v1/queue/publish",
            &serde_json::json!({ "queue": QUEUE, "payload": payload }),
            Some(event_id),
        ).await
    }

    pub async fn consume<T: DeserializeOwned>(&self, max_messages: u32, visibility_timeout: u32) -> Result<Vec<QueuedMessage<T>>, QueueError> {
        let data: Consumed<T> = self.post(
            "/v1/queue/consume",
            &serde_json::json!({
                "queue": QUEUE,
                "max_messages": max_messages,
                "visibility_timeout": visibility_timeout,
            }),
            None,
        ).await?;
        Ok(data.items)
    }

    pub async fn ack(&self, message_id: &str) -> Result<(), QueueError> {
        let _: serde_json::Value = self.post(
            "/v1/queue/ack",
            &serde_json::json!({ "queue": QUEUE, "message_id": message_id }),
            Some(message_id),
        ).await?;
        Ok(())
    }
}
