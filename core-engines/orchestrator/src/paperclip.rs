use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const SCHEMA: &str = "athernex.paperclip.review.v1";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewRequest {
    pub schema_version: String,
    pub correlation_id: String,
    pub idempotency_key: String,
    pub action_class: String,
    pub resource_class: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewResponse {
    schema_version: String,
    correlation_id: String,
    idempotency_key: String,
    outcome: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReviewOutcome {
    Approved,
    Rejected,
    Pending,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AdapterError {
    InvalidRequest,
    Timeout,
    Transport,
    InvalidResponse,
}

pub trait ReviewTransport {
    fn exchange(&mut self, request: &[u8], timeout: Duration) -> Result<Vec<u8>, AdapterError>;
}

pub fn review(
    transport: &mut impl ReviewTransport,
    request: &ReviewRequest,
) -> Result<ReviewOutcome, AdapterError> {
    let safe = |value: &str| {
        !value.is_empty()
            && value.len() <= 96
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    };
    if request.schema_version != SCHEMA
        || !safe(&request.correlation_id)
        || !safe(&request.idempotency_key)
        || !safe(&request.action_class)
        || !safe(&request.resource_class)
    {
        return Err(AdapterError::InvalidRequest);
    }
    let payload = serde_json::to_vec(request).map_err(|_| AdapterError::InvalidRequest)?;
    for attempt in 0..3 {
        match transport.exchange(&payload, Duration::from_secs(2)) {
            Ok(raw) => {
                if raw.len() > 4096 {
                    return Err(AdapterError::InvalidResponse);
                }
                let response: ReviewResponse =
                    serde_json::from_slice(&raw).map_err(|_| AdapterError::InvalidResponse)?;
                if response.schema_version != SCHEMA
                    || response.correlation_id != request.correlation_id
                    || response.idempotency_key != request.idempotency_key
                {
                    return Err(AdapterError::InvalidResponse);
                }
                return match response.outcome.as_str() {
                    "approved" => Ok(ReviewOutcome::Approved),
                    "rejected" => Ok(ReviewOutcome::Rejected),
                    "pending" => Ok(ReviewOutcome::Pending),
                    _ => Err(AdapterError::InvalidResponse),
                };
            }
            Err(AdapterError::Timeout | AdapterError::Transport) if attempt < 2 => continue,
            Err(error) => return Err(error),
        }
    }
    Err(AdapterError::Timeout)
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fixture {
        failures: usize,
        outcome: &'static str,
        calls: usize,
    }
    impl ReviewTransport for Fixture {
        fn exchange(&mut self, _: &[u8], timeout: Duration) -> Result<Vec<u8>, AdapterError> {
            assert_eq!(timeout, Duration::from_secs(2));
            self.calls += 1;
            if self.calls <= self.failures {
                return Err(AdapterError::Timeout);
            }
            Ok(format!(r#"{{"schema_version":"{SCHEMA}","correlation_id":"c1","idempotency_key":"i1","outcome":"{}"}}"#,self.outcome).into_bytes())
        }
    }
    fn request() -> ReviewRequest {
        ReviewRequest {
            schema_version: SCHEMA.into(),
            correlation_id: "c1".into(),
            idempotency_key: "i1".into(),
            action_class: "schedule".into(),
            resource_class: "compute".into(),
        }
    }
    #[test]
    fn retry_and_review_outcomes() {
        let mut fixture = Fixture {
            failures: 2,
            outcome: "approved",
            calls: 0,
        };
        assert_eq!(
            review(&mut fixture, &request()),
            Ok(ReviewOutcome::Approved)
        );
        assert_eq!(fixture.calls, 3);
        let mut rejected = Fixture {
            failures: 0,
            outcome: "rejected",
            calls: 0,
        };
        assert_eq!(
            review(&mut rejected, &request()),
            Ok(ReviewOutcome::Rejected)
        );
    }
    #[test]
    fn timeout_and_malformed_are_closed() {
        let mut timeout = Fixture {
            failures: 3,
            outcome: "approved",
            calls: 0,
        };
        assert_eq!(review(&mut timeout, &request()), Err(AdapterError::Timeout));
        let mut malformed = Fixture {
            failures: 0,
            outcome: "other",
            calls: 0,
        };
        assert_eq!(
            review(&mut malformed, &request()),
            Err(AdapterError::InvalidResponse)
        );
    }
}
