# CODEX.md

This file is the MRGI working ledger for detachable Codex repo-improvement loops.

## Operating Rules

- Treat README.md and this CODEX.md as the source of truth for project direction and current stage.
- Each unchecked checkbox is a task from the agent's perspective. Nested checkboxes are valid tasks.
- Prefer the smallest useful change that showcases the repository owner's skillsets.
- Do not erase useful history. Move completed tasks to the completed log.
- If a task fails, keep it unchecked and annotate the latest failure, likely cause, and next attempt.
- Commit only coherent, verified changes. Use clear commit messages and push when a remote is configured.
- Return control to the human between stages with a concise boomerang summary: changed, verified, verdict, suggested next task.

## Current Stage

- [x] Hygiene: inspect repo state, clean stale task notes, identify the next best task
- [x] Stage 2: implement the selected focused task to working state
- [x] Stage 2: verify deeply enough to decide whether the task deserves a checkmark
- [x] Stage 3: update repo ledger, commit verified work, and report the boomerang verdict

## Task List

- [ ] Add Paperclip adapter implementation

## Active Attempt

- Task: Add load tests for Kafka backpressure and retry behavior
- Stage: Stage 3 boomerang
- Last result: Added bounded staged Kafka load tests for max-poll behavior, retry classification, and dead-letter classification, plus an opt-in live Kafka load smoke that publishes eight records and verifies drain batches stay within `max_poll_records`. Verified with `make check` and `ATHERNEX_KAFKA_INTEGRATION=1 cargo test -p orchestrator live_kafka_load_smoke_bounds_polling_when_enabled -- --nocapture`.
- Last failure: None
- Next attempt: Add a public-safe Paperclip adapter implementation behind a narrow request/response boundary.

## Completed Log

- 2026-07-24: Completed bounded Kafka load tests for backpressure and retry behavior. Added max-poll enforcement to the staged Kafka adapter, a deterministic load report covering retry and dead-letter classification, unit tests for bounded staged drains, an opt-in live Kafka load smoke against local Kafka, and README validation commands. Verified with `make check` and `ATHERNEX_KAFKA_INTEGRATION=1 cargo test -p orchestrator live_kafka_load_smoke_bounds_polling_when_enabled -- --nocapture`; optional `cargo-audit` and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed the live Kafka client implementation behind the adapter facade. Added an `rdkafka` live broker with fallible publish/drain methods, header-preserving record conversion, explicit UTF-8/decode errors, opt-in live smoke validation, and an environment-gated Kafka integration test. Updated local compose from unavailable `bitnami/kafka:3.7` to official `apache/kafka:3.7.1`, moved Kafka UI to `127.0.0.1:18080`, and verified real publish/consume against local Kafka. Verified with `make check`, `make local-up`, `ATHERNEX_KAFKA_INTEGRATION=1 cargo test -p orchestrator live_kafka_broker_round_trips_against_local_kafka_when_enabled -- --nocapture`, and `ATHERNEX_LIVE_KAFKA_SMOKE=1 cargo run -p orchestrator`; optional `cargo-audit` and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed the real Kafka producer/consumer adapter skeleton behind the existing broker traits. Added sanitized adapter configuration, typed Kafka record headers, envelope round-tripping, a staged `KafkaBrokerAdapter`, malformed-record dead-letter routing that avoids payload echo, sample orchestrator output, and 4 focused tests. Verified with `make check`; optional `cargo-audit` and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed Kubernetes scheduler adapter contract tests for node lifecycle handoff. Added sanitized Rust lifecycle handoff events for remote capacity admission and cordon actions, namespace normalization, sample output, and tests proving local/hold decisions do not create Kubernetes node lifecycle events or leak private node details. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed full boomerang cycle for the public-safe Kafka producer/consumer contract slice. Added typed Rust Kafka topics, message envelopes with idempotency/correlation metadata, producer/consumer traits, an in-memory broker, scheduling decision routing, retry and dead-letter behavior, and 5 focused tests. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-23: Completed Stage 1 hygiene. Confirmed `CODEX.md` exists, `.mrgi` is ignored, inspected README.md and repository structure, and selected the next showcase task.
