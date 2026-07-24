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

- [ ] Add a live Kafka client implementation behind the adapter facade

## Active Attempt

- Task: Add a real Kafka producer/consumer adapter skeleton behind the existing broker traits
- Stage: Stage 3 boomerang
- Last result: Added a typed Kafka adapter facade behind the existing producer/consumer traits, including sanitized adapter config, envelope-to-record conversion, record decoding, malformed-record dead-letter routing, sample orchestrator output, and focused tests. Verified with `make check`.
- Last failure: None
- Next attempt: Add a live Kafka client implementation behind the adapter facade, gated so local tests can keep using the in-memory and staged adapter paths.

## Completed Log

- 2026-07-24: Completed the real Kafka producer/consumer adapter skeleton behind the existing broker traits. Added sanitized adapter configuration, typed Kafka record headers, envelope round-tripping, a staged `KafkaBrokerAdapter`, malformed-record dead-letter routing that avoids payload echo, sample orchestrator output, and 4 focused tests. Verified with `make check`; optional `cargo-audit` and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed Kubernetes scheduler adapter contract tests for node lifecycle handoff. Added sanitized Rust lifecycle handoff events for remote capacity admission and cordon actions, namespace normalization, sample output, and tests proving local/hold decisions do not create Kubernetes node lifecycle events or leak private node details. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed full boomerang cycle for the public-safe Kafka producer/consumer contract slice. Added typed Rust Kafka topics, message envelopes with idempotency/correlation metadata, producer/consumer traits, an in-memory broker, scheduling decision routing, retry and dead-letter behavior, and 5 focused tests. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-23: Completed Stage 1 hygiene. Confirmed `CODEX.md` exists, `.mrgi` is ignored, inspected README.md and repository structure, and selected the next showcase task.
