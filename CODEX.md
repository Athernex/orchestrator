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

- [ ] Add Kubernetes scheduler adapter contract tests for node lifecycle handoff

## Active Attempt

- Task: Implement a public-safe Kafka producer/consumer contract slice in the Rust orchestrator
- Stage: Stage 3 boomerang
- Last result: Added typed Kafka topic/envelope contracts, producer/consumer traits, an in-memory broker, scheduling-to-topic routing, retry/dead-letter handling, and tests. Verified with `make check`.
- Last failure: None
- Next attempt: Add Kubernetes scheduler adapter contract tests for sanitized node lifecycle handoff events without private manifests.

## Completed Log

- 2026-07-24: Completed full boomerang cycle for the public-safe Kafka producer/consumer contract slice. Added typed Rust Kafka topics, message envelopes with idempotency/correlation metadata, producer/consumer traits, an in-memory broker, scheduling decision routing, retry and dead-letter behavior, and 5 focused tests. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-23: Completed Stage 1 hygiene. Confirmed `CODEX.md` exists, `.mrgi` is ignored, inspected README.md and repository structure, and selected the next showcase task.
