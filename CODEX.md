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

- [ ] Add a real Kafka producer/consumer adapter skeleton behind the existing broker traits

## Active Attempt

- Task: Add Kubernetes scheduler adapter contract tests for node lifecycle handoff
- Stage: Stage 3 boomerang
- Last result: Added typed Kubernetes node lifecycle handoff events for scheduler power-on and idle power-off decisions, namespace sanitization, sample orchestrator output, and public-safety contract tests. Verified with `make check`.
- Last failure: None
- Next attempt: Add a minimal real Kafka producer/consumer adapter skeleton behind the existing broker traits while preserving the in-memory broker for tests.

## Completed Log

- 2026-07-24: Completed Kubernetes scheduler adapter contract tests for node lifecycle handoff. Added sanitized Rust lifecycle handoff events for remote capacity admission and cordon actions, namespace normalization, sample output, and tests proving local/hold decisions do not create Kubernetes node lifecycle events or leak private node details. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-24: Completed full boomerang cycle for the public-safe Kafka producer/consumer contract slice. Added typed Rust Kafka topics, message envelopes with idempotency/correlation metadata, producer/consumer traits, an in-memory broker, scheduling decision routing, retry and dead-letter behavior, and 5 focused tests. Verified with `make check`; optional `cargo-audit`, `cargo-cyclonedx`, and OpenTofu checks were skipped because the tools are not installed.
- 2026-07-23: Completed Stage 1 hygiene. Confirmed `CODEX.md` exists, `.mrgi` is ignored, inspected README.md and repository structure, and selected the next showcase task.
