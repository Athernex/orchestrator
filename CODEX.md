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
- [ ] Stage 2: implement the selected focused task to working state
- [ ] Stage 2: verify deeply enough to decide whether the task deserves a checkmark

## Task List

- [ ] Implement a public-safe Kafka producer/consumer contract slice in the Rust orchestrator

## Active Attempt

- Task: Select the next showcase task from README.md and repository structure
- Stage: Stage 1 hygiene and next-task selection
- Last result: Selected the Rust Kafka producer/consumer contract slice as the next task because it best demonstrates distributed-systems orchestration, typed workflow boundaries, failure handling, and public-safe infrastructure design.
- Last failure: None
- Next attempt: In Stage 2, add the smallest working Kafka-facing slice: typed command/observation envelopes, producer/consumer boundary traits or adapters, tests for routing and dead-letter behavior, and README/CODEX updates after verification.

## Completed Log

- 2026-07-23: Completed Stage 1 hygiene. Confirmed `CODEX.md` exists, `.mrgi` is ignored, inspected README.md and repository structure, and selected the next showcase task.
