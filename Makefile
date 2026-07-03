.PHONY: local-up local-down local-logs run-orchestrator run-paperclip run-codex-scheduler fmt check

COMPOSE_FILE := infrastructure/local-dev/docker-compose.yml

local-up:
	docker compose -f $(COMPOSE_FILE) up -d

local-down:
	docker compose -f $(COMPOSE_FILE) down

local-logs:
	docker compose -f $(COMPOSE_FILE) logs -f

run-orchestrator:
	cargo run -p orchestrator

run-paperclip:
	npx --registry https://registry.npmjs.org paperclipai run

run-codex-scheduler:
	python3 tools/codex_scheduler_bridge.py

fmt:
	cargo fmt --all

check:
	cargo check --workspace
