#!/usr/bin/env python3
"""Validate the public CI workflow keeps the expected interview tracks covered."""

from pathlib import Path


WORKFLOW = Path(".github/workflows/ci.yml")

REQUIRED_SNIPPETS = (
    "cargo fmt --all -- --check",
    "cargo clippy --workspace --all-targets -- -D warnings",
    "make check-automation check-workflows",
    "opentofu/setup-opentofu",
    "run: tofu test",
)


def main() -> None:
    if not WORKFLOW.exists():
        raise SystemExit(f"{WORKFLOW} does not exist")

    content = WORKFLOW.read_text(encoding="utf-8")
    missing = [snippet for snippet in REQUIRED_SNIPPETS if snippet not in content]
    if missing:
        formatted = "\n".join(f"- {snippet}" for snippet in missing)
        raise SystemExit(f"{WORKFLOW} is missing required checks:\n{formatted}")


if __name__ == "__main__":
    main()
