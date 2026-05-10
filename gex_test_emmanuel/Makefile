SHELL := /bin/bash

.PHONY: all lint-fix test sync

# Ensure project's dev environment is available using uv
sync:
	@echo "Syncing dev environment for integration-api"
	@uv sync --dev || { echo "uv sync failed; ensure uv is installed and configured"; exit 1; }

# Run formatter/fixer. Prefers ruff (with --fix).
lint-fix: sync
	@echo "Running lint/format (ruff --fix)"
	@uv run ruff check --fix . || echo "No formatter found (add ruff to dev deps)"
