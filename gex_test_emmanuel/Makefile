SHELL := /bin/bash

lint-fix:
	@uv sync
	@uv sync --dev
	@uv run ruff check --fix .
