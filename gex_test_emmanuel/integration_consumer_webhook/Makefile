SHELL := /bin/bash

test:
	@uv sync
	@uv sync --extra dev
	@.venv/bin/python -m pytest -q

lint-fix:
	@uv sync
	@uv sync --extra dev
	@uv run ruff check --fix .
