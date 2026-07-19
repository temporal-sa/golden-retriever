.PHONY: install verify integration replay headless app

install:
	uv sync --frozen --extra dev

verify:
	uv run ruff check .
	uv run ruff format --check .
	uv run python -m compileall -q src apps tests
	uv run pytest
	uv run retrieval-demo-headless --json
	node --check apps/retrieval_demo/static/app.js
	uv build

integration:
	RUN_TEMPORAL_INTEGRATION=1 uv run pytest -m integration tests/integration tests/demo

replay:
	uv run pytest -m replay tests/replay

headless:
	uv run retrieval-demo-headless

app:
	RETRIEVAL_DEMO_MODE=true uv run retrieval-demo-app
