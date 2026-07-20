FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /uvx /bin/

WORKDIR /workspace
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY apps ./apps
RUN uv sync --frozen --no-dev --extra demo-app --no-editable

FROM python:3.12-slim-bookworm AS runtime

ARG APP_UID=10001
RUN useradd --create-home --uid "${APP_UID}" --shell /usr/sbin/nologin retrieval

WORKDIR /workspace
ENV PATH="/workspace/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATABRICKS_APP_PORT=8000

COPY --from=builder --chown=retrieval:retrieval /workspace/.venv ./.venv
USER retrieval

EXPOSE 8000
ENTRYPOINT ["retrieval-demo-app"]
