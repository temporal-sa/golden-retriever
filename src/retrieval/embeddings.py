"""Embedding ports and the Databricks Model Serving adapter.

Embedding calls belong inside Activities and request handlers.  Only compact
results or opaque references may cross a Temporal Workflow boundary.
"""

from __future__ import annotations

import asyncio
import math
import os
from collections.abc import Mapping, Sequence
from typing import Any, Protocol


class EmbeddingError(RuntimeError):
    """An embedding endpoint returned an unusable response."""


class EmbeddingProvider(Protocol):
    """Async embedding boundary shared by ingestion and query paths."""

    @property
    def identity(self) -> str: ...

    @property
    def dimension(self) -> int: ...

    async def embed(
        self,
        texts: Sequence[str],
        *,
        query: bool = False,
    ) -> tuple[tuple[float, ...], ...]: ...


class DatabricksEmbeddingProvider:
    """Call a Databricks Model Serving embedding endpoint without blocking the loop."""

    def __init__(
        self,
        endpoint_name: str,
        *,
        dimension: int = 1024,
        document_prefix: str = "",
        query_prefix: str = "",
        workspace_client: Any | None = None,
    ) -> None:
        if not endpoint_name.strip():
            raise ValueError("embedding endpoint name must not be empty")
        if dimension < 1:
            raise ValueError("embedding dimension must be positive")
        self._endpoint_name = endpoint_name.strip()
        self._dimension = dimension
        self._document_prefix = document_prefix
        self._query_prefix = query_prefix
        self._workspace_client = workspace_client

    @property
    def identity(self) -> str:
        return self._endpoint_name

    @property
    def dimension(self) -> int:
        return self._dimension

    async def embed(
        self,
        texts: Sequence[str],
        *,
        query: bool = False,
    ) -> tuple[tuple[float, ...], ...]:
        values = tuple(texts)
        if not values:
            return ()
        if any(not text.strip() for text in values):
            raise ValueError("embedding inputs must not be empty")
        prefix = self._query_prefix if query else self._document_prefix
        inputs = [f"{prefix}{text}" for text in values]
        response = await asyncio.to_thread(self._query_endpoint, inputs)
        data = getattr(response, "data", None)
        if data is None and isinstance(response, Mapping):
            data = response.get("data")
        if data is None or len(data) != len(inputs):
            raise EmbeddingError("embedding endpoint returned the wrong number of vectors")
        vectors: list[tuple[float, ...]] = []
        for item in data:
            raw = (
                item.get("embedding")
                if isinstance(item, Mapping)
                else getattr(item, "embedding", None)
            )
            if raw is None:
                raise EmbeddingError("embedding endpoint response omitted an embedding")
            vector = tuple(float(value) for value in raw)
            if len(vector) != self._dimension:
                raise EmbeddingError(
                    f"embedding dimension mismatch: expected {self._dimension}, found {len(vector)}"
                )
            if any(not math.isfinite(value) for value in vector):
                raise EmbeddingError("embedding endpoint returned a non-finite value")
            vectors.append(vector)
        return tuple(vectors)

    def _query_endpoint(self, inputs: list[str]) -> Any:
        client = self._workspace_client
        if client is None:
            try:
                from databricks.sdk import WorkspaceClient
            except ImportError as exc:  # pragma: no cover - guarded by deployment extras
                raise RuntimeError("Databricks embeddings require databricks-sdk") from exc
            client = WorkspaceClient()
            self._workspace_client = client
        return client.serving_endpoints.query(name=self._endpoint_name, input=inputs)


def create_embedding_provider(
    environ: Mapping[str, str] | None = None,
) -> DatabricksEmbeddingProvider:
    values = os.environ if environ is None else environ
    endpoint = values.get("DATABRICKS_EMBEDDING_ENDPOINT", "").strip()
    if not endpoint:
        raise ValueError("DATABRICKS_EMBEDDING_ENDPOINT is required")
    raw_dimension = values.get("RETRIEVAL_EMBEDDING_DIMENSION", "1024")
    try:
        dimension = int(raw_dimension)
    except ValueError as exc:
        raise ValueError("RETRIEVAL_EMBEDDING_DIMENSION must be an integer") from exc
    return DatabricksEmbeddingProvider(
        endpoint,
        dimension=dimension,
        document_prefix=values.get("RETRIEVAL_EMBEDDING_DOCUMENT_PREFIX", ""),
        query_prefix=values.get("RETRIEVAL_EMBEDDING_QUERY_PREFIX", ""),
    )


__all__ = [
    "DatabricksEmbeddingProvider",
    "EmbeddingError",
    "EmbeddingProvider",
    "create_embedding_provider",
]
