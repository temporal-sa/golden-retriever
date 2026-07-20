from __future__ import annotations

from types import SimpleNamespace

import pytest

from retrieval.embeddings import (
    DatabricksEmbeddingProvider,
    EmbeddingError,
    create_embedding_provider,
)


class _ServingEndpoints:
    def __init__(self, vectors) -> None:
        self.vectors = vectors
        self.calls = []

    def query(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(data=[SimpleNamespace(embedding=vector) for vector in self.vectors])


class _Workspace:
    def __init__(self, vectors) -> None:
        self.serving_endpoints = _ServingEndpoints(vectors)


@pytest.mark.asyncio
async def test_databricks_embedding_provider_batches_and_validates_dimension() -> None:
    workspace = _Workspace(((0.1, 0.2, 0.3), (0.4, 0.5, 0.6)))
    provider = DatabricksEmbeddingProvider(
        "embedding-endpoint",
        dimension=3,
        document_prefix="passage: ",
        workspace_client=workspace,
    )

    vectors = await provider.embed(("alpha", "beta"))

    assert vectors == ((0.1, 0.2, 0.3), (0.4, 0.5, 0.6))
    assert workspace.serving_endpoints.calls == [
        {
            "name": "embedding-endpoint",
            "input": ["passage: alpha", "passage: beta"],
        }
    ]


@pytest.mark.asyncio
async def test_embedding_provider_rejects_malformed_endpoint_response() -> None:
    provider = DatabricksEmbeddingProvider(
        "embedding-endpoint",
        dimension=3,
        workspace_client=_Workspace(((0.1, 0.2),)),
    )

    with pytest.raises(EmbeddingError, match="dimension mismatch"):
        await provider.embed(("alpha",))


def test_embedding_factory_requires_explicit_endpoint() -> None:
    with pytest.raises(ValueError, match="DATABRICKS_EMBEDDING_ENDPOINT"):
        create_embedding_provider({})

    provider = create_embedding_provider(
        {
            "DATABRICKS_EMBEDDING_ENDPOINT": "system.ai.gte-large-en",
            "RETRIEVAL_EMBEDDING_DIMENSION": "1024",
        }
    )
    assert provider.identity == "system.ai.gte-large-en"
    assert provider.dimension == 1024
