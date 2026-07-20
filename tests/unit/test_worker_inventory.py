from __future__ import annotations

from pathlib import Path

import pytest
from temporalio import activity, workflow
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from retrieval.config import RetrievalTemporalConfig
from retrieval.temporal.activities.provider_api import EmptyProviderGateway
from retrieval.temporal.activities.repositories import (
    InMemoryRetrievalRepository,
    InMemoryStagingStore,
)
from retrieval.temporal.runtime_config import TemporalRuntimeConfig
from retrieval.temporal.worker import (
    V2_WORKFLOW_TYPES,
    AdapterBundle,
    _load_adapters,
    build_workers,
)
from retrieval.temporal.workflows.legacy import LEGACY_DRAIN_WORKFLOWS

EXPECTED_V2_TYPES = {
    "ActivateUserWorkflow",
    "CleanupUsersWorkflow",
    "CommentsResyncWorkflow",
    "DeactivateAllUsersWorkflow",
    "DeactivateOneUserWorkflow",
    "DeactivateStoreWorkflow",
    "DeactivateUserWorkflow",
    "DocumentIngestionWorkflow",
    "FailedUserRemediationWorkflow",
    "FilesPageWorkflow",
    "RemoveObjectsWorkflow",
    "ResourcePagesWorkflow",
    "ResourceSyncWorkflow",
    "RootSyncWorkflow",
    "StoreControllerWorkflow",
    "UserQuotaWorkflow",
    "UserSyncWorkflow",
    "ProviderPreflightWorkflow",
}

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_secret_env_files_are_excluded_from_git_and_worker_build_context() -> None:
    for ignore_file in (".gitignore", ".dockerignore"):
        patterns = (_REPOSITORY_ROOT / ignore_file).read_text().splitlines()
        assert "*.env" in patterns
        assert ".env*" in patterns
        assert "!.env.example" in patterns


def test_databricks_bundle_syncs_required_repository_root() -> None:
    bundle = (_REPOSITORY_ROOT / "apps/retrieval_demo/databricks.yml").read_text()

    assert "sync:\n  paths:\n    - ../.." in bundle
    assert "source_code_path: ../.." in bundle
    assert "name: LAKEBASE_ENDPOINT\n            value_from: postgres" in bundle
    assert "name: TEMPORAL_API_KEY\n            value_from: temporal-api-key" in bundle
    assert "name: PGHOST" not in bundle


def test_final_inventory_has_exactly_eighteen_v2_workflow_types() -> None:
    names = {
        workflow._Definition.must_from_class(workflow_type).name
        for workflow_type in V2_WORKFLOW_TYPES
    }

    assert len(V2_WORKFLOW_TYPES) == 18
    assert names == EXPECTED_V2_TYPES
    assert "QuotaWaitWorkflow" not in names
    assert "AccessioningWorkflow" not in names


@pytest.mark.parametrize("workflow_type", V2_WORKFLOW_TYPES)
@pytest.mark.asyncio
async def test_every_v2_workflow_prepares_in_temporal_sandbox(
    workflow_type: type,
) -> None:
    definition = workflow._Definition.must_from_class(workflow_type)
    SandboxedWorkflowRunner().prepare_workflow(definition)


def test_only_two_legacy_drain_types_are_retained() -> None:
    names = {
        workflow._Definition.must_from_class(workflow_type).name
        for workflow_type in LEGACY_DRAIN_WORKFLOWS
    }
    assert names == {"QuotaWaitWorkflow", "AccessioningWorkflow"}


def test_runtime_config_reads_typed_bundle_factory() -> None:
    runtime = TemporalRuntimeConfig.from_env(
        {"RETRIEVAL_ADAPTER_BUNDLE_FACTORY": "retrieval.demo.bootstrap:create_adapter_bundle"}
    )

    assert runtime.adapter_bundle_factory == ("retrieval.demo.bootstrap:create_adapter_bundle")


def test_runtime_config_enables_tls_for_api_key_and_rejects_insecure_override() -> None:
    assert TemporalRuntimeConfig.from_env({"TEMPORAL_API_KEY": "secret"}).tls

    with pytest.raises(ValueError, match="requires TEMPORAL_TLS=true"):
        TemporalRuntimeConfig.from_env(
            {
                "TEMPORAL_API_KEY": "secret",
                "TEMPORAL_TLS": "false",
            }
        )


def test_runtime_config_reads_worker_controller_versioning_names() -> None:
    runtime = TemporalRuntimeConfig.from_env(
        {
            "TEMPORAL_WORKER_DEPLOYMENT_NAME": "registry-retrieval",
            "TEMPORAL_WORKER_BUILD_ID": "sha-1234",
        }
    )

    assert runtime.deployment_name == "registry-retrieval"
    assert runtime.build_id == "sha-1234"


def test_runtime_config_prefers_existing_versioning_names_over_controller_aliases() -> None:
    runtime = TemporalRuntimeConfig.from_env(
        {
            "TEMPORAL_DEPLOYMENT_NAME": "explicit-deployment",
            "TEMPORAL_BUILD_ID": "explicit-build",
            "TEMPORAL_WORKER_DEPLOYMENT_NAME": "controller-deployment",
            "TEMPORAL_WORKER_BUILD_ID": "controller-build",
        }
    )

    assert runtime.deployment_name == "explicit-deployment"
    assert runtime.build_id == "explicit-build"


@pytest.mark.asyncio
async def test_partial_production_adapter_configuration_always_fails_closed() -> None:
    runtime = TemporalRuntimeConfig(
        repository_factory="example.adapters:repository",
        allow_unsafe_in_memory_adapters=True,
    )

    with pytest.raises(RuntimeError, match="all-or-nothing"):
        await _load_adapters(runtime)


@pytest.mark.asyncio
async def test_bundle_factory_cannot_be_combined_with_unsafe_adapters() -> None:
    runtime = TemporalRuntimeConfig(
        adapter_bundle_factory="example.adapters:create_bundle",
        allow_unsafe_in_memory_adapters=True,
    )

    with pytest.raises(RuntimeError, match="cannot be combined"):
        await _load_adapters(runtime)


@pytest.mark.asyncio
async def test_adapter_bundle_closes_each_unique_resource_once() -> None:
    class _Resource:
        def __init__(self) -> None:
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

    repository = _Resource()
    staging = _Resource()
    bundle = AdapterBundle(
        repository=repository,  # type: ignore[arg-type]
        staging_store=staging,  # type: ignore[arg-type]
        provider_gateway=repository,  # type: ignore[arg-type]
        before_document_commit=staging,  # type: ignore[arg-type]
    )

    await bundle.aclose()

    assert repository.close_count == 1
    assert staging.close_count == 1


@pytest.mark.asyncio
async def test_adapter_bundle_attempts_every_close_before_raising() -> None:
    class _Resource:
        def __init__(self, *, fail: bool = False) -> None:
            self.fail = fail
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1
            if self.fail:
                raise RuntimeError("close failed")

    repository = _Resource()
    staging = _Resource(fail=True)
    provider = _Resource()
    bundle = AdapterBundle(
        repository=repository,  # type: ignore[arg-type]
        staging_store=staging,  # type: ignore[arg-type]
        provider_gateway=provider,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="close failed"):
        await bundle.aclose()

    assert repository.close_count == 1
    assert staging.close_count == 1
    assert provider.close_count == 1


@pytest.mark.asyncio
async def test_partial_individual_adapter_load_rolls_back_successes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Resource:
        def __init__(self) -> None:
            self.close_count = 0

        async def aclose(self) -> None:
            self.close_count += 1

    repository = _Resource()
    provider = _Resource()

    async def fake_load(path: str) -> object:
        if path == "example:repository":
            return repository
        if path == "example:provider":
            return provider
        raise RuntimeError("staging factory failed")

    monkeypatch.setattr("retrieval.temporal.worker._load_factory", fake_load)
    runtime = TemporalRuntimeConfig(
        repository_factory="example:repository",
        staging_store_factory="example:staging",
        provider_gateway_factory="example:provider",
    )

    with pytest.raises(RuntimeError, match="staging factory failed"):
        await _load_adapters(runtime)

    assert repository.close_count == 1
    assert provider.close_count == 1


def test_worker_registers_the_bounded_cleanup_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_activity_sets: list[list[object]] = []

    class _Worker:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            captured_activity_sets.append(kwargs.get("activities", []))  # type: ignore[arg-type]

    monkeypatch.setattr("retrieval.temporal.worker.Worker", _Worker)
    build_workers(
        object(),  # type: ignore[arg-type]
        runtime=TemporalRuntimeConfig(),
        config=RetrievalTemporalConfig(),
        repository=InMemoryRetrievalRepository(),
        staging_store=InMemoryStagingStore(),
        provider_gateway=EmptyProviderGateway(),
    )

    registered = {
        activity._Definition.must_from_callable(callable_).name
        for activity_set in captured_activity_sets
        for callable_ in activity_set
    }
    assert "remove_object_batch_generation_fenced" in registered
