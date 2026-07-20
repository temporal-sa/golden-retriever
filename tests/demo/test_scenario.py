from __future__ import annotations

import hashlib

import pytest

from retrieval.demo.config import DemoConfig, DemoConfigurationError, DemoModeDisabledError
from retrieval.demo.fixtures import (
    FixtureStagingStore,
    InvalidFixtureUriError,
    load_northstar_scenario,
)


def test_demo_mode_is_off_by_default_and_requires_explicit_opt_in() -> None:
    config = DemoConfig.from_env({})

    assert config.enabled is False
    with pytest.raises(DemoModeDisabledError):
        config.require_enabled()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("RETRIEVAL_DEMO_HOLD_TIMEOUT_SECONDS", "nan"),
        ("RETRIEVAL_DEMO_HOLD_TIMEOUT_SECONDS", "inf"),
        ("RETRIEVAL_DEMO_CONTROL_POLL_SECONDS", "-inf"),
        ("RETRIEVAL_DEMO_STORE_KEY_PREFIX", "custom"),
    ],
)
def test_demo_configuration_rejects_nonfinite_values_and_custom_store_prefix(
    name: str,
    value: str,
) -> None:
    with pytest.raises(DemoConfigurationError):
        DemoConfig.from_env({name: value})


async def test_manifest_has_five_hash_verified_documents_and_safe_fixture_uris() -> None:
    scenario = load_northstar_scenario()
    staging = FixtureStagingStore(scenario)

    assert scenario.display_name == "Northstar AI"
    assert scenario.baseline_generation == 7
    assert scenario.quota_retry_after_seconds == 5
    assert len(scenario.documents) == 5
    assert scenario.documents[-1].document_key == scenario.held_document_key
    for document in scenario.documents:
        body = await staging.get(document.fixture_uri)
        assert hashlib.sha256(body).hexdigest() == document.content_hash

    for invalid_uri in (
        "fixture://northstar/../scenario.json",
        "fixture://northstar/%2e%2e/scenario.json",
        "fixture://other/northstar-qbr.md",
        "file:///etc/passwd",
        "fixture://northstar/not-in-manifest.md",
    ):
        with pytest.raises(InvalidFixtureUriError):
            await staging.get(invalid_uri)
