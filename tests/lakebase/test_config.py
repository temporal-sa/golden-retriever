from __future__ import annotations

import pytest

from retrieval.lakebase.config import (
    LakebaseConfig,
    LakebaseConfigurationError,
)

OAUTH_ENV = {
    "PGHOST": "primary.example.databricks.com",
    "PGDATABASE": "retrieval_demo",
    "PGUSER": "app-service-principal",
    "LAKEBASE_ENDPOINT": "projects/demo/branches/production/endpoints/primary",
}


def test_from_env_prefers_canonical_pg_values_and_applies_defaults() -> None:
    config = LakebaseConfig.from_env(
        {
            **OAUTH_ENV,
            "LAKEBASE_HOST": "ignored.example.com",
            "LAKEBASE_DATABASE": "ignored",
            "LAKEBASE_USER": "ignored",
        }
    )

    assert config.host == "primary.example.databricks.com"
    assert config.database == "retrieval_demo"
    assert config.user == "app-service-principal"
    assert config.port == 5432
    assert config.sslmode == "require"
    assert config.pool_min_size == 1
    assert config.pool_max_size == 10
    assert config.uses_oauth


def test_from_env_accepts_lakebase_aliases_and_local_password() -> None:
    config = LakebaseConfig.from_env(
        {
            "LAKEBASE_HOST": "127.0.0.1",
            "LAKEBASE_PORT": "55432",
            "LAKEBASE_DATABASE": "retrieval_test",
            "LAKEBASE_USER": "developer",
            "LAKEBASE_PASSWORD": "local-secret",
            "LAKEBASE_POOL_MIN_SIZE": "0",
            "LAKEBASE_POOL_MAX_SIZE": "4",
            "LAKEBASE_STATEMENT_TIMEOUT_SECONDS": "2.5",
        }
    )

    assert config.host == "127.0.0.1"
    assert config.port == 55432
    assert config.password == "local-secret"
    assert config.pool_min_size == 0
    assert config.pool_max_size == 4
    assert config.statement_timeout_seconds == 2.5
    assert not config.uses_oauth
    assert "local-secret" not in repr(config)


def test_process_specific_default_pool_max_is_overridable() -> None:
    assert LakebaseConfig.from_env(OAUTH_ENV, default_pool_max_size=20).pool_max_size == 20
    assert (
        LakebaseConfig.from_env(
            {**OAUTH_ENV, "LAKEBASE_POOL_MAX_SIZE": "7"},
            default_pool_max_size=20,
        ).pool_max_size
        == 7
    )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"PGHOST": ""}, "PGHOST/LAKEBASE_HOST is required"),
        ({"LAKEBASE_ENDPOINT": ""}, "LAKEBASE_ENDPOINT is required"),
        ({"PGSSLMODE": "disable"}, "sslmode must require TLS"),
        ({"PGPORT": "not-a-port"}, "PGPORT/LAKEBASE_PORT must be an integer"),
        ({"LAKEBASE_POOL_MIN_SIZE": "11"}, "must not exceed"),
        ({"LAKEBASE_LOCK_TIMEOUT_SECONDS": "0"}, "must be finite and positive"),
        ({"LAKEBASE_LOCK_TIMEOUT_SECONDS": "nan"}, "must be finite and positive"),
        ({"LAKEBASE_LOCK_TIMEOUT_SECONDS": "inf"}, "must be finite and positive"),
    ],
)
def test_invalid_settings_fail_before_connecting(changes: dict[str, str], message: str) -> None:
    source = {**OAUTH_ENV, **changes}
    with pytest.raises(LakebaseConfigurationError, match=message):
        LakebaseConfig.from_env(source)


def test_oauth_and_static_password_cannot_be_ambiguous() -> None:
    with pytest.raises(LakebaseConfigurationError, match="either LAKEBASE_ENDPOINT"):
        LakebaseConfig.from_env({**OAUTH_ENV, "PGPASSWORD": "do-not-log-me"})


def test_endpoint_must_be_the_full_endpoint_resource_path() -> None:
    with pytest.raises(LakebaseConfigurationError, match="endpoint resource path"):
        LakebaseConfig.from_env({**OAUTH_ENV, "LAKEBASE_ENDPOINT": "projects/demo"})
