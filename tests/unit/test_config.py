from __future__ import annotations

from datetime import timedelta

import pytest

from retrieval.config import ConfigurationError, RetrievalTemporalConfig

EXPECTED_ENVIRONMENT_VARIABLES = {
    "STORE_SYNC_MAX_ACTIVE_USERS",
    "STORE_SYNC_USER_PAGE_SIZE",
    "ROUND_USER_WINDOW_SIZE",
    "ROUND_PAGE_SLICE_SIZE",
    "RESOURCE_CONCURRENCY",
    "FILES_PAGE_WINDOW_SIZE",
    "FILES_PER_PAGE_CONCURRENCY",
    "DOCUMENT_INGESTION_CONCURRENCY",
    "USER_QUOTA_MAX_IN_FLIGHT",
    "USER_QUOTA_MAX_PENDING_REQUESTS",
    "USER_QUOTA_DEDUP_WINDOW_SIZE",
    "USER_QUOTA_CONTINUE_AS_NEW_MESSAGE_COUNT",
    "DEACTIVATION_DRAIN_TIMEOUT",
    "TEMPORAL_ENABLE_PRIORITY_FAIRNESS",
    "TEMPORAL_PROVIDER_QUEUE_RPS",
    "TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT",
}


def test_defaults_are_bounded_and_feature_is_off() -> None:
    config = RetrievalTemporalConfig.from_env({})
    assert config.store_sync_max_active_users > 0
    assert config.files_page_window_size > 0
    assert config.files_per_page_concurrency > 0
    assert config.document_ingestion_concurrency > 0
    assert config.user_quota_max_in_flight > 0
    assert config.deactivation_drain_timeout == timedelta(minutes=5)
    assert config.temporal_enable_priority_fairness is False
    assert set(config.environment_variables()) == EXPECTED_ENVIRONMENT_VARIABLES


def test_all_environment_values_are_typed() -> None:
    config = RetrievalTemporalConfig.from_env(
        {
            "STORE_SYNC_MAX_ACTIVE_USERS": "12",
            "STORE_SYNC_USER_PAGE_SIZE": "50",
            "ROUND_USER_WINDOW_SIZE": "8",
            "ROUND_PAGE_SLICE_SIZE": "3",
            "RESOURCE_CONCURRENCY": "6",
            "FILES_PAGE_WINDOW_SIZE": "7",
            "FILES_PER_PAGE_CONCURRENCY": "9",
            "DOCUMENT_INGESTION_CONCURRENCY": "11",
            "USER_QUOTA_MAX_IN_FLIGHT": "2",
            "USER_QUOTA_MAX_PENDING_REQUESTS": "300",
            "USER_QUOTA_DEDUP_WINDOW_SIZE": "200",
            "USER_QUOTA_CONTINUE_AS_NEW_MESSAGE_COUNT": "900",
            "DEACTIVATION_DRAIN_TIMEOUT": "2.5m",
            "TEMPORAL_ENABLE_PRIORITY_FAIRNESS": "yes",
            "TEMPORAL_PROVIDER_QUEUE_RPS": "25.5",
            "TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT": "4",
        }
    )
    assert config.store_sync_max_active_users == 12
    assert config.store_sync_user_page_size == 50
    assert config.round_user_window_size == 8
    assert config.round_page_slice_size == 3
    assert config.resource_concurrency == 6
    assert config.files_page_window_size == 7
    assert config.files_per_page_concurrency == 9
    assert config.document_ingestion_concurrency == 11
    assert config.user_quota_max_in_flight == 2
    assert config.user_quota_max_pending_requests == 300
    assert config.user_quota_dedup_window_size == 200
    assert config.user_quota_continue_as_new_message_count == 900
    assert config.deactivation_drain_timeout == timedelta(seconds=150)
    assert config.deactivation_drain_timeout_seconds == 150
    assert config.temporal_enable_priority_fairness is True
    assert config.temporal_provider_queue_rps == 25.5
    assert config.temporal_fairness_key_rps_default == 4.0


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("STORE_SYNC_MAX_ACTIVE_USERS", "0"),
        ("FILES_PAGE_WINDOW_SIZE", "-1"),
        ("USER_QUOTA_MAX_IN_FLIGHT", "many"),
        ("USER_QUOTA_MAX_PENDING_REQUESTS", "0"),
        ("DEACTIVATION_DRAIN_TIMEOUT", "later"),
        ("DEACTIVATION_DRAIN_TIMEOUT", "0s"),
        ("TEMPORAL_ENABLE_PRIORITY_FAIRNESS", "sometimes"),
        ("TEMPORAL_PROVIDER_QUEUE_RPS", "0"),
        ("TEMPORAL_FAIRNESS_KEY_RPS_DEFAULT", "-2"),
    ],
)
def test_invalid_environment_values_are_rejected(name: str, value: str) -> None:
    with pytest.raises(ConfigurationError):
        RetrievalTemporalConfig.from_env({name: value})


def test_dedup_window_cannot_be_smaller_than_max_in_flight() -> None:
    with pytest.raises(ConfigurationError, match="dedup"):
        RetrievalTemporalConfig.from_env(
            {
                "USER_QUOTA_MAX_IN_FLIGHT": "5",
                "USER_QUOTA_DEDUP_WINDOW_SIZE": "4",
            }
        )


def test_pending_window_cannot_be_smaller_than_max_in_flight() -> None:
    with pytest.raises(ConfigurationError, match="max_pending"):
        RetrievalTemporalConfig.from_env(
            {
                "USER_QUOTA_MAX_IN_FLIGHT": "5",
                "USER_QUOTA_MAX_PENDING_REQUESTS": "4",
            }
        )


def test_pending_window_has_a_payload_safety_ceiling() -> None:
    with pytest.raises(ConfigurationError, match="must not exceed 350"):
        RetrievalTemporalConfig.from_env({"USER_QUOTA_MAX_PENDING_REQUESTS": "351"})
