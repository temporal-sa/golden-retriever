from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError

from retrieval.temporal.activities import provider_api
from retrieval.temporal.activities.provider_api import (
    FetchResourcePageRequest,
    ListActiveUsersRequest,
    ProviderActivities,
    ProviderQuotaExhausted,
)
from retrieval.temporal.common.metrics import (
    PROVIDER_QUOTA_EXHAUSTED,
    PROVIDER_REQUESTS,
    SafeMetricEmitter,
    activity_metrics,
    bounded_metric_attributes,
)
from retrieval.temporal.models.operations import WorkClass
from retrieval.temporal.models.quota import QuotaScope


class _RecordingInstrument:
    def __init__(self, events: list[tuple[str, str, int, dict[str, object]]], kind: str, name: str):
        self._events = events
        self._kind = kind
        self._name = name

    def add(self, value: int, attributes: dict[str, object]) -> None:
        self._events.append((self._kind, self._name, value, attributes))

    def set(self, value: int, attributes: dict[str, object]) -> None:
        self._events.append((self._kind, self._name, value, attributes))

    def record(self, value: int, attributes: dict[str, object]) -> None:
        self._events.append((self._kind, self._name, value, attributes))


class _RecordingMeter:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, int, dict[str, object]]] = []

    def _instrument(self, kind: str, name: str, *_args: object) -> _RecordingInstrument:
        return _RecordingInstrument(self.events, kind, name)

    def create_counter(self, name: str, *_args: object) -> _RecordingInstrument:
        return self._instrument("counter", name)

    def create_gauge(self, name: str, *_args: object) -> _RecordingInstrument:
        return self._instrument("gauge", name)

    def create_histogram(self, name: str, *_args: object) -> _RecordingInstrument:
        return self._instrument("histogram", name)


class _ExplodingMeter:
    def create_counter(self, *_args: object) -> None:
        raise RuntimeError("exporter unavailable")

    def create_gauge(self, *_args: object) -> None:
        raise RuntimeError("exporter unavailable")

    def create_histogram(self, *_args: object) -> None:
        raise RuntimeError("exporter unavailable")


class _QuotaGateway:
    async def list_active_users(self, _request: ListActiveUsersRequest):
        raise ProviderQuotaExhausted(retry_after_seconds=30)

    async def fetch_resource_page(self, _request: FetchResourcePageRequest):
        raise ProviderQuotaExhausted(retry_after_seconds=30)


def test_attributes_drop_identifiers_and_collapse_oversized_values() -> None:
    attributes = bounded_metric_attributes(
        {
            "provider": "p" * 100,
            "quota_class": "reads",
            "work_class": WorkClass.BACKFILL,
            "store_key": "customer@example.com",
            "request_id": "request-secret",
        }
    )

    assert attributes == {
        "provider": "other",
        "quota_class": "reads",
        "work_class": "backfill",
    }


def test_safe_emitter_records_counter_gauge_and_histogram() -> None:
    meter = _RecordingMeter()
    metrics = SafeMetricEmitter(meter, {"operation": "quota"})  # type: ignore[arg-type]

    metrics.increment("counter", 2, attributes={"status": "accepted"})
    metrics.gauge("gauge", 3)
    metrics.histogram("histogram", 4, unit="ms")

    assert meter.events == [
        ("counter", "counter", 2, {"operation": "quota", "status": "accepted"}),
        ("gauge", "gauge", 3, {"operation": "quota"}),
        ("histogram", "histogram", 4, {"operation": "quota"}),
    ]


def test_metric_failures_and_missing_activity_context_are_no_ops() -> None:
    metrics = SafeMetricEmitter(_ExplodingMeter())  # type: ignore[arg-type]
    metrics.increment("counter")
    metrics.gauge("gauge", 1)
    metrics.histogram("histogram", 1)

    # Obtaining an SDK Activity meter outside an Activity raises internally;
    # the helper must replace it with Temporal's no-op meter.
    activity_metrics(operation="unit_test").increment("outside_context")


@pytest.mark.asyncio
async def test_provider_quota_exhaustion_emits_bounded_429_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    meter = _RecordingMeter()

    def fake_activity_metrics(**attributes: object) -> SafeMetricEmitter:
        return SafeMetricEmitter(meter, attributes)  # type: ignore[arg-type]

    monkeypatch.setattr(provider_api, "activity_metrics", fake_activity_metrics)
    scope = QuotaScope(
        provider="fake-provider",
        credential_key="credential-must-not-be-a-label",
        quota_class="reads",
    )
    result = await ProviderActivities(_QuotaGateway()).list_active_users(
        ListActiveUsersRequest(
            store_key="store-must-not-be-a-label",
            lifecycle_generation=1,
            cursor=None,
            page_size=10,
            request_id="request-must-not-be-a-label",
            quota_scope=scope,
        )
    )

    assert result.quota_exhausted is True
    names = [event[1] for event in meter.events]
    assert names == [PROVIDER_REQUESTS, PROVIDER_QUOTA_EXHAUSTED]
    for _, _, _, attributes in meter.events:
        assert attributes["provider"] == "fake-provider"
        assert attributes["quota_class"] == "reads"
        assert "credential_key" not in attributes
        assert "store_key" not in attributes
        assert "request_id" not in attributes


@pytest.mark.asyncio
async def test_unscoped_provider_quota_exhaustion_is_non_retryable() -> None:
    with pytest.raises(ApplicationError) as raised:
        await ProviderActivities(_QuotaGateway()).list_active_users(
            ListActiveUsersRequest(
                store_key="store",
                lifecycle_generation=1,
                cursor=None,
                page_size=10,
                request_id="request",
                quota_scope=None,
            )
        )

    assert raised.value.type == "ProviderQuotaExhausted"
    assert raised.value.non_retryable is True
