"""Versioned, hash-verified Northstar fixture manifest and staging adapter."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any
from urllib.parse import unquote, urlsplit

from retrieval.temporal.models.documents import DocumentRef

from .config import DemoConfig


class InvalidScenarioError(ValueError):
    """The packaged scenario is malformed or its fixture bytes have changed."""


class InvalidFixtureUriError(ValueError):
    """A staging URI is outside the allowlisted packaged scenario."""


@dataclass(frozen=True)
class NorthstarDocument:
    document_key: str
    source_version: str
    fixture_uri: str
    content_hash: str

    def reference(self) -> DocumentRef:
        return DocumentRef(
            document_key=self.document_key,
            source_version=self.source_version,
            staging_uri=self.fixture_uri,
            content_hash=self.content_hash,
        )


@dataclass(frozen=True)
class NorthstarScenario:
    schema_version: int
    scenario_id: str
    display_name: str
    baseline_generation: int
    user_key: str
    resource_key: str
    quota_operation: str
    quota_retry_after_seconds: float
    held_document_key: str
    documents: tuple[NorthstarDocument, ...]
    answer_document_keys: tuple[str, ...]

    @property
    def references(self) -> tuple[DocumentRef, ...]:
        return tuple(document.reference() for document in self.documents)

    @property
    def by_key(self) -> Mapping[str, NorthstarDocument]:
        return MappingProxyType({document.document_key: document for document in self.documents})

    @property
    def by_uri(self) -> Mapping[str, NorthstarDocument]:
        return MappingProxyType({document.fixture_uri: document for document in self.documents})


def _fixture_root():
    return resources.files("retrieval.demo").joinpath("fixtures", "northstar")


def load_northstar_scenario(*, validate_hashes: bool = True) -> NorthstarScenario:
    manifest_resource = _fixture_root().joinpath("scenario.json")
    try:
        payload = json.loads(manifest_resource.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvalidScenarioError("Northstar scenario manifest cannot be loaded") from exc
    if not isinstance(payload, dict):
        raise InvalidScenarioError("Northstar scenario manifest must be a JSON object")
    scenario = _parse_scenario(payload)
    _validate_scenario(scenario, validate_hashes=validate_hashes)
    return scenario


def _parse_scenario(payload: Mapping[str, Any]) -> NorthstarScenario:
    required = {
        "schema_version",
        "scenario_id",
        "display_name",
        "baseline_generation",
        "user_key",
        "resource_key",
        "quota_operation",
        "quota_retry_after_seconds",
        "held_document_key",
        "documents",
        "answer_document_keys",
    }
    missing = sorted(required.difference(payload))
    if missing:
        raise InvalidScenarioError(f"scenario manifest is missing: {', '.join(missing)}")
    raw_documents = payload["documents"]
    if not isinstance(raw_documents, list):
        raise InvalidScenarioError("scenario documents must be a list")
    try:
        documents = tuple(
            NorthstarDocument(
                document_key=str(item["document_key"]),
                source_version=str(item["source_version"]),
                fixture_uri=str(item["fixture_uri"]),
                content_hash=str(item["content_hash"]),
            )
            for item in raw_documents
        )
        return NorthstarScenario(
            schema_version=int(payload["schema_version"]),
            scenario_id=str(payload["scenario_id"]),
            display_name=str(payload["display_name"]),
            baseline_generation=int(payload["baseline_generation"]),
            user_key=str(payload["user_key"]),
            resource_key=str(payload["resource_key"]),
            quota_operation=str(payload["quota_operation"]),
            quota_retry_after_seconds=float(payload["quota_retry_after_seconds"]),
            held_document_key=str(payload["held_document_key"]),
            documents=documents,
            answer_document_keys=tuple(str(value) for value in payload["answer_document_keys"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidScenarioError("scenario manifest contains invalid field types") from exc


def _validate_scenario(scenario: NorthstarScenario, *, validate_hashes: bool) -> None:
    if scenario.schema_version != 1 or scenario.scenario_id != "northstar-v1":
        raise InvalidScenarioError("unsupported Northstar scenario version")
    if scenario.display_name != "Northstar AI":
        raise InvalidScenarioError("Northstar display name must remain stable")
    if scenario.baseline_generation != 7:
        raise InvalidScenarioError("Northstar baseline generation must be 7")
    if scenario.quota_retry_after_seconds != 5.0:
        raise InvalidScenarioError("Northstar quota retry window must be five seconds")
    if len(scenario.documents) != 5:
        raise InvalidScenarioError("Northstar scenario must contain exactly five documents")
    keys = tuple(document.document_key for document in scenario.documents)
    uris = tuple(document.fixture_uri for document in scenario.documents)
    if len(set(keys)) != len(keys) or len(set(uris)) != len(uris):
        raise InvalidScenarioError("document keys and fixture URIs must be unique")
    if scenario.held_document_key not in keys:
        raise InvalidScenarioError("held_document_key is not listed in documents")
    if not set(scenario.answer_document_keys).issubset(keys):
        raise InvalidScenarioError("answer_document_keys contains an unknown document")
    for document in scenario.documents:
        path = _path_for_uri(document.fixture_uri)
        if path.name != document.document_key:
            raise InvalidScenarioError("fixture URI filename must equal document_key")
        if len(document.content_hash) != 64 or any(
            character not in "0123456789abcdef" for character in document.content_hash
        ):
            raise InvalidScenarioError("fixture content hashes must be lowercase SHA-256")
        if validate_hashes:
            try:
                body = _fixture_root().joinpath(path.name).read_bytes()
            except OSError as exc:
                raise InvalidScenarioError(f"fixture {path.name!r} cannot be loaded") from exc
            actual = hashlib.sha256(body).hexdigest()
            if actual != document.content_hash:
                raise InvalidScenarioError(
                    f"fixture {path.name!r} does not match its manifest hash"
                )


def _path_for_uri(uri: str) -> PurePosixPath:
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "fixture"
        or parsed.netloc != "northstar"
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise InvalidFixtureUriError("only fixture://northstar/<manifest-file> is supported")
    if unquote(parsed.path) != parsed.path:
        raise InvalidFixtureUriError("percent-encoded fixture paths are not allowed")
    path = PurePosixPath(parsed.path)
    if not path.is_absolute() or len(path.parts) != 2 or path.name in {"", ".", ".."}:
        raise InvalidFixtureUriError("fixture URI must identify one manifest file")
    return path


class FixtureStagingStore:
    """Read-only adapter for exact manifest-listed ``fixture://`` URIs."""

    def __init__(self, scenario: NorthstarScenario) -> None:
        self._scenario = scenario
        self._allowlist = dict(scenario.by_uri)

    async def get(self, staging_uri: str) -> bytes:
        path = _path_for_uri(staging_uri)
        document = self._allowlist.get(staging_uri)
        if document is None:
            raise InvalidFixtureUriError("fixture URI is not listed in the active scenario")
        body = _fixture_root().joinpath(path.name).read_bytes()
        if hashlib.sha256(body).hexdigest() != document.content_hash:
            raise InvalidScenarioError("packaged fixture no longer matches its manifest hash")
        return body


def create_staging_store() -> FixtureStagingStore:
    config = DemoConfig.from_env()
    config.require_enabled()
    scenario = load_northstar_scenario()
    if config.scenario_id != scenario.scenario_id:
        raise InvalidScenarioError(f"unsupported configured scenario {config.scenario_id!r}")
    return FixtureStagingStore(scenario)


__all__ = [
    "FixtureStagingStore",
    "InvalidFixtureUriError",
    "InvalidScenarioError",
    "NorthstarDocument",
    "NorthstarScenario",
    "create_staging_store",
    "load_northstar_scenario",
]
