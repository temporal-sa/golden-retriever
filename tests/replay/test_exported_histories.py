"""Replay every exported production history checked into the artifact directory."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from .workflow_registry import REPLAY_WORKFLOWS

HISTORY_ROOT = Path(__file__).resolve().parents[2] / "artifacts" / "histories"
EXPORTED_HISTORIES = tuple(sorted(HISTORY_ROOT.rglob("*.json")))


def _history_cases() -> tuple[pytest.ParameterSet | Path, ...]:
    if EXPORTED_HISTORIES:
        return EXPORTED_HISTORIES
    return (
        pytest.param(
            None,
            marks=pytest.mark.skip(
                reason=(
                    "no exported Temporal JSON histories found under "
                    "artifacts/histories; add representative histories to enable replay"
                )
            ),
            id="no-exported-histories",
        ),
    )


def _load_history(path: Path) -> WorkflowHistory:
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AssertionError(f"cannot read exported history {path}: {exc}") from exc

    # CLI exports do not contain a Workflow ID.  A top-level workflowId may be added
    # without upsetting the SDK parser (unknown fields are ignored); otherwise the
    # relative filename is a stable replay-only identity.
    workflow_id = payload.get("workflowId") or payload.get("workflow_id")
    if not isinstance(workflow_id, str) or not workflow_id:
        workflow_id = path.relative_to(HISTORY_ROOT).with_suffix("").as_posix()
    return WorkflowHistory.from_json(workflow_id, payload)


@pytest.mark.replay
@pytest.mark.parametrize(
    "history_path",
    _history_cases(),
    ids=lambda value: (
        value.relative_to(HISTORY_ROOT).as_posix() if isinstance(value, Path) else str(value)
    ),
)
async def test_exported_history_replays(history_path: Path | None) -> None:
    """Fail on the first nondeterministic exported history."""

    assert history_path is not None  # The no-history case is skipped by parametrization.
    history = _load_history(history_path)
    await Replayer(workflows=REPLAY_WORKFLOWS).replay_workflow(
        history,
        raise_on_replay_failure=True,
    )
