from __future__ import annotations

from retrieval.demo.headless import run_headless_story


async def test_headless_story_proves_quota_fence_stale_writer_and_cleanup() -> None:
    result = await run_headless_story()

    assert result.quota_retry_after_seconds == 5
    assert result.committed_before_fence == 4
    assert result.stale_document_key == "late-security-review.md"
    assert result.citation_document_keys == (
        "northstar-qbr.md",
        "renewal-plan.md",
        "stakeholders.md",
        "support-escalation.md",
    )
    assert (result.final_state, result.final_generation) == ("inactive", 8)
    assert (result.final_document_count, result.final_chunk_count) == (0, 0)
    for required in (
        "quota_wait_started",
        "quota_wait_completed",
        "document_commit_held",
        "deactivation_fenced",
        "held_commit_released",
        "stale_generation_rejected",
        "cleanup_batch_completed",
        "store_inactive",
    ):
        assert required in result.event_types
