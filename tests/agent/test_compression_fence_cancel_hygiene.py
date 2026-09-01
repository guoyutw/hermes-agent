"""Hygiene fence cancellation must unwind a streaming summary promptly — #97064 parity.

A HygieneTurnHoldExceeded abandons the hygiene turn after 10s but leaves the
summary LLM streaming. The commit fence revokes admission; the active provider
call must observe that fence via a single Event-shaped SummaryCancelSource
(hard interrupt OR fence cancelled) and raise AuxiliaryExplicitCancellation
so compress_context unwinds, releases the compression lock, and emits no
COMPACTION_BOUNDARY_COMMITTED. The subsequent hard-limit Preflight can then
acquire compression normally.
"""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from agent.auxiliary_client import AuxiliaryExplicitCancellation, aux_interrupt_protection
from agent.conversation_compression import CompressionCommitFence


def test_fence_cancel_unwinds_protected_provider_within_bound():
    """Protected provider with fence-combined cancel unwinds within 0.5s."""
    from agent.auxiliary_client import _run_protected_sync_provider_call

    fence = CompressionCommitFence()

    class _SummaryCancelSource:
        def is_set(self) -> bool:
            return bool(fence.is_cancelled)

    source = _SummaryCancelSource()
    started = threading.Event()
    outcome: dict = {}

    def blocking_callback(_kwargs):
        started.set()
        time.sleep(5)
        return "should-not-return"

    def run():
        try:
            with aux_interrupt_protection(cancel_event=source):
                res = _run_protected_sync_provider_call(blocking_callback, {})
                outcome["result"] = res
        except AuxiliaryExplicitCancellation as exc:
            outcome["exception"] = exc
        except Exception as exc:
            outcome["exception"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    assert started.wait(timeout=2), "worker did not start"
    time.sleep(0.05)
    assert fence.try_cancel_before_commit() is True
    assert fence.is_cancelled is True
    t.join(timeout=1.0)
    assert not t.is_alive(), "fence-cancelled worker did not unwind promptly"
    assert isinstance(outcome.get("exception"), AuxiliaryExplicitCancellation)


def test_compression_lock_released_after_fence_cancel():
    """Holder-qualified durable lock must be releasable after fence cancel."""
    fence = CompressionCommitFence()
    lock_holder: dict = {}

    class FakeDB:
        def acquire_compression_lock(self, sid):
            if sid in lock_holder:
                return False
            lock_holder[sid] = "holder-1"
            fence.register_cancelled_lock_release(lambda: lock_holder.pop(sid, None))
            return True

    db = FakeDB()
    sid = "test-fence-cancel-lock"
    assert db.acquire_compression_lock(sid) is True
    assert sid in lock_holder
    # Hygiene handler revokes fence and releases.
    assert fence.try_cancel_before_commit() is True
    fence.release_cancelled_compression_lock()
    assert sid not in lock_holder, "lock must be released after fence cancel"
    # Subsequent attempt can acquire.
    assert db.acquire_compression_lock(sid) is True


def test_fallback_providers_alias_still_resolves():
    """Owner-approved fallback_providers must still resolve as fallback_chain (moved from Temp)."""
    from unittest.mock import patch

    from agent.conversation_compression import resolve_compression_fallback_route

    with patch(
        "agent.auxiliary_client._get_auxiliary_task_config",
        return_value={"fallback_providers": [{"provider": "openai-codex", "model": "gpt-5.6-luna", "timeout": 120}]},
    ):
        route = resolve_compression_fallback_route()
        assert route is not None and route["provider"] == "openai-codex"

    with patch(
        "agent.auxiliary_client._get_auxiliary_task_config",
        return_value={
            "fallback_chain": [{"provider": "custom", "model": "a"}],
            "fallback_providers": [{"provider": "other", "model": "b"}],
        },
    ):
        route = resolve_compression_fallback_route()
        assert route["provider"] == "custom"

    with patch("agent.auxiliary_client._get_auxiliary_task_config", return_value={}):
        assert resolve_compression_fallback_route() is None
