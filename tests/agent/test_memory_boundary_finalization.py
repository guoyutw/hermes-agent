import logging

from agent.memory_manager import MemoryManager
from agent.memory_provider import MemoryProvider


class _Ledger:
    def __init__(self, statuses=None):
        self.rows = dict(statuses or {})
        self.calls = []

    def get_compression_boundary(self, boundary_id):
        return {"compression_boundary_id": boundary_id}

    def get_provider_finalization(self, boundary_id, provider_id):
        row = self.rows.get(provider_id)
        return dict(row) if row else None

    def claim_provider_finalization_attempt(self, boundary_id, provider_id):
        row = self.rows[provider_id]
        if row["status"] == "COMPLETE":
            return dict(row)
        if row["status"] not in {"NOT_STARTED", "FAILED"}:
            raise RuntimeError("illegal claim")
        row.update(status="PENDING", attempt_count=row["attempt_count"] + 1, last_error_class=None)
        self.calls.append(("claim", provider_id))
        return dict(row)

    def complete_provider_finalization(self, boundary_id, provider_id):
        self.rows[provider_id]["status"] = "COMPLETE"
        self.calls.append(("complete", provider_id))
        return dict(self.rows[provider_id])

    def fail_provider_finalization(self, boundary_id, provider_id, error_class):
        self.rows[provider_id].update(status="FAILED", last_error_class=error_class)
        self.calls.append(("fail", provider_id, error_class))
        return dict(self.rows[provider_id])


class _Provider(MemoryProvider):
    name = "durable"
    boundary_finalization_idempotency = "native"

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def is_available(self):
        return True

    def initialize(self, session_id, **kwargs):
        pass

    def get_tool_schemas(self):
        return []

    def finalize_memory_for_boundary(self, context, *, idempotency_key):
        self.calls.append((dict(context), idempotency_key))
        if self.error:
            raise self.error

    def has_finalized_boundary(self, *, idempotency_key):
        return False


def _context():
    return {
        "compression_boundary_id": "boundary-7",
        "source_session_id": "source",
        "target_session_id": "target",
        "mode": "ROTATION",
        "old_session_identity": "source",
        "committed_transcript_reference": {
            "kind": "session_db_boundary",
            "source_session_id": "source",
            "target_session_id": "target",
            "source_message_watermark": 41,
            "target_active_message_ids": [101, 102],
        },
        "snapshot_id": "snapshot-7",
        "snapshot_sha256": "a" * 64,
        "protected_block_sha256": "b" * 64,
        "guard_version": "cfg-s1b-v1",
    }


def _manager(provider):
    manager = MemoryManager()
    manager.add_provider(provider)
    return manager


def test_missing_committed_transcript_reference_is_rejected_before_dispatch():
    provider = _Provider()
    ledger = _Ledger({"durable": {"status": "NOT_STARTED", "attempt_count": 0, "last_error_class": None}})
    context = _context()
    del context["committed_transcript_reference"]

    try:
        _manager(provider).finalize_memory_for_boundary(context, session_db=ledger)
    except ValueError as exc:
        assert "committed_transcript_reference" in str(exc)
    else:
        raise AssertionError("missing committed transcript reference was accepted")

    assert provider.calls == []
    assert ledger.calls == []


def test_claims_dispatches_with_boundary_id_and_completes():
    provider = _Provider()
    ledger = _Ledger({"durable": {"status": "NOT_STARTED", "attempt_count": 0, "last_error_class": None}})

    result = _manager(provider).finalize_memory_for_boundary(_context(), session_db=ledger)

    assert result["durable"]["status"] == "COMPLETE"
    assert provider.calls == [(_context(), "boundary-7")]
    assert ledger.calls == [("claim", "durable"), ("complete", "durable")]


def test_complete_is_terminal_and_not_redispatched():
    provider = _Provider()
    ledger = _Ledger({"durable": {"status": "COMPLETE", "attempt_count": 1, "last_error_class": None}})

    result = _manager(provider).finalize_memory_for_boundary(_context(), session_db=ledger)

    assert result["durable"]["status"] == "COMPLETE"
    assert provider.calls == []
    assert ledger.calls == []


def test_provider_failure_is_recorded_and_does_not_escape():
    provider = _Provider(RuntimeError("secret provider detail"))
    ledger = _Ledger({"durable": {"status": "NOT_STARTED", "attempt_count": 0, "last_error_class": None}})

    result = _manager(provider).finalize_memory_for_boundary(_context(), session_db=ledger)

    assert result["durable"]["status"] == "FAILED"
    assert ledger.rows["durable"]["last_error_class"] == "RuntimeError"


def test_failed_attempt_retries_but_stops_at_bound():
    provider = _Provider()
    ledger = _Ledger({"durable": {"status": "FAILED", "attempt_count": 2, "last_error_class": "TimeoutError"}})
    result = _manager(provider).finalize_memory_for_boundary(_context(), session_db=ledger, max_attempts=3)
    assert result["durable"]["status"] == "COMPLETE"
    assert ledger.rows["durable"]["attempt_count"] == 3

    provider2 = _Provider()
    ledger2 = _Ledger({"durable": {"status": "FAILED", "attempt_count": 3, "last_error_class": "TimeoutError"}})
    result2 = _manager(provider2).finalize_memory_for_boundary(_context(), session_db=ledger2, max_attempts=3)
    assert result2["durable"]["status"] == "FAILED"
    assert result2["durable"]["disposition"] == "retry_exhausted"
    assert provider2.calls == []


def test_provider_without_idempotency_or_durable_dedupe_is_blocked():
    provider = _Provider()
    provider.boundary_finalization_idempotency = "none"
    ledger = _Ledger({"durable": {"status": "NOT_STARTED", "attempt_count": 0, "last_error_class": None}})

    result = _manager(provider).finalize_memory_for_boundary(_context(), session_db=ledger)

    assert result["durable"]["status"] == "FAILED"
    assert ledger.rows["durable"]["last_error_class"] == "IdempotencyUnavailable"
    assert provider.calls == []


def test_normative_events_cover_pending_complete_failed_and_replay_skip(caplog):
    caplog.set_level(logging.INFO, logger="agent.memory_manager")
    ok = _Provider()
    ok_ledger = _Ledger({"durable": {"status": "NOT_STARTED", "attempt_count": 0, "last_error_class": None}})
    _manager(ok).finalize_memory_for_boundary(_context(), session_db=ok_ledger)

    failed = _Provider(RuntimeError("detail must not leak"))
    failed_ledger = _Ledger({"durable": {"status": "NOT_STARTED", "attempt_count": 0, "last_error_class": None}})
    _manager(failed).finalize_memory_for_boundary(_context(), session_db=failed_ledger)

    replay = _Provider()
    replay_ledger = _Ledger({"durable": {"status": "COMPLETE", "attempt_count": 1, "last_error_class": None}})
    _manager(replay).finalize_memory_for_boundary(_context(), session_db=replay_ledger)

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("MEMORY_FINALIZATION_PENDING ") for message in messages)
    assert any(message.startswith("MEMORY_FINALIZATION_COMPLETE ") for message in messages)
    assert any(message.startswith("MEMORY_FINALIZATION_FAILED ") for message in messages)
    assert any(message.startswith("MEMORY_FINALIZATION_REPLAY_SKIPPED ") for message in messages)
    assert all("detail must not leak" not in message for message in messages)
