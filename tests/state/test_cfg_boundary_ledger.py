from __future__ import annotations

import sqlite3

import pytest

from agent.memory_manager import MemoryManager
from agent.memory_provider import MemoryProvider
from hermes_state import SessionDB


@pytest.fixture()
def db(tmp_path):
    session_db = SessionDB(db_path=tmp_path / "state.db")
    session_db.create_session("source", source="test")
    try:
        yield session_db
    finally:
        session_db.close()


def boundary(boundary_id="boundary-1", seq=1, target="source"):
    return {
        "compression_boundary_id": boundary_id,
        "source_session_id": "source",
        "target_session_id": target,
        "mode": "IN_PLACE" if target == "source" else "ROTATION",
        "committed_transcript_reference": {
            "kind": "session_db_boundary",
            "source_session_id": "source",
            "target_session_id": target,
            "source_message_watermark": 1,
            "target_active_message_ids": [1],
        },
        "boundary_seq": seq,
        "snapshot_id": "snapshot-1",
        "snapshot_sha256": "a" * 64,
        "protected_block_sha256": "b" * 64,
        "snapshot_json": {
            "schema_version": "ps-v1", "facts": [], "supersessions": []
        },
        "protected_block_json": {"schema_version": "pb-v1", "facts": []},
        "committed_transcript_reference": {
            "kind": "session_db_boundary", "source_session_id": "source",
            "target_session_id": target, "source_message_watermark": 1,
            "target_active_message_ids": [],
        },
        "guard_version": "compression-fidelity-guard-v0.2",
        "created_at": 10.0,
        "committed_at": 11.0,
    }


def test_allocate_boundary_seq_never_reuses_uncommitted_allocation(db):
    assert db.allocate_compression_boundary_seq("source") == 1
    assert db.allocate_compression_boundary_seq("source") == 2


def test_archive_and_compact_commits_boundary_and_ledgers_atomically(db):
    db.append_message("source", "user", "before")
    db.archive_and_compact(
        "source",
        [{"role": "assistant", "content": "summary"}],
        boundary=boundary(),
        provider_ids=["byterover", "byterover", "other"],
    )

    assert db.get_compression_boundary("boundary-1")["boundary_seq"] == 1
    ledger = db.get_provider_finalization("boundary-1", "byterover")
    assert {k: ledger[k] for k in ledger if k != "updated_at"} == {
        "compression_boundary_id": "boundary-1",
        "provider_id": "byterover",
        "status": "NOT_STARTED",
        "attempt_count": 0,
        "last_error_class": None,
    }
    assert float(ledger["updated_at"]) == 11.0
    with db._read_ctx() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM compression_boundary_provider_finalizations "
            "WHERE compression_boundary_id = ?", ("boundary-1",)
        ).fetchone()[0] == 2


def test_boundary_insert_failure_rolls_back_compaction(db):
    db.append_message("source", "user", "before")
    db.archive_and_compact(
        "source", [{"role": "assistant", "content": "first"}], boundary=boundary()
    )
    before = [row["content"] for row in db.get_messages("source")]

    with pytest.raises(sqlite3.IntegrityError):
        db.archive_and_compact(
            "source",
            [{"role": "assistant", "content": "must rollback"}],
            boundary=boundary("boundary-2", seq=1),
        )

    assert [row["content"] for row in db.get_messages("source")] == before
    assert db.get_compression_boundary("boundary-2") is None


def test_provider_ledger_state_machine_and_complete_terminal_noop(db):
    db.archive_and_compact("source", [], boundary=boundary(), provider_ids=["byterover"])

    pending = db.claim_provider_finalization_attempt("boundary-1", "byterover")
    assert (pending["status"], pending["attempt_count"]) == ("PENDING", 1)
    complete = db.complete_provider_finalization("boundary-1", "byterover")
    assert complete["status"] == "COMPLETE"
    replay = db.claim_provider_finalization_attempt("boundary-1", "byterover")
    assert replay == complete

    with pytest.raises(RuntimeError, match="illegal provider finalization transition"):
        db.fail_provider_finalization("boundary-1", "byterover", "TimeoutError")


def test_failed_provider_can_be_reclaimed_with_incremented_attempt(db):
    db.archive_and_compact("source", [], boundary=boundary(), provider_ids=["byterover"])
    db.claim_provider_finalization_attempt("boundary-1", "byterover")
    failed = db.fail_provider_finalization("boundary-1", "byterover", "TimeoutError")
    assert failed["status"] == "FAILED"
    assert failed["last_error_class"] == "TimeoutError"

    pending = db.claim_provider_finalization_attempt("boundary-1", "byterover")
    assert (pending["status"], pending["attempt_count"], pending["last_error_class"]) == (
        "PENDING", 2, None
    )


def test_pending_and_missing_transitions_fail_closed(db):
    db.archive_and_compact("source", [], boundary=boundary(), provider_ids=["byterover"])
    db.claim_provider_finalization_attempt("boundary-1", "byterover")

    with pytest.raises(RuntimeError, match="illegal provider finalization transition"):
        db.claim_provider_finalization_attempt("boundary-1", "byterover")
    with pytest.raises(RuntimeError, match="provider finalization row not found"):
        db.complete_provider_finalization("boundary-1", "missing")


def test_fail_rejects_unbounded_error_payload(db):
    db.archive_and_compact("source", [], boundary=boundary(), provider_ids=["byterover"])
    db.claim_provider_finalization_attempt("boundary-1", "byterover")

    with pytest.raises(ValueError, match="bounded error class"):
        db.fail_provider_finalization("boundary-1", "byterover", "x" * 256)
    assert db.get_provider_finalization("boundary-1", "byterover")["status"] == "PENDING"


class _SqliteDedupeProvider(MemoryProvider):
    name = "durable-sqlite"
    boundary_finalization_idempotency = "durable_dedupe"

    def __init__(self, path):
        self.path = path
        self.dispatches = 0
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS effects (boundary_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
            )

    def is_available(self):
        return True

    def initialize(self, session_id, **kwargs):
        pass

    def get_tool_schemas(self):
        return []

    def finalize_memory_for_boundary(self, context, *, idempotency_key):
        self.dispatches += 1
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO effects(boundary_id, payload) VALUES (?, ?)",
                (idempotency_key, context["snapshot_sha256"]),
            )

    def has_finalized_boundary(self, *, idempotency_key):
        with sqlite3.connect(self.path) as conn:
            return conn.execute(
                "SELECT 1 FROM effects WHERE boundary_id = ?", (idempotency_key,)
            ).fetchone() is not None

    def effect_count(self):
        with sqlite3.connect(self.path) as conn:
            return conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]


def test_boundary_payload_survives_restart_and_resolves_chain(tmp_path):
    state_path = tmp_path / "state.db"
    first = SessionDB(db_path=state_path)
    first.create_session("source", source="test")
    first.archive_and_compact(
        "source", [{"role": "assistant", "content": "summary"}], boundary=boundary()
    )
    first.close()

    restarted = SessionDB(db_path=state_path)
    restored = restarted.get_compression_boundary("boundary-1")
    latest = restarted.get_latest_compression_boundary("source")

    assert restored["snapshot"] == boundary()["snapshot_json"]
    assert restored["protected_block"] == boundary()["protected_block_json"]
    assert latest["compression_boundary_id"] == "boundary-1"
    assert latest["snapshot_id"] == "snapshot-1"
    restarted.close()


def test_pending_crash_window_reconciles_durable_effect_after_restart(tmp_path):
    state_path = tmp_path / "state.db"
    effect_path = tmp_path / "provider-effects.db"
    first_db = SessionDB(db_path=state_path)
    first_db.create_session("source", source="test")
    first_db.archive_and_compact(
        "source", [], boundary=boundary(), provider_ids=["durable-sqlite"]
    )
    first_db.claim_provider_finalization_attempt("boundary-1", "durable-sqlite")
    first_provider = _SqliteDedupeProvider(effect_path)
    context = {
        **boundary(),
        "old_session_identity": "source",
    }
    first_provider.finalize_memory_for_boundary(context, idempotency_key="boundary-1")
    assert first_provider.effect_count() == 1
    first_db.close()  # crash window: effect durable, ledger remains PENDING

    restarted_db = SessionDB(db_path=state_path)
    restarted_provider = _SqliteDedupeProvider(effect_path)
    manager = MemoryManager(session_db=restarted_db)
    manager.add_provider(restarted_provider)
    result = manager.finalize_memory_for_boundary(context)

    assert result["durable-sqlite"]["status"] == "COMPLETE"
    assert result["durable-sqlite"]["disposition"] == "replay_skipped"
    assert restarted_provider.dispatches == 0
    assert restarted_provider.effect_count() == 1
    assert restarted_db.get_provider_finalization(
        "boundary-1", "durable-sqlite"
    )["status"] == "COMPLETE"
    restarted_db.close()
