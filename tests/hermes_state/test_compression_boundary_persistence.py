"""Tests for the pure durable protected-boundary SessionDB primitive."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from agent.protected_state import ContractValidationError, ProtectedBlock
from hermes_state import SessionDB
from hermes_state_boundary import (
    BOUNDARY_CONTRACT_VERSION,
    boundary_identity,
)


def _block(value: str = "complete") -> ProtectedBlock:
    return ProtectedBlock.from_dict(
        {
            "schema_version": "protected-block-v1",
            "facts": [
                {
                    "schema_version": "protected-fact-v1",
                    "fact_kind": "task_state",
                    "capture_status": "CAPTURED",
                    "value": value,
                    "provenance": {"session_id": "source", "message_id": "message-1"},
                    "source_identity": {"source_type": "test", "source_id": "run-1"},
                }
            ],
            "supersessions": [],
        }
    )


def _db(tmp_path: Path, *, target: bool = True) -> SessionDB:
    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("source", source="test")
    if target:
        db.create_session("target", source="test")
    return db


def _row_count(db: SessionDB) -> int:
    with db._read_ctx() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM compression_boundaries").fetchone()[0])


def test_fresh_db_creates_boundary_table_and_schema_version(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        with db._read_ctx() as conn:
            table = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'compression_boundaries'"
            ).fetchone()
            version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        assert table is not None
        assert "ON DELETE CASCADE" in table[0]
        assert version == 26
    finally:
        db.close()


def test_existing_schema_upgrade_preserves_sessions_and_messages(tmp_path: Path) -> None:
    db = _db(tmp_path)
    db.append_message("source", "user", "keep me")
    db._execute_write(
        lambda conn: (
            conn.execute("UPDATE schema_version SET version = 25"),
            conn.execute("DROP TABLE compression_boundaries"),
        )
    )
    db.close()

    reopened = SessionDB(db_path=tmp_path / "state.db")
    try:
        assert [row["content"] for row in reopened.get_messages("source")] == ["keep me"]
        with reopened._read_ctx() as conn:
            assert conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'compression_boundaries'"
            ).fetchone()
            assert conn.execute("SELECT version FROM schema_version").fetchone()[0] == 26
    finally:
        reopened.close()


def test_mode_and_source_target_constraints_are_validated(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        with pytest.raises(ContractValidationError, match="mode"):
            db.persist_protected_boundary(
                source_session_id="source",
                target_session_id="source",
                mode="INVALID",
                protected_block=_block(),
            )
        with pytest.raises(ContractValidationError, match="IN_PLACE"):
            db.persist_protected_boundary(
                source_session_id="source",
                target_session_id="target",
                mode="IN_PLACE",
                protected_block=_block(),
            )
        with pytest.raises(ContractValidationError, match="ROTATION"):
            db.persist_protected_boundary(
                source_session_id="source",
                target_session_id="source",
                mode="ROTATION",
                protected_block=_block(),
            )
    finally:
        db.close()


def test_source_scoped_sequence_is_monotonic_and_duplicate_is_db_protected(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        first = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        second = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block("next")
        )
        assert first["boundary_seq"] == 1
        assert second["boundary_seq"] == 2
        with db._read_ctx() as conn:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO compression_boundaries ("
                    "compression_boundary_id, source_session_id, target_session_id, mode, boundary_seq, "
                    "contract_version, protected_block_json, protected_block_sha256, created_at, committed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "cb1_" + "f" * 64,
                        "source",
                        "source",
                        "IN_PLACE",
                        1,
                        BOUNDARY_CONTRACT_VERSION,
                        first["canonical_json"],
                        first["protected_block_sha256"],
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                    ),
                )
    finally:
        db.close()


def test_sqlite_constraints_reject_invalid_boundary_rows(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        columns = (
            "compression_boundary_id, source_session_id, target_session_id, mode, boundary_seq, "
            "contract_version, protected_block_json, protected_block_sha256, created_at, committed_at"
        )
        values = (
            "cb1_" + "a" * 64,
            "source",
            "source",
            "IN_PLACE",
            1,
            BOUNDARY_CONTRACT_VERSION,
            record["canonical_json"],
            record["protected_block_sha256"],
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        )
        for bad_values in (
            (*values[:4], 0, *values[5:]),
            (*values[:3], "INVALID", *values[4:]),
            (*values[:2], "target", "IN_PLACE", *values[4:]),
            (*values[:1], "missing", "missing", "IN_PLACE", *values[4:]),
        ):
            with db._read_ctx() as conn:
                with pytest.raises(sqlite3.IntegrityError):
                    conn.execute(
                        f"INSERT INTO compression_boundaries ({columns}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        bad_values,
                    )
    finally:
        db.close()


def test_durable_save_close_reopen_and_canonical_roundtrip(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="target", mode="ROTATION", protected_block=_block()
        )
        boundary_id = record["compression_boundary_id"]
        assert boundary_id == boundary_identity(
            source_session_id="source",
            target_session_id="target",
            mode="ROTATION",
            boundary_seq=1,
            contract_version=BOUNDARY_CONTRACT_VERSION,
            protected_block_sha256=record["protected_block_sha256"],
        )
    finally:
        db.close()

    reopened = SessionDB(db_path=tmp_path / "state.db")
    try:
        readback = reopened.read_protected_boundary(boundary_id)
        assert readback is not None
        assert readback["canonical_json"] == record["canonical_json"]
        assert readback["protected_block"] == record["protected_block"]
        assert readback["protected_block_sha256"] == record["protected_block_sha256"]
        assert readback["recomputed_sha256"] == record["protected_block_sha256"]
    finally:
        reopened.close()


def test_fk_cascade_removes_boundary_with_source_session(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="target", mode="ROTATION", protected_block=_block()
        )
        db._execute_write(lambda conn: conn.execute("DELETE FROM sessions WHERE id = ?", ("source",)))
        assert db.read_protected_boundary(record["compression_boundary_id"]) is None
        assert _row_count(db) == 0
    finally:
        db.close()


def test_payload_tamper_is_rejected(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE compression_boundaries SET protected_block_json = ? WHERE compression_boundary_id = ?",
                (json.dumps({"schema_version": "protected-block-v1", "facts": [], "supersessions": []}), record["compression_boundary_id"]),
            )
        )
        with pytest.raises(ContractValidationError, match="canonical|hash"):
            db.read_protected_boundary(record["compression_boundary_id"])
    finally:
        db.close()


def test_stored_hash_tamper_is_rejected(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE compression_boundaries SET protected_block_sha256 = ? WHERE compression_boundary_id = ?",
                ("0" * 64, record["compression_boundary_id"]),
            )
        )
        with pytest.raises(ContractValidationError, match="hash|identity"):
            db.read_protected_boundary(record["compression_boundary_id"])
    finally:
        db.close()


def test_identity_linkage_tamper_is_rejected(tmp_path: Path) -> None:
    db = _db(tmp_path)
    db.create_session("other", source="test")
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="target", mode="ROTATION", protected_block=_block()
        )
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE compression_boundaries SET target_session_id = ? WHERE compression_boundary_id = ?",
                ("other", record["compression_boundary_id"]),
            )
        )
        with pytest.raises(ContractValidationError, match="identity"):
            db.read_protected_boundary(record["compression_boundary_id"])
    finally:
        db.close()


def test_schema_contract_unknown_duplicate_and_nonfinite_payloads_fail_closed(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        bad_payloads = [
            '{"schema_version":"protected-block-v1","facts":[],"supersessions":[],"extra":1}',
            '{"schema_version":"protected-block-v1","facts":[],"facts":[],"supersessions":[]}',
            '{"schema_version":"protected-block-v1","facts":[{"value":NaN}],"supersessions":[]}',
        ]
        for payload in bad_payloads:
            db._execute_write(
                lambda conn, payload=payload: conn.execute(
                    "UPDATE compression_boundaries SET protected_block_json = ? WHERE compression_boundary_id = ?",
                    (payload, record["compression_boundary_id"]),
                )
            )
            with pytest.raises(ContractValidationError):
                db.read_protected_boundary(record["compression_boundary_id"])
    finally:
        db.close()


def test_contract_version_mismatch_is_rejected(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        record = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE compression_boundaries SET contract_version = ? WHERE compression_boundary_id = ?",
                ("protected-boundary-v2", record["compression_boundary_id"]),
            )
        )
        with pytest.raises(ContractValidationError, match="contract"):
            db.read_protected_boundary(record["compression_boundary_id"])
    finally:
        db.close()


def test_exact_replay_is_idempotent_and_mismatched_replay_rejects(tmp_path: Path) -> None:
    db = _db(tmp_path)
    try:
        first = db.persist_protected_boundary(
            source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
        )
        replay = db.persist_protected_boundary(
            source_session_id="source",
            target_session_id="source",
            mode="IN_PLACE",
            protected_block=_block(),
            boundary_id=first["compression_boundary_id"],
        )
        assert replay == first
        with pytest.raises(ContractValidationError, match="replay|mismatch"):
            db.persist_protected_boundary(
                source_session_id="source",
                target_session_id="source",
                mode="IN_PLACE",
                protected_block=_block("different"),
                boundary_id=first["compression_boundary_id"],
            )
        assert _row_count(db) == 1
    finally:
        db.close()


def test_transaction_failure_does_not_leave_partial_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = _db(tmp_path)
    try:
        original = db._insert_protected_boundary

        def fail_after_insert(conn: sqlite3.Connection, **kwargs: object) -> dict[str, object]:
            original(conn, **kwargs)
            raise RuntimeError("simulated commit failure")

        monkeypatch.setattr(db, "_insert_protected_boundary", fail_after_insert)
        with pytest.raises(RuntimeError, match="simulated"):
            db.persist_protected_boundary(
                source_session_id="source", target_session_id="source", mode="IN_PLACE", protected_block=_block()
            )
        assert _row_count(db) == 0
    finally:
        db.close()
