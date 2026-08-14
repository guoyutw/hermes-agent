"""Durable protected-state persistence primitives for SessionDB.

This module intentionally stops at one durable SQLite record.  It does not
select compression candidates, invoke providers, rewrite messages, rotate
sessions, or implement recovery/runtime guard behavior.
"""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from agent.protected_state import (
    ContractValidationError,
    ProtectedBlock,
    canonical_json,
    parse_canonical_json,
    sha256_hex,
)


BOUNDARY_CONTRACT_VERSION = "protected-boundary-v1"
_BOUNDARY_ID_RE = re.compile(r"^cb1_[0-9a-f]{64}$")
_MODES = {"IN_PLACE", "ROTATION"}


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractValidationError(f"{field} must be a non-empty string")
    return value


def _mode(value: Any) -> str:
    if not isinstance(value, str) or value not in _MODES:
        raise ContractValidationError(f"invalid mode: {value!r}")
    return value


def _boundary_id(value: Any) -> str:
    value = _text(value, "compression_boundary_id")
    if _BOUNDARY_ID_RE.fullmatch(value) is None:
        raise ContractValidationError("invalid compression_boundary_id")
    return value


def boundary_identity(
    *,
    source_session_id: str,
    target_session_id: str,
    mode: str,
    boundary_seq: int,
    contract_version: str,
    protected_block_sha256: str,
) -> str:
    """Return the deterministic identity for one durable boundary record."""
    if isinstance(boundary_seq, bool) or not isinstance(boundary_seq, int) or boundary_seq < 1:
        raise ContractValidationError("boundary_seq must be a positive integer")
    source = _text(source_session_id, "source_session_id")
    target = _text(target_session_id, "target_session_id")
    selected_mode = _mode(mode)
    if selected_mode == "IN_PLACE" and source != target:
        raise ContractValidationError("IN_PLACE source and target must match")
    if selected_mode == "ROTATION" and source == target:
        raise ContractValidationError("ROTATION source and target must differ")
    contract = _text(contract_version, "contract_version")
    protected_hash = _text(protected_block_sha256, "protected_block_sha256")
    if not re.fullmatch(r"[0-9a-f]{64}", protected_hash):
        raise ContractValidationError("protected_block_sha256 must be lowercase sha256")
    return "cb1_" + sha256_hex(
        {
            "source_session_id": source,
            "target_session_id": target,
            "mode": selected_mode,
            "boundary_seq": boundary_seq,
            "contract_version": contract,
            "protected_block_sha256": protected_hash,
        }
    )


class CompressionBoundaryPersistenceMixin:
    """Small SessionDB mixin for durable boundary save/readback only."""

    @staticmethod
    def _insert_protected_boundary(
        conn: sqlite3.Connection,
        *,
        compression_boundary_id: str,
        source_session_id: str,
        target_session_id: str,
        mode: str,
        boundary_seq: int,
        contract_version: str,
        protected_block_json: str,
        protected_block_sha256: str,
        created_at: str,
        committed_at: str,
    ) -> None:
        conn.execute(
            """INSERT INTO compression_boundaries (
                compression_boundary_id, source_session_id, target_session_id,
                mode, boundary_seq, contract_version, protected_block_json,
                protected_block_sha256, created_at, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                compression_boundary_id,
                source_session_id,
                target_session_id,
                mode,
                boundary_seq,
                contract_version,
                protected_block_json,
                protected_block_sha256,
                created_at,
                committed_at,
            ),
        )

    @classmethod
    def _row_to_protected_boundary(cls, row: sqlite3.Row) -> dict[str, Any]:
        raw = dict(row)
        boundary_id = _boundary_id(raw.get("compression_boundary_id"))
        source = _text(raw.get("source_session_id"), "source_session_id")
        target = _text(raw.get("target_session_id"), "target_session_id")
        selected_mode = _mode(raw.get("mode"))
        if selected_mode == "IN_PLACE" and source != target:
            raise ContractValidationError("stored IN_PLACE source and target mismatch")
        if selected_mode == "ROTATION" and source == target:
            raise ContractValidationError("stored ROTATION source and target mismatch")

        sequence = raw.get("boundary_seq")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise ContractValidationError("stored boundary_seq must be a positive integer")
        contract = _text(raw.get("contract_version"), "contract_version")
        if contract != BOUNDARY_CONTRACT_VERSION:
            raise ContractValidationError("unsupported boundary contract version")
        created_at = _text(raw.get("created_at"), "created_at")
        committed_at = _text(raw.get("committed_at"), "committed_at")
        stored_json = raw.get("protected_block_json")
        if not isinstance(stored_json, str):
            raise ContractValidationError("stored protected_block_json must be text")

        parsed = parse_canonical_json(stored_json)
        block = ProtectedBlock.from_dict(parsed)
        canonical = canonical_json(block.to_dict())
        if stored_json != canonical:
            raise ContractValidationError("protected block is not canonical JSON")
        stored_hash = _text(raw.get("protected_block_sha256"), "protected_block_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", stored_hash):
            raise ContractValidationError("stored protected_block_sha256 must be lowercase sha256")
        recomputed_hash = sha256_hex(block.to_dict())
        if stored_hash != recomputed_hash:
            raise ContractValidationError("protected block hash mismatch")

        expected_id = boundary_identity(
            source_session_id=source,
            target_session_id=target,
            mode=selected_mode,
            boundary_seq=sequence,
            contract_version=contract,
            protected_block_sha256=recomputed_hash,
        )
        if boundary_id != expected_id:
            raise ContractValidationError("compression boundary identity mismatch")
        return {
            "compression_boundary_id": boundary_id,
            "source_session_id": source,
            "target_session_id": target,
            "mode": selected_mode,
            "boundary_seq": sequence,
            "contract_version": contract,
            "protected_block_json": stored_json,
            "canonical_json": canonical,
            "protected_block": block.to_dict(),
            "protected_block_sha256": stored_hash,
            "recomputed_sha256": recomputed_hash,
            "created_at": created_at,
            "committed_at": committed_at,
        }

    @classmethod
    def _read_boundary_in_tx(
        cls, conn: sqlite3.Connection, compression_boundary_id: str
    ) -> Optional[dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM compression_boundaries WHERE compression_boundary_id = ?",
            (compression_boundary_id,),
        ).fetchone()
        return None if row is None else cls._row_to_protected_boundary(row)

    def persist_protected_boundary(
        self,
        *,
        source_session_id: str,
        target_session_id: str,
        mode: str,
        protected_block: ProtectedBlock,
        boundary_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Atomically persist one validated ProtectedBlock boundary.

        ``boundary_id`` is optional on first write and can be supplied on a
        retry after the caller has observed the returned identity.  A supplied
        identity is accepted only when the complete durable payload matches.
        """
        source = _text(source_session_id, "source_session_id")
        target = _text(target_session_id, "target_session_id")
        selected_mode = _mode(mode)
        if selected_mode == "IN_PLACE" and source != target:
            raise ContractValidationError("IN_PLACE source and target must match")
        if selected_mode == "ROTATION" and source == target:
            raise ContractValidationError("ROTATION source and target must differ")
        if not isinstance(protected_block, ProtectedBlock):
            raise ContractValidationError("protected_block must be a ProtectedBlock")
        if boundary_id is not None:
            boundary_id = _boundary_id(boundary_id)

        payload = protected_block.to_dict()
        serialized = canonical_json(payload)
        protected_hash = sha256_hex(payload)

        def write(conn: sqlite3.Connection) -> dict[str, Any]:
            sessions = conn.execute(
                "SELECT id FROM sessions WHERE id IN (?, ?)", (source, target)
            ).fetchall()
            if len(sessions) != len({source, target}):
                raise ContractValidationError("source or target session does not exist")

            if boundary_id is not None:
                existing = self._read_boundary_in_tx(conn, boundary_id)
                if existing is not None:
                    if (
                        existing["source_session_id"] != source
                        or existing["target_session_id"] != target
                        or existing["mode"] != selected_mode
                        or existing["protected_block_json"] != serialized
                        or existing["protected_block_sha256"] != protected_hash
                    ):
                        raise ContractValidationError("compression boundary replay mismatch")
                    return existing

            row = conn.execute(
                """SELECT COALESCE(MAX(boundary_seq), 0) + 1
                   FROM compression_boundaries
                   WHERE source_session_id = ?""",
                (source,),
            ).fetchone()
            sequence = int(row[0])
            expected_id = boundary_identity(
                source_session_id=source,
                target_session_id=target,
                mode=selected_mode,
                boundary_seq=sequence,
                contract_version=BOUNDARY_CONTRACT_VERSION,
                protected_block_sha256=protected_hash,
            )
            if boundary_id is not None and boundary_id != expected_id:
                raise ContractValidationError("compression boundary identity mismatch")

            now = _now_text()
            self._insert_protected_boundary(
                conn,
                compression_boundary_id=expected_id,
                source_session_id=source,
                target_session_id=target,
                mode=selected_mode,
                boundary_seq=sequence,
                contract_version=BOUNDARY_CONTRACT_VERSION,
                protected_block_json=serialized,
                protected_block_sha256=protected_hash,
                created_at=now,
                committed_at=now,
            )
            row = conn.execute(
                "SELECT * FROM compression_boundaries WHERE compression_boundary_id = ?",
                (expected_id,),
            ).fetchone()
            assert row is not None
            return self._row_to_protected_boundary(row)

        return self._execute_write(write)

    def read_protected_boundary(self, compression_boundary_id: str) -> Optional[dict[str, Any]]:
        """Read and fully verify one durable boundary, or return ``None``."""
        boundary_id = _boundary_id(compression_boundary_id)
        with self._read_ctx() as conn:
            return self._read_boundary_in_tx(conn, boundary_id)


__all__ = [
    "BOUNDARY_CONTRACT_VERSION",
    "CompressionBoundaryPersistenceMixin",
    "boundary_identity",
]
