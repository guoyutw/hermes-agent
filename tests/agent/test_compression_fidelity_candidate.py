from __future__ import annotations

import copy

import pytest

from agent.conversation_compression import (
    CompressionGuardFailure,
    _committed_transcript_reference,
    _prepare_cfg_candidate,
    _produce_compression_protected_annotations,
    _validate_cfg_candidate_identity,
)
from compression_fidelity_guard_v2 import FidelityResult, validate_protected_block


def _fact(kind: str, value: str, message_id: str) -> dict:
    return {
        "schema_version": "pf-v1",
        "fact_kind": kind,
        "capture_status": "CAPTURED",
        "value": value,
        "provenance": {"session_id": "source", "message_id": message_id},
        "authority_identity": f"test:{kind}",
        "capture_source": "CALLER_ANNOTATED",
    }


def _annotations(task: str = "running") -> dict:
    return {
        "facts": [
            _fact("lifecycle_state", "ACTIVE", "m1"),
            _fact("task_state", task, "m2"),
        ]
    }


def _build(annotations):
    candidate = [{"role": "assistant", "content": "compressed summary"}]
    boundary, snapshot, block = _prepare_cfg_candidate(
        source_session_id="source",
        target_session_id="source",
        mode="IN_PLACE",
        boundary_seq=1,
        created_at="2026-09-03T00:00:00+00:00",
        protected_annotations=annotations,
        previous_snapshot=None,
        candidate=candidate,
    )
    return boundary, snapshot, block, candidate


def test_committed_transcript_reference_is_bounded_db_locator():
    class _DB:
        def get_active_message_ids(self, session_id):
            assert session_id == "target"
            return [101, 102]

    reference = _committed_transcript_reference(
        _DB(),
        source_session_id="source",
        target_session_id="target",
        source_message_watermark=41,
    )

    assert reference == {
        "kind": "session_db_boundary",
        "source_session_id": "source",
        "target_session_id": "target",
        "source_message_watermark": 41,
        "target_active_message_ids": [101, 102],
    }
    assert "messages" not in reference


def test_empty_protected_evidence_fails_closed():
    with pytest.raises(CompressionGuardFailure, match="FACT_MISSING"):
        _build({"facts": []})


def test_real_required_facts_pass_and_bind_candidate_identity():
    boundary, snapshot, block, candidate = _build(_annotations())

    assert validate_protected_block(
        block, required_current_kinds=("lifecycle_state", "task_state")
    ).result is FidelityResult.PASS
    assert snapshot.facts == block.facts
    assert boundary["snapshot_sha256"] != boundary["protected_block_sha256"]
    _validate_cfg_candidate_identity(candidate, block)


def test_distinct_precompression_evidence_produces_distinct_hashes():
    first, _, _, _ = _build(_annotations("running"))
    second, _, _, _ = _build(_annotations("blocked"))

    assert first["snapshot_sha256"] != second["snapshot_sha256"]
    assert first["protected_block_sha256"] != second["protected_block_sha256"]


def test_candidate_missing_or_tampered_machine_identity_fails_closed():
    _, _, block, candidate = _build(_annotations())
    tampered = copy.deepcopy(candidate)
    tampered[1]["content"] = "tampered protected block"

    with pytest.raises(CompressionGuardFailure, match="CANDIDATE_BLOCK_MISSING"):
        _validate_cfg_candidate_identity(tampered, block)


def test_production_producer_captures_required_state_without_injection(tmp_path):
    class TodoStore:
        def read(self):
            return [{"id": "ship", "content": "ship CFG", "status": "in_progress"}]

    class Agent:
        session_id = "real-session"
        session_start = "2026-09-03T00:00:00+00:00"
        _todo_store = TodoStore()
        _session_db = None
        _last_error_identity = {"class": "TimeoutError", "operation": "compress"}

    annotations = _produce_compression_protected_annotations(
        Agent(), [{"id": 42, "role": "user", "content": "continue task"}]
    )
    by_kind = {fact["fact_kind"]: fact for fact in annotations["facts"]}

    assert by_kind["lifecycle_state"]["value"] == "ACTIVE"
    assert by_kind["task_state"]["value"]["active_todos"][0]["id"] == "ship"
    assert by_kind["error_identity"]["value"]["class"] == "TimeoutError"
    assert by_kind["task_state"]["provenance"] == {
        "session_id": "real-session", "message_id": "42"
    }

    candidate = [{"role": "assistant", "content": "summary"}]
    _, _, block = _prepare_cfg_candidate(
        source_session_id="real-session",
        target_session_id="real-session",
        mode="IN_PLACE",
        boundary_seq=1,
        created_at="2026-09-03T00:00:00+00:00",
        protected_annotations=annotations,
        previous_snapshot=None,
        candidate=candidate,
    )
    assert validate_protected_block(
        block, required_current_kinds=("lifecycle_state", "task_state")
    ).result is FidelityResult.PASS
    assert candidate[1]["role"] == "system"
    assert candidate[1]["protected_block"] == block.to_dict()
