#!/usr/bin/env python3
"""Create fresh, durable Issue #17 CFG evidence with current runtime code.

This is intentionally an execution witness, not a test-report collector.  It
runs one manual force=True compaction and one gateway hygiene hard-limit
compaction against fresh sessions in the selected state DB, then proves the
new boundaries, payloads, post-boundary appends, and watermark preservation.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import datetime as dt
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import types
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def count_rows(db_path: Path, table: str) -> int:
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def active_rows(db_path: Path, session_id: str) -> list[dict]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            "SELECT id, role, content, active FROM messages "
            "WHERE session_id=? AND active=1 ORDER BY id", (session_id,)
        )]


class TodoStore:
    def __init__(self, route: str):
        self.route = route

    def read(self):
        return [{"id": f"{self.route}-task", "content": f"verify {self.route}", "status": "in_progress"}]

    def format_for_injection(self):
        return ""


class DeterministicCompressor:
    """Only the external summary producer is deterministic; runtime commit is real."""
    def __init__(self, route: str):
        self.route = route
        self.compression_count = 0
        self._last_compress_aborted = False
        self._last_summary_error = None
        self._last_aux_model_failure_model = None
        self._consecutive_timeout_failures = 0

    def bind_session_state(self, *_args):
        return None

    def compress(self, messages, **_kwargs):
        self.compression_count += 1
        return [{"role": "assistant", "content": f"deterministic {self.route} summary"}]


class RuntimeAgent:
    """Minimal host that executes the current production compress_context."""
    last_gateway_instance = None

    def __init__(self, *, session_db, session_id: str, route: str, model: str = "evidence/current-candidate", **_kwargs):
        self._session_db = session_db
        self.session_id = session_id
        self.session_start = dt.datetime.now(dt.timezone.utc).isoformat()
        self.platform = "cli" if route == "manual" else "gateway_hygiene"
        self.model = model
        self.api_mode = "chat_completions"
        self._session_init_model_config = None
        self.working_directory = None
        self.compression_in_place = True
        self.compression_enabled = True
        self.compression_checkpoint_required = False
        self._compression_feasibility_checked = True
        self._cached_system_prompt = "evidence system prompt"
        self._todo_store = TodoStore(route)
        self._memory_manager = None
        self.tools = []
        self.log_prefix = ""
        self.context_compressor = DeterministicCompressor(route)
        self._last_compaction_in_place = False
        self._end_session_on_close = False
        self._print_fn = lambda *_a, **_kw: None
        self.route = route
        if route == "gateway_hygiene":
            type(self).last_gateway_instance = self

    def _build_system_prompt(self, _system):
        return self._cached_system_prompt

    def _invalidate_system_prompt(self):
        return None

    def _emit_status(self, _status):
        return None

    def _emit_warning(self, _warning):
        return None

    def _touch_activity(self, *_a, **_kw):
        return None

    def shutdown_memory_provider(self):
        return None

    def close(self):
        return None

    def _compress_context(self, messages, system_message, **kwargs):
        from agent.conversation_compression import compress_context
        return compress_context(self, messages, system_message, **kwargs)


class CaptureAdapter:
    def __init__(self):
        self.sent = []

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append({"chat_id": chat_id, "content": content})
        return SimpleNamespace(success=True, message_id="evidence")


async def run_gateway_hygiene(db, sid: str, history: list[dict], out: Path):
    """Enter the actual GatewayRunner threshold branch and current compressor."""
    import gateway.run as gateway_run
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import MessageEvent
    from gateway.session import SessionEntry, SessionSource
    from hermes_state import AsyncSessionDB

    cfg = out / "config.yaml"
    cfg.write_text(
        "compression:\n"
        "  enabled: true\n"
        "  hygiene_hard_message_limit: 4\n"
        "  hygiene_timeout_seconds: 30\n"
        "  hygiene_total_ceiling_seconds: 60\n"
        "  hygiene_max_turn_hold_seconds: 30\n",
        encoding="utf-8",
    )
    gateway_run._hermes_home = out
    gateway_run._load_gateway_config = lambda: {
        "model": {"default": "evidence/current-candidate", "context_length": 1000000},
        "compression": {
            "enabled": True,
            "hygiene_hard_message_limit": 4,
            "hygiene_timeout_seconds": 30,
            "hygiene_total_ceiling_seconds": 60,
            "hygiene_max_turn_hold_seconds": 30,
        },
    }
    gateway_run._resolve_runtime_agent_kwargs = lambda: {"api_key": "evidence-key"}

    fake_run_agent = types.ModuleType("run_agent")
    class GatewayEvidenceAgent(RuntimeAgent):
        def __init__(self, **kwargs):
            super().__init__(route="gateway_hygiene", **kwargs)
    fake_run_agent.AIAgent = GatewayEvidenceAgent
    previous_run_agent = sys.modules.get("run_agent")
    sys.modules["run_agent"] = fake_run_agent

    runner = object.__new__(gateway_run.GatewayRunner)
    adapter = CaptureAdapter()
    runner.config = GatewayConfig(platforms={Platform.TELEGRAM: PlatformConfig(enabled=True, token="fake")})
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._voice_mode = {}
    runner.hooks = SimpleNamespace(emit=AsyncMock(), loaded_hooks=False)
    entry = SessionEntry(
        session_key=f"agent:main:telegram:private:{sid}", session_id=sid,
        created_at=dt.datetime.now(), updated_at=dt.datetime.now(),
        platform=Platform.TELEGRAM, chat_type="private",
    )
    runner.session_store = MagicMock()
    runner.session_store.get_or_create_session.return_value = entry
    runner.session_store.load_transcript.return_value = copy.deepcopy(history)
    runner.session_store.has_any_sessions.return_value = True
    runner.session_store.rewrite_transcript = MagicMock()
    runner.session_store.append_to_transcript = MagicMock()
    runner._running_agents = {}
    runner._pending_messages = {}
    runner._pending_approvals = {}
    runner._session_db = AsyncSessionDB(db)
    runner._is_user_authorized = lambda _source: True
    runner._set_session_env = lambda _context: None
    runner._resolve_session_agent_runtime = lambda **_kw: (
        "evidence/current-candidate",
        {"api_key": "evidence-key", "provider": "evidence", "base_url": "http://evidence.invalid"},
    )
    runner._run_agent = AsyncMock(return_value={
        "final_response": "gateway evidence turn complete", "messages": [],
        "tools": [], "history_offset": 0, "last_prompt_tokens": 0,
    })

    event = MessageEvent(
        text="continue after hygiene", message_id="1",
        source=SessionSource(platform=Platform.TELEGRAM, chat_id=sid,
                             chat_type="private", user_id="evidence"),
    )
    try:
        result = await runner._handle_message(event)
    finally:
        if previous_run_agent is None:
            sys.modules.pop("run_agent", None)
        else:
            sys.modules["run_agent"] = previous_run_agent
    return result, adapter.sent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hermes-repo", default=r"C:\Users\a9594\AppData\Local\hermes\hermes-agent")
    parser.add_argument("--state-db", default=r"C:\Users\a9594\AppData\Local\hermes\state.db")
    parser.add_argument("--output-dir", default=str(Path(__file__).resolve().parent))
    args = parser.parse_args()
    repo, state_db, out = Path(args.hermes_repo), Path(args.state_db), Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(repo))
    os.chdir(repo)

    from hermes_state import SessionDB
    import agent.conversation_compression as cc

    log_path = out / "cfg_runtime_evidence.log"
    evidence_db = out / "cfg_runtime_evidence.db"
    json_path = out / "cfg_runtime_evidence.json"
    for artifact in (log_path, evidence_db, json_path):
        if artifact.exists():
            artifact.unlink()

    lines: list[str] = []
    class EvidenceHandler(logging.Handler):
        def emit(self, record):
            text = self.format(record)
            if ("FIDELITY_GUARD_" in text or "COMPACTION_BOUNDARY_" in text
                    or "Session hygiene:" in text or "context compression started" in text):
                lines.append(text)
    handler = EvidenceHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    old_level = root.level
    root.setLevel(logging.INFO)
    root.addHandler(handler)

    db = SessionDB(db_path=state_db)
    baseline = count_rows(state_db, "compression_boundaries")
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    routes = {}
    try:
        # Manual /compress semantics: direct production entry with force=True.
        manual_sid = f"cfg-evidence-manual-{run_id}"
        db.create_session(manual_sid, "cli", model="evidence/current-candidate")
        manual_history = []
        for i in range(6):
            role = "user" if i % 2 == 0 else "assistant"
            content = f"manual pre-boundary {i} {run_id} " + (f"manual-payload-{i}-" * 2000)
            db.append_message(manual_sid, role, content)
            manual_history.append({"role": role, "content": content, "id": i + 1})
        manual_watermark = db.get_active_message_watermark(manual_sid)
        manual_agent = RuntimeAgent(session_db=db, session_id=manual_sid, route="manual")
        manual_result, _ = cc.compress_context(
            manual_agent, copy.deepcopy(manual_history), "evidence system prompt",
            approx_tokens=100000, force=True,
        )
        db.append_message(manual_sid, "user", f"manual post-boundary {run_id}")
        routes["manual"] = {
            "session_id": manual_sid, "force": True,
            "pre_message_count": len(manual_history), "watermark": manual_watermark,
            "returned_message_count": len(manual_result),
        }

        # Gateway path: threshold decision and helper-agent call both execute in gateway.run.
        hygiene_sid = f"cfg-evidence-hygiene-{run_id}"
        db.create_session(hygiene_sid, "telegram", model="evidence/current-candidate")
        hygiene_history = []
        for i in range(8):
            role = "user" if i % 2 == 0 else "assistant"
            content = f"hygiene pre-boundary {i} {run_id} " + (f"hygiene-payload-{i}-" * 2000)
            db.append_message(hygiene_sid, role, content)
            hygiene_history.append({"role": role, "content": content, "timestamp": f"{run_id}-{i}"})
        hygiene_watermark = db.get_active_message_watermark(hygiene_sid)
        gateway_result, sent = asyncio.run(run_gateway_hygiene(db, hygiene_sid, hygiene_history, out))
        db.append_message(hygiene_sid, "user", f"hygiene post-boundary {run_id}")
        routes["gateway_hygiene"] = {
            "session_id": hygiene_sid, "force": False,
            "threshold_path": "compression.hygiene_hard_message_limit",
            "threshold": 4, "observed_message_count": len(hygiene_history),
            "pre_message_count": len(hygiene_history), "watermark": hygiene_watermark,
            "gateway_result": gateway_result, "notices": sent,
        }
    finally:
        root.removeHandler(handler)
        root.setLevel(old_level)
        db.close()

    with sqlite3.connect(state_db) as source:
        source.row_factory = sqlite3.Row
        fresh = [dict(r) for r in source.execute(
            "SELECT * FROM compression_boundaries WHERE source_session_id IN (?,?) "
            "ORDER BY committed_at, compression_boundary_id",
            (routes["manual"]["session_id"], routes["gateway_hygiene"]["session_id"]),
        )]
        for row in fresh:
            row["snapshot"] = json.loads(row.pop("snapshot_json"))
            row["protected_block"] = json.loads(row.pop("protected_block_json"))
        current_count = int(source.execute("SELECT COUNT(*) FROM compression_boundaries").fetchone()[0])

    checks = []
    for route, info in routes.items():
        sid = info["session_id"]
        boundary = next((b for b in fresh if b["source_session_id"] == sid), None)
        active = active_rows(state_db, sid)
        with sqlite3.connect(state_db) as conn:
            archived_pre = int(conn.execute(
                "SELECT COUNT(*) FROM messages WHERE session_id=? AND active=0 AND id<=?",
                (sid, info["watermark"]),
            ).fetchone()[0])
            post = int(conn.execute(
                "SELECT COUNT(*) FROM messages WHERE session_id=? AND active=1 AND id>?",
                (sid, info["watermark"]),
            ).fetchone()[0])
        payload_ok = bool(boundary and boundary["snapshot"] and boundary["protected_block"])
        check = {
            "route": route, "session_id": sid,
            "boundary_id": boundary["compression_boundary_id"] if boundary else None,
            "boundary_new": bool(boundary), "boundary_seq": boundary["boundary_seq"] if boundary else None,
            "snapshot_payload_readback": payload_ok,
            "snapshot_sha256": boundary["snapshot_sha256"] if boundary else None,
            "protected_block_sha256": boundary["protected_block_sha256"] if boundary else None,
            "hashes_distinct": bool(boundary and boundary["snapshot_sha256"] != boundary["protected_block_sha256"]),
            "archived_pre_watermark_count": archived_pre,
            "expected_pre_watermark_count": info["pre_message_count"],
            "post_watermark_active_count": post,
            "active_readback_count": len(active),
            "continued_use": post >= 1 and any("post-boundary" in str(r["content"]) for r in active),
            "no_loss": archived_pre == info["pre_message_count"] and post >= 1,
        }
        checks.append(check)

    guard_passes = sum("FIDELITY_GUARD_PASS" in line for line in lines)
    commits = sum("COMPACTION_BOUNDARY_COMMITTED" in line for line in lines)
    hygiene_triggered = any("Session hygiene:" in line and "auto-compressing" in line for line in lines)
    distinct_route_hashes = len({b["snapshot_sha256"] for b in fresh}) == 2
    success = (
        len(fresh) == 2 and current_count == baseline + 2 and guard_passes == 2
        and commits == 2 and hygiene_triggered and distinct_route_hashes
        and all(c["snapshot_payload_readback"] and c["hashes_distinct"]
                and c["continued_use"] and c["no_loss"] for c in checks)
    )

    # Durable, self-contained evidence DB: exact fresh boundary rows + checks.
    audit = sqlite3.connect(evidence_db)
    audit.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    audit.execute("CREATE TABLE boundaries (route TEXT NOT NULL, payload_json TEXT NOT NULL)")
    audit.execute("CREATE TABLE verification_checks (route TEXT NOT NULL, payload_json TEXT NOT NULL)")
    for boundary in fresh:
        route = "manual" if boundary["source_session_id"] == routes["manual"]["session_id"] else "gateway_hygiene"
        audit.execute("INSERT INTO boundaries VALUES (?,?)", (route, json.dumps(boundary, sort_keys=True)))
    for check in checks:
        audit.execute("INSERT INTO verification_checks VALUES (?,?)", (check["route"], json.dumps(check, sort_keys=True)))
    for key, value in {
        "run_id": run_id, "source_state_db": str(state_db),
        "baseline_boundary_count": str(baseline), "final_boundary_count": str(current_count),
        "source_state_db_sha256": sha256(state_db),
    }.items():
        audit.execute("INSERT INTO metadata VALUES (?,?)", (key, value))
    audit.commit()
    audit.close()

    lines.extend([
        f"EVIDENCE baseline_boundaries={baseline} final_boundaries={current_count} delta={current_count-baseline}",
        f"EVIDENCE gateway_hygiene_threshold_triggered={hygiene_triggered}",
        f"EVIDENCE fresh_boundary_ids={[b['compression_boundary_id'] for b in fresh]}",
        f"EVIDENCE continued_use={all(c['continued_use'] for c in checks)} no_loss={all(c['no_loss'] for c in checks)}",
        f"EVIDENCE result={'PASS' if success else 'FAIL'}",
    ])
    log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = {
        "schema_version": 2, "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_id": run_id, "hermes_repo": str(repo), "source_state_db": str(state_db),
        "current_candidate": {"conversation_compression_sha256": sha256(repo / "agent" / "conversation_compression.py"),
                              "gateway_run_sha256": sha256(repo / "gateway" / "run.py")},
        "baseline_boundary_count": baseline, "boundary_count": current_count,
        "new_boundary_count": current_count - baseline,
        "fresh_boundaries": fresh, "checks": checks,
        "log_witness": {"fidelity_guard_pass_count": guard_passes,
                        "boundary_committed_count": commits,
                        "gateway_hygiene_threshold_triggered": hygiene_triggered},
        "distinct_route_snapshot_hashes": distinct_route_hashes,
        "continued_use": all(c["continued_use"] for c in checks),
        "no_loss": all(c["no_loss"] for c in checks),
        "success": success,
        "artifacts": {"log": str(log_path), "db": str(evidence_db), "json": str(json_path)},
    }
    result["artifact_sha256"] = {
        "run_cfg_runtime_evidence.py": sha256(Path(__file__)),
        "cfg_runtime_evidence.log": sha256(log_path),
        "cfg_runtime_evidence.db": sha256(evidence_db),
    }
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({
        "result": "PASS" if success else "FAIL", "baseline_boundary_count": baseline,
        "boundary_count": current_count, "new_boundary_count": current_count-baseline,
        "fresh_boundary_ids": [b["compression_boundary_id"] for b in fresh],
        "gateway_hygiene_threshold_triggered": hygiene_triggered,
        "fidelity_guard_pass_count": guard_passes, "boundary_committed_count": commits,
        "continued_use": result["continued_use"], "no_loss": result["no_loss"],
        "artifacts": result["artifacts"],
    }, indent=2))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
