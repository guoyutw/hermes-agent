"""Maintained regression: hygiene turn-hold must detach, not cancel.

Covers fix fd05793 for 20260831_104801_8c85cbc2 / 20260831_090733_f2edf34c
where the 10.0s turn-hold previously called try_cancel_before_commit()
and aborted as explicit_interrupt (401->0). New path detaches so
fallback+commit can finish in background (399->149 at 75.2s).

This is a source-level contract test (AGENTS.md bans Temp scripts for
maintained evidence).
"""
import pathlib


GATEWAY_RUN = pathlib.Path(__file__).resolve().parents[2] / "gateway" / "run.py"


def test_hygiene_turn_hold_detaches_without_cancel():
    src = GATEWAY_RUN.read_text(encoding="utf-8", errors="ignore")
    assert 'session hygiene turn-hold (detached)' in src
    idx = src.find("except HygieneTurnHoldExceeded:")
    assert idx != -1
    end = src.find("                                    except asyncio.TimeoutError:", idx + 1)
    assert end != -1
    branch = src[idx:end]
    # Exclude comment lines — the fix comment mentions the old name
    code_lines = "\n".join(l for l in branch.splitlines() if not l.strip().startswith("#"))
    assert "try_cancel_before_commit" not in code_lines, "hygiene turn-hold must not cancel"
    assert "release_cancelled_compression_lock" not in code_lines, "must not release cancelled lock"
    assert "_hyg_cleanup_deferred = True" in branch
    assert "_defer_agent_cleanup_until_future_done" in branch


def test_hygiene_detach_still_records_turnhold_cooldown():
    src = GATEWAY_RUN.read_text(encoding="utf-8", errors="ignore")
    idx = src.find("except HygieneTurnHoldExceeded:")
    end = src.find("                                    except asyncio.TimeoutError:", idx + 1)
    branch = src[idx:end]
    # Short non-escalating retry must still be recorded
    assert "_HYGIENE_TURNHOLD_RETRY_SECONDS" in branch
    assert "_record_hygiene_cooldown" in branch
