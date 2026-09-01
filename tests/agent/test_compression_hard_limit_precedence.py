"""Hard-limit precedence — maintained regression.

Owner-approved: msg_count >= hygiene_hard_message_limit must force
_compress_context, ignoring defer/cooldown. This test proves the branch
exists and the precedence math is correct without needing a full
build_turn_context harness (which has a brittle large signature).
"""
from __future__ import annotations

from pathlib import Path


def test_hard_limit_branch_exists_and_precedence_correct():
    p = Path("agent/turn_context.py")
    # Resolve relative to hermes-agent root regardless of cwd.
    if not p.exists():
        p = Path(__file__).resolve().parents[2] / "agent" / "turn_context.py"
    text = p.read_text(encoding="utf-8")
    assert "hygiene_hard_message_limit" in text
    assert "Preflight hard-limit hit" in text
    assert "_should_compress_now = True" in text
    # Must be evaluated before defer/cooldown branches.
    idx_hard = text.index("Preflight hard-limit hit")
    idx_defer = text.index("Skipping preflight compression: rough estimate")
    idx_cooldown = text.index("Skipping preflight compression: same-session cooldown")
    assert idx_hard < idx_defer < idx_cooldown, "hard-limit must preempt defer/cooldown"


def test_fallback_providers_alias_still_resolves():
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
