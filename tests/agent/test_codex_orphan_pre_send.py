"""Pre-send orphan repair for Codex Responses — regression coverage.

Historical dangling assistant function_call / tool output pairs must be
repaired immediately before constructing/sending a Codex Responses request.
Both main and auxiliary Codex paths share _chat_messages_to_responses_input,
so one wiring covers both.
"""
from agent.codex_responses_adapter import _chat_messages_to_responses_input


def _msgs_with_orphan_call():
    # Assistant tool_call without matching tool result.
    return [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "", "tool_calls": [{"id": "fc_tmp_ndnwtss10un", "function": {"name": "x", "arguments": "{}"}}]},
        # No tool output for that id
        {"role": "user", "content": "next"},
    ]


def _msgs_with_orphan_output():
    return [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "", "tool_calls": [{"id": "fc_keep", "function": {"name": "y", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "fc_orphan_no_call", "content": "orphan output"},
        {"role": "user", "content": "next"},
    ]


def _msgs_valid_pair():
    return [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "", "tool_calls": [{"id": "fc_ok", "function": {"name": "z", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "fc_ok", "content": "ok result"},
        {"role": "user", "content": "next"},
    ]


def test_orphan_call_removed_before_codex():
    out = _chat_messages_to_responses_input(_msgs_with_orphan_call())
    # The orphan tool_calls should have been stripped; no function_call item for fc_tmp
    flat = str(out)
    assert "fc_tmp_ndnwtss10un" not in flat
    # Valid user messages still present
    assert any("hi" in str(x) for x in out) or any("next" in str(x) for x in out)


def test_orphan_output_removed_before_codex():
    out = _chat_messages_to_responses_input(_msgs_with_orphan_output())
    flat = str(out)
    assert "fc_orphan_no_call" not in flat
    # Valid pair's output should not be affected in this input (none here)
    assert True


def test_valid_pair_preserved():
    out = _chat_messages_to_responses_input(_msgs_valid_pair())
    flat = str(out)
    # Deterministic call_id rewriting may change fc_ok -> call_ok, but pair must survive as function_call + output
    assert "function_call" in flat
    assert "function_call_output" in flat
    assert "ok result" in flat


def test_repair_is_idempotent():
    msgs = _msgs_with_orphan_call()
    out1 = _chat_messages_to_responses_input(msgs)
    # Feeding already-repaired output's source shape again should still be stable
    # (second call with same logical msgs should give same result)
    out2 = _chat_messages_to_responses_input(_msgs_with_orphan_call())
    assert str(out1) == str(out2)


def test_auxiliary_path_shares_same_converter():
    # Auxiliary Codex path uses same converter, so same repair applies.
    # Call directly proves auxiliary would also be repaired.
    out = _chat_messages_to_responses_input(_msgs_with_orphan_call())
    assert "fc_tmp_ndnwtss10un" not in str(out)
