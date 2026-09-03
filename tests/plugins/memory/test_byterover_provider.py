"""Tests for the ByteRover memory provider config gates."""

from plugins.memory.byterover import ByteRoverMemoryProvider


def test_auto_extract_false_skips_sync_turn(monkeypatch):
    calls = []
    provider = ByteRoverMemoryProvider({"auto_extract": False})
    provider.initialize("session-1")

    monkeypatch.setattr("plugins.memory.byterover._run_brv", lambda *args, **kwargs: calls.append((args, kwargs)))

    provider.sync_turn("please remember this detail", "acknowledged")

    assert calls == []
    assert provider._sync_thread is None


def test_pre_compress_is_observational_only(monkeypatch):
    calls = []
    provider = ByteRoverMemoryProvider({"auto_extract": True})
    monkeypatch.setattr(
        "plugins.memory.byterover._run_brv",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    assert provider.on_pre_compress([{"role": "user", "content": "remember me"}]) == ""
    assert calls == []


def test_boundary_hook_owns_former_precompress_curate(monkeypatch):
    calls = []
    provider = ByteRoverMemoryProvider({"auto_extract": True})
    monkeypatch.setattr(
        "plugins.memory.byterover._run_brv",
        lambda args, **kwargs: calls.append((args, kwargs)) or {"success": True},
    )
    context = {
        "compression_boundary_id": "boundary-1",
        "source_session_id": "source",
        "target_session_id": "target",
        "mode": "ROTATION",
        "old_session_identity": "source",
        "snapshot_id": "snapshot-1",
        "snapshot_sha256": "a" * 64,
        "protected_block_sha256": "b" * 64,
        "guard_version": "cfg-s1b-v1",
    }

    provider.finalize_memory_for_boundary(context, idempotency_key="boundary-1")

    assert len(calls) == 1
    assert calls[0][0][:2] == ["curate", "--"]
    assert "boundary-1" in calls[0][0][2]
