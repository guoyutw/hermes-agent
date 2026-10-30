"""Deterministic Telegram text-reply bridge for OpenChatX deletion approvals.

This module never invokes the Hermes model. It forwards raw Telegram identity
and decision data, signed by a host-local Ed25519 key.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization


def _config_path() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    if local:
        return Path(local) / "hermes" / "openchatx-approval.json"
    return Path.home() / ".hermes" / "openchatx-approval.json"


def _load_config() -> dict[str, Any]:
    path = _config_path()
    data = json.loads(path.read_text(encoding="utf-8"))
    required = ("endpoint", "privateKeyPath", "chatId", "threadId")
    missing = [key for key in required if not str(data.get(key) or "").strip()]
    if missing:
        raise RuntimeError(f"OpenChatX approval config missing: {', '.join(missing)}")
    return data


def _raw_message_identity(message: Any) -> tuple[Any, Any, str, str]:
    chat = getattr(message, "chat", None)
    user = getattr(message, "from_user", None)
    if chat is None or user is None:
        raise RuntimeError("Telegram message is missing raw identity fields.")
    chat_id = str(getattr(message, "chat_id", "") or getattr(chat, "id", "") or "")
    thread_id = str(getattr(message, "message_thread_id", "") or "")
    if not chat_id:
        raise RuntimeError("Telegram message is missing chat identity.")
    return chat, user, chat_id, thread_id


def classify_openchatx_text_approval_candidate(message: Any) -> tuple[bool, str]:
    text = getattr(message, "text", None)
    if text not in {"核准", "取消"}:
        return False, "text_not_exact"
    reply = getattr(message, "reply_to_message", None)
    if reply is None:
        return False, "no_reply_to"
    try:
        config = _load_config()
    except Exception as exc:
        return False, f"config_error:{type(exc).__name__}"
    try:
        _chat, _user, chat_id, thread_id = _raw_message_identity(message)
    except Exception as exc:
        return False, f"identity_error:{type(exc).__name__}"
    expected_chat = str(config["chatId"])
    expected_thread = str(config["threadId"])
    if chat_id != expected_chat:
        return False, f"chat_mismatch:{chat_id}!={expected_chat}"
    if thread_id != expected_thread:
        return False, f"thread_mismatch:{thread_id}!={expected_thread}"
    return True, "matched"


def _signing_payload(payload: dict[str, Any]) -> str:
    return "\n".join([
        "openchatx-telegram-approval-text-v1",
        str(payload["decision"]),
        str(payload["fromId"]),
        "1" if payload["isBot"] else "0",
        str(payload["chatId"]),
        str(payload["threadId"]),
        str(payload["chatType"]),
        str(payload["messageId"]),
        str(payload["replyToMessageId"]),
        str(payload["text"]),
        str(payload["receivedAtMs"]),
    ])


def _post_signed_event(config: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    key_bytes = Path(str(config["privateKeyPath"])).read_bytes()
    key = serialization.load_pem_private_key(key_bytes, password=None)
    signature = key.sign(_signing_payload(payload).encode("utf-8"))
    body = dict(payload)
    body["signature"] = base64.b64encode(signature).decode("ascii")
    request = urllib.request.Request(
        str(config["endpoint"]),
        data=json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5.0) as response:
            response_body = response.read().decode("utf-8")
            return json.loads(response_body or "{}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenChatX rejected approval event ({exc.code}): {detail}") from exc


async def forward_openchatx_text_approval(message: Any) -> dict[str, Any]:
    text = getattr(message, "text", None)
    if text not in {"核准", "取消"}:
        raise RuntimeError("Telegram approval reply must be exactly 核准 or 取消.")
    reply = getattr(message, "reply_to_message", None)
    if reply is None:
        raise RuntimeError("Telegram approval reply must reply to an approval card.")

    config = _load_config()
    chat, user, chat_id, thread_id = _raw_message_identity(message)
    if chat_id != str(config["chatId"]) or thread_id != str(config["threadId"]):
        raise RuntimeError("Telegram approval reply did not originate from the configured route.")

    payload = {
        "version": 1,
        "kind": "text-reply",
        "decision": "approve" if text == "核准" else "cancel",
        "fromId": str(getattr(user, "id", "") or ""),
        "isBot": bool(getattr(user, "is_bot", False)),
        "chatId": chat_id,
        "threadId": thread_id,
        "chatType": str(getattr(chat, "type", "") or ""),
        "messageId": str(getattr(message, "message_id", "") or ""),
        "replyToMessageId": str(getattr(reply, "message_id", "") or ""),
        "text": text,
        "receivedAtMs": int(time.time() * 1000),
    }
    if not payload["fromId"] or not payload["messageId"] or not payload["replyToMessageId"]:
        raise RuntimeError("Telegram approval reply is missing required raw identity fields.")

    return await asyncio.to_thread(_post_signed_event, config, payload)
