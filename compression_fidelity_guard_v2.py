"""Compression Fidelity Guard v0.2 S0 pure contract layer.

This module is deliberately storage- and runtime-independent.  It contains
strict in-memory representations, canonical JSON/hash helpers, and pure
validation primitives only; it does not import the conversation DB, providers,
or compression pipeline.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Mapping


SCHEMA_VERSION_FACT = "pf-v1"
SCHEMA_VERSION_SNAPSHOT = "ps-v1"
SCHEMA_VERSION_BLOCK = "pb-v1"
GUARD_VERSION = "compression-fidelity-guard-v0.2"
FULL_COMMIT_SHA_LENGTH = 40  # current repository object format: SHA-1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_FACT_ID = re.compile(r"^pf1_[0-9a-f]{64}$")
_BOUNDARY_ID = re.compile(r"^cb1_[0-9a-f]{64}$")


class ContractValidationError(ValueError):
    """Raised when a v0.2 contract value is malformed or not strict."""


class _ValueEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class FactKind(_ValueEnum):
    FILE_PATH = "file_path"
    COMMIT_SHA = "commit_sha"
    LIFECYCLE_STATE = "lifecycle_state"
    TASK_STATE = "task_state"
    ERROR_IDENTITY = "error_identity"
    COMMAND_RESULT = "command_result"
    OWNER_DECISION = "owner_decision"
    ACCEPTANCE_STATE = "acceptance_state"
    NEXT_ACTION = "next_action"
    UNCLASSIFIED = "unclassified"


class CaptureStatus(_ValueEnum):
    CAPTURED = "CAPTURED"
    POINTER_ONLY = "POINTER_ONLY"
    UNRESOLVED = "UNRESOLVED"


class RuntimeCaptureSupport(_ValueEnum):
    NATIVE_STRUCTURED = "NATIVE_STRUCTURED"
    CALLER_ANNOTATED = "CALLER_ANNOTATED"
    POINTER_ONLY = "POINTER_ONLY"
    UNSUPPORTED_V0 = "UNSUPPORTED_V0"


class BoundaryMode(_ValueEnum):
    IN_PLACE = "IN_PLACE"
    ROTATION = "ROTATION"


class ProviderFinalizationStatus(_ValueEnum):
    NOT_STARTED = "NOT_STARTED"
    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class FidelityResult(_ValueEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    ALLOW_WITH_SUPERSEDE_EVIDENCE = "ALLOW_WITH_SUPERSEDE_EVIDENCE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class FidelityReason(_ValueEnum):
    SCHEMA_INVALID = "SCHEMA_INVALID"
    UNKNOWN_SCHEMA_VERSION = "UNKNOWN_SCHEMA_VERSION"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    FACT_MISSING = "FACT_MISSING"
    FACT_ID_MISMATCH = "FACT_ID_MISMATCH"
    FULL_SHA_MISMATCH = "FULL_SHA_MISMATCH"
    PATH_MISMATCH = "PATH_MISMATCH"
    STATE_TRANSITION_INVALID = "STATE_TRANSITION_INVALID"
    SUPERSESSION_EVIDENCE_MISSING = "SUPERSESSION_EVIDENCE_MISSING"
    PROVENANCE_MISSING = "PROVENANCE_MISSING"
    PROVENANCE_STALE = "PROVENANCE_STALE"
    PROVENANCE_IDENTITY_MISMATCH = "PROVENANCE_IDENTITY_MISMATCH"
    INPUT_CONTEXT_CHANGED = "INPUT_CONTEXT_CHANGED"
    CANDIDATE_BLOCK_MISSING = "CANDIDATE_BLOCK_MISSING"
    CANDIDATE_BLOCK_TAMPERED = "CANDIDATE_BLOCK_TAMPERED"
    GUARD_UNAVAILABLE = "GUARD_UNAVAILABLE"


_ALLOWED_SUPPORT: dict[FactKind, RuntimeCaptureSupport] = {
    FactKind.FILE_PATH: RuntimeCaptureSupport.NATIVE_STRUCTURED,
    FactKind.COMMIT_SHA: RuntimeCaptureSupport.NATIVE_STRUCTURED,
    FactKind.LIFECYCLE_STATE: RuntimeCaptureSupport.CALLER_ANNOTATED,
    FactKind.TASK_STATE: RuntimeCaptureSupport.CALLER_ANNOTATED,
    FactKind.ERROR_IDENTITY: RuntimeCaptureSupport.NATIVE_STRUCTURED,
    FactKind.COMMAND_RESULT: RuntimeCaptureSupport.NATIVE_STRUCTURED,
    FactKind.OWNER_DECISION: RuntimeCaptureSupport.CALLER_ANNOTATED,
    FactKind.ACCEPTANCE_STATE: RuntimeCaptureSupport.CALLER_ANNOTATED,
    FactKind.NEXT_ACTION: RuntimeCaptureSupport.CALLER_ANNOTATED,
    FactKind.UNCLASSIFIED: RuntimeCaptureSupport.UNSUPPORTED_V0,
}


def _plain(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, FactProvenancePointer):
        return value.to_dict()
    if isinstance(value, PathIdentity):
        return value.to_dict()
    if isinstance(value, ProtectedFact):
        return value.to_dict()
    if isinstance(value, Supersession):
        return value.to_dict()
    if isinstance(value, ProtectedStateSnapshot):
        return value.to_dict()
    if isinstance(value, ProtectedBlock):
        return value.to_dict()
    if isinstance(value, CompressionBoundary):
        return value.to_dict()
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ContractValidationError("JSON object keys must be strings")
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _reject_nonfinite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ContractValidationError("non-finite number is not allowed")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_nonfinite(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_nonfinite(item)


def canonical_json(value: Any) -> str:
    """Serialize JSON values using the frozen v0.2 byte contract."""
    value = _plain(value)
    _reject_nonfinite(value)
    try:
        return json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ContractValidationError(str(exc)) from exc


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def parse_canonical_json(text: str) -> Any:
    """Parse strict JSON, rejecting duplicate keys and non-finite constants."""
    if not isinstance(text, str):
        raise ContractValidationError("canonical JSON must be text")

    def duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ContractValidationError(f"duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> Any:
        raise ContractValidationError(f"non-finite constant: {value}")

    try:
        return json.loads(
            text,
            object_pairs_hook=duplicate_keys,
            parse_constant=reject_constant,
        )
    except ContractValidationError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ContractValidationError(f"invalid JSON: {exc}") from exc


def sha256_hex(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _enum(enum_type: type[Enum], value: Any, field: str) -> Any:
    try:
        return value if isinstance(value, enum_type) else enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ContractValidationError(f"invalid {field}: {value!r}") from exc


def _strict_dict(data: Any, allowed: set[str], name: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ContractValidationError(f"{name} must be an object")
    unknown = set(data) - allowed
    if unknown:
        raise ContractValidationError(f"unknown field in {name}: {sorted(unknown)}")
    return data


def _required(data: Mapping[str, Any], fields: set[str], name: str) -> None:
    missing = fields - set(data)
    if missing:
        raise ContractValidationError(f"missing field in {name}: {sorted(missing)}")


def _text(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ContractValidationError(f"{field} must be a non-empty string")
    return value


@dataclass(frozen=True)
class FactProvenancePointer:
    session_id: str
    message_id: str
    tool_call_id: str | None = None
    tool_name: str | None = None
    parent_session_id: str | None = None

    _FIELDS: ClassVar[set[str]] = {
        "session_id", "message_id", "tool_call_id", "tool_name", "parent_session_id"
    }

    @classmethod
    def from_dict(cls, data: Any) -> "FactProvenancePointer":
        data = _strict_dict(data, cls._FIELDS, "provenance")
        _required(data, {"session_id", "message_id"}, "provenance")
        values: dict[str, Any] = {}
        for key in cls._FIELDS:
            value = data.get(key)
            if value is not None:
                values[key] = _text(value, f"provenance.{key}")
            else:
                values[key] = None
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        result = {"session_id": self.session_id, "message_id": self.message_id}
        for key in ("tool_call_id", "tool_name", "parent_session_id"):
            value = getattr(self, key)
            if value is not None:
                result[key] = value
        return result


@dataclass(frozen=True)
class PathIdentity:
    raw_value: str
    source_domain: str | None
    identity_mode: str
    identity_value: str

    _FIELDS: ClassVar[set[str]] = {"raw_value", "source_domain", "identity"}

    @classmethod
    def from_dict(cls, data: Any) -> "PathIdentity":
        data = _strict_dict(data, cls._FIELDS, "path_identity")
        _required(data, {"raw_value", "source_domain", "identity"}, "path_identity")
        raw = _text(data["raw_value"], "raw_value", allow_empty=True)
        domain = data["source_domain"]
        if domain is not None:
            domain = _text(domain, "source_domain")
        identity = _strict_dict(data["identity"], {"mode", "value"}, "identity")
        _required(identity, {"mode", "value"}, "identity")
        mode = _text(identity["mode"], "identity.mode")
        value = _text(identity["value"], "identity.value", allow_empty=True)
        if mode != "DOMAIN_EXACT":
            raise ContractValidationError(f"invalid identity.mode: {mode!r}")
        if value != raw:
            raise ContractValidationError("DOMAIN_EXACT identity.value must preserve raw_value")
        return cls(raw, domain, mode, value)

    @property
    def capture_status(self) -> CaptureStatus:
        return CaptureStatus.CAPTURED if self.source_domain is not None else CaptureStatus.POINTER_ONLY

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_value": self.raw_value,
            "source_domain": self.source_domain,
            "identity": {"mode": self.identity_mode, "value": self.identity_value},
        }


@dataclass(frozen=True)
class ProtectedFact:
    schema_version: str
    fact_kind: FactKind
    capture_status: CaptureStatus
    value: Any
    provenance: FactProvenancePointer
    authority_identity: str
    capture_source: RuntimeCaptureSupport | str

    _FIELDS: ClassVar[set[str]] = {
        "schema_version", "fact_kind", "capture_status", "value", "provenance",
        "authority_identity", "capture_source"
    }

    @classmethod
    def from_dict(cls, data: Any) -> "ProtectedFact":
        data = _strict_dict(data, cls._FIELDS, "ProtectedFact")
        _required(data, cls._FIELDS, "ProtectedFact")
        if data["schema_version"] != SCHEMA_VERSION_FACT:
            raise ContractValidationError("unsupported ProtectedFact schema version")
        kind = _enum(FactKind, data["fact_kind"], "fact_kind")
        status = _enum(CaptureStatus, data["capture_status"], "capture_status")
        source_enum = _enum(RuntimeCaptureSupport, data["capture_source"], "capture_source")
        provenance = FactProvenancePointer.from_dict(data["provenance"])
        authority = _text(data["authority_identity"], "authority_identity")
        if kind is FactKind.FILE_PATH:
            if not isinstance(data["value"], dict):
                raise ContractValidationError("file_path value must be a PathIdentity object")
            value: Any = PathIdentity.from_dict(data["value"])
        else:
            value = data["value"]
        if kind is FactKind.COMMIT_SHA and status is CaptureStatus.CAPTURED:
            if not isinstance(value, str) or not _COMMIT_SHA.fullmatch(value):
                raise ContractValidationError("CAPTURED commit_sha must be a full repository hash")

        if status is CaptureStatus.CAPTURED and kind is FactKind.UNCLASSIFIED:
            raise ContractValidationError("unsupported fact kind cannot be CAPTURED")
        support = runtime_support_for(kind)
        if status is CaptureStatus.CAPTURED and support in {
            RuntimeCaptureSupport.POINTER_ONLY, RuntimeCaptureSupport.UNSUPPORTED_V0
        }:
            raise ContractValidationError("fact kind has no CAPTURED runtime support")
        if status is CaptureStatus.CAPTURED and source_enum is RuntimeCaptureSupport.POINTER_ONLY:
            raise ContractValidationError("POINTER_ONLY source cannot produce CAPTURED fact")
        return cls(SCHEMA_VERSION_FACT, kind, status, value, provenance, authority, source_enum)

    @property
    def fact_id(self) -> str:
        return fact_id(self)

    def _identity_payload(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "fact_kind": self.fact_kind.value,
            "capture_status": self.capture_status.value,
            "value": _plain(self.value),
            "authority_identity": self.authority_identity,
        }
        if self.capture_status is not CaptureStatus.CAPTURED:
            payload["provenance"] = self.provenance.to_dict()
        return payload

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "fact_kind": self.fact_kind.value,
            "capture_status": self.capture_status.value,
            "value": _plain(self.value),
            "provenance": self.provenance.to_dict(),
            "authority_identity": self.authority_identity,
            "capture_source": self.capture_source.value if isinstance(self.capture_source, Enum) else self.capture_source,
        }


def fact_id(fact: ProtectedFact) -> str:
    if not isinstance(fact, ProtectedFact):
        raise ContractValidationError("fact_id requires ProtectedFact")
    return "pf1_" + sha256_hex(fact._identity_payload())


@dataclass(frozen=True)
class Supersession:
    old_fact_id: str
    new_fact_id: str
    new_provenance: FactProvenancePointer
    authority_ref: str
    ordering: int

    _FIELDS: ClassVar[set[str]] = {"old_fact_id", "new_fact_id", "new_provenance", "authority_ref", "ordering"}

    @classmethod
    def from_dict(cls, data: Any) -> "Supersession":
        data = _strict_dict(data, cls._FIELDS, "Supersession")
        _required(data, cls._FIELDS, "Supersession")
        for key in ("old_fact_id", "new_fact_id"):
            if not isinstance(data[key], str) or not _FACT_ID.fullmatch(data[key]):
                raise ContractValidationError(f"invalid {key}")
        ordering = data["ordering"]
        if isinstance(ordering, bool) or not isinstance(ordering, int) or ordering < 1:
            raise ContractValidationError("ordering must be a positive integer")
        return cls(
            data["old_fact_id"], data["new_fact_id"],
            FactProvenancePointer.from_dict(data["new_provenance"]),
            _text(data["authority_ref"], "authority_ref"), ordering,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "old_fact_id": self.old_fact_id, "new_fact_id": self.new_fact_id,
            "new_provenance": self.new_provenance.to_dict(),
            "authority_ref": self.authority_ref, "ordering": self.ordering,
        }


@dataclass(frozen=True)
class ProtectedStateSnapshot:
    schema_version: str
    facts: tuple[ProtectedFact, ...]
    supersessions: tuple[Supersession, ...]

    _FIELDS: ClassVar[set[str]] = {"schema_version", "facts", "supersessions"}

    @classmethod
    def from_dict(cls, data: Any) -> "ProtectedStateSnapshot":
        data = _strict_dict(data, cls._FIELDS, "ProtectedStateSnapshot")
        _required(data, cls._FIELDS, "ProtectedStateSnapshot")
        if data["schema_version"] != SCHEMA_VERSION_SNAPSHOT:
            raise ContractValidationError("unsupported ProtectedStateSnapshot schema version")
        if not isinstance(data["facts"], list) or not isinstance(data["supersessions"], list):
            raise ContractValidationError("snapshot facts and supersessions must be arrays")
        return cls(
            SCHEMA_VERSION_SNAPSHOT,
            tuple(ProtectedFact.from_dict(item) for item in data["facts"]),
            tuple(Supersession.from_dict(item) for item in data["supersessions"]),
        )

    @property
    def identity(self) -> str:
        return "ps1_" + sha256_hex(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "facts": [fact.to_dict() for fact in self.facts],
            "supersessions": [item.to_dict() for item in self.supersessions],
        }


@dataclass(frozen=True)
class ProtectedBlock:
    schema_version: str
    facts: tuple[ProtectedFact, ...]
    legacy_status: str | None = None

    _FIELDS: ClassVar[set[str]] = {"schema_version", "facts", "legacy_status"}

    @classmethod
    def from_dict(cls, data: Any) -> "ProtectedBlock":
        data = _strict_dict(data, cls._FIELDS, "ProtectedBlock")
        _required(data, {"schema_version", "facts"}, "ProtectedBlock")
        if data["schema_version"] != SCHEMA_VERSION_BLOCK:
            raise ContractValidationError("unsupported ProtectedBlock schema version")
        if not isinstance(data["facts"], list):
            raise ContractValidationError("ProtectedBlock facts must be an array")
        legacy = data.get("legacy_status")
        if legacy is not None and legacy != "UNPROTECTED":
            raise ContractValidationError("legacy_status must be UNPROTECTED")
        return cls(
            SCHEMA_VERSION_BLOCK,
            tuple(ProtectedFact.from_dict(item) for item in data["facts"]),
            legacy,
        )

    @property
    def identity(self) -> str:
        return "pb1_" + sha256_hex(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "facts": [fact.to_dict() for fact in self.facts],
        }
        if self.legacy_status is not None:
            result["legacy_status"] = self.legacy_status
        return result


@dataclass(frozen=True)
class CompressionBoundary:
    compression_boundary_id: str
    source_session_id: str
    target_session_id: str
    mode: BoundaryMode
    boundary_seq: int
    guard_version: str
    snapshot_identity: dict[str, str]
    created_at: str

    _FIELDS: ClassVar[set[str]] = {
        "compression_boundary_id", "source_session_id", "target_session_id", "mode",
        "boundary_seq", "guard_version", "snapshot_identity", "created_at"
    }

    @classmethod
    def from_dict(cls, data: Any) -> "CompressionBoundary":
        data = _strict_dict(data, cls._FIELDS, "CompressionBoundary")
        _required(data, cls._FIELDS, "CompressionBoundary")
        snapshot = _strict_dict(data["snapshot_identity"], {"snapshot_id", "snapshot_sha256"}, "snapshot_identity")
        _required(snapshot, {"snapshot_id", "snapshot_sha256"}, "snapshot_identity")
        snapshot_id = _text(snapshot["snapshot_id"], "snapshot_identity.snapshot_id")
        snapshot_sha256 = snapshot["snapshot_sha256"]
        if not isinstance(snapshot_sha256, str) or not _SHA256.fullmatch(snapshot_sha256):
            raise ContractValidationError("snapshot_sha256 must be lowercase sha256")
        seq = data["boundary_seq"]
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            raise ContractValidationError("boundary_seq must be a positive integer")
        source = _text(data["source_session_id"], "source_session_id")
        target = _text(data["target_session_id"], "target_session_id")
        mode = _enum(BoundaryMode, data["mode"], "mode")
        if mode is BoundaryMode.IN_PLACE and source != target:
            raise ContractValidationError("IN_PLACE target must equal source")
        if mode is BoundaryMode.ROTATION and source == target:
            raise ContractValidationError("ROTATION target must differ from source")
        boundary_id = _text(data["compression_boundary_id"], "compression_boundary_id")
        if not _BOUNDARY_ID.fullmatch(boundary_id):
            raise ContractValidationError("invalid compression_boundary_id")
        if data["guard_version"] != GUARD_VERSION:
            raise ContractValidationError("unsupported guard_version")
        created_at = _text(data["created_at"], "created_at")
        candidate = cls(boundary_id, source, target, mode, seq, GUARD_VERSION,
                        {"snapshot_id": snapshot_id, "snapshot_sha256": snapshot_sha256}, created_at)
        if boundary_identity(candidate) != boundary_id:
            raise ContractValidationError("compression_boundary_id does not match boundary identity")
        return candidate

    def to_dict(self) -> dict[str, Any]:
        return {
            "compression_boundary_id": self.compression_boundary_id,
            "source_session_id": self.source_session_id,
            "target_session_id": self.target_session_id,
            "mode": self.mode.value,
            "boundary_seq": self.boundary_seq,
            "guard_version": self.guard_version,
            "snapshot_identity": dict(self.snapshot_identity),
            "created_at": self.created_at,
        }


def boundary_identity(boundary: CompressionBoundary) -> str:
    payload = {
        "source_session_id": boundary.source_session_id,
        "target_session_id": boundary.target_session_id,
        "mode": boundary.mode.value,
        "boundary_seq": boundary.boundary_seq,
        "guard_version": boundary.guard_version,
        "snapshot_identity": boundary.snapshot_identity,
    }
    return "cb1_" + sha256_hex(payload)


def make_boundary(
    *, source_session_id: str, target_session_id: str, mode: str | BoundaryMode,
    boundary_seq: int, guard_version: str, snapshot_identity: dict[str, str], created_at: str,
) -> CompressionBoundary:
    provisional = CompressionBoundary(
        "cb1_" + "0" * 64, _text(source_session_id, "source_session_id"),
        _text(target_session_id, "target_session_id"), _enum(BoundaryMode, mode, "mode"),
        boundary_seq, guard_version, dict(snapshot_identity), _text(created_at, "created_at"),
    )
    data = provisional.to_dict()
    data["compression_boundary_id"] = boundary_identity(provisional)
    return CompressionBoundary.from_dict(data)


@dataclass(frozen=True)
class ValidationResult:
    result: FidelityResult
    reason: FidelityReason | None = None


def validate_protected_block(block: Any, *, required_current_kinds: list[FactKind] | tuple[FactKind, ...] = ()) -> ValidationResult:
    if not isinstance(block, ProtectedBlock):
        return ValidationResult(FidelityResult.FAIL, FidelityReason.CANDIDATE_BLOCK_MISSING)
    for item in block.facts:
        if item.fact_id != fact_id(item):
            return ValidationResult(FidelityResult.FAIL, FidelityReason.FACT_ID_MISMATCH)
    present = {item.fact_kind for item in block.facts if item.capture_status is not CaptureStatus.UNRESOLVED}
    for required in required_current_kinds:
        required = _enum(FactKind, required, "required fact kind")
        if required not in present:
            return ValidationResult(FidelityResult.FAIL, FidelityReason.FACT_MISSING)
    return ValidationResult(FidelityResult.PASS)


def validate_recovery_limits(value: Any) -> ValidationResult:
    if not isinstance(value, dict) or set(value) != {"max_rows", "max_chars"}:
        return ValidationResult(FidelityResult.FAIL, FidelityReason.SCHEMA_INVALID)
    rows, chars = value["max_rows"], value["max_chars"]
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0 or rows > 8:
        return ValidationResult(FidelityResult.FAIL, FidelityReason.SCHEMA_INVALID)
    if isinstance(chars, bool) or not isinstance(chars, int) or chars < 0 or chars > 32000:
        return ValidationResult(FidelityResult.FAIL, FidelityReason.SCHEMA_INVALID)
    return ValidationResult(FidelityResult.PASS)


def runtime_support_for(kind: FactKind | str) -> RuntimeCaptureSupport:
    return _ALLOWED_SUPPORT[_enum(FactKind, kind, "fact_kind")]


def provider_replay_semantics(status: ProviderFinalizationStatus | str) -> str:
    status = _enum(ProviderFinalizationStatus, status, "provider status")
    return {
        ProviderFinalizationStatus.COMPLETE: "REPLAY_SKIPPED",
        ProviderFinalizationStatus.FAILED: "RETRY_ALLOWED",
        ProviderFinalizationStatus.PENDING: "RESUME_ALLOWED",
        ProviderFinalizationStatus.NOT_STARTED: "DISPATCH_ALLOWED",
    }[status]


def validate_provider_transition(
    previous: ProviderFinalizationStatus | str,
    current: ProviderFinalizationStatus | str,
) -> ValidationResult:
    previous = _enum(ProviderFinalizationStatus, previous, "provider status")
    current = _enum(ProviderFinalizationStatus, current, "provider status")
    allowed = {
        ProviderFinalizationStatus.NOT_STARTED: {ProviderFinalizationStatus.PENDING},
        ProviderFinalizationStatus.PENDING: {ProviderFinalizationStatus.COMPLETE, ProviderFinalizationStatus.FAILED},
        ProviderFinalizationStatus.FAILED: {ProviderFinalizationStatus.PENDING},
        ProviderFinalizationStatus.COMPLETE: set(),
    }
    if current not in allowed[previous]:
        return ValidationResult(FidelityResult.FAIL, FidelityReason.STATE_TRANSITION_INVALID)
    return ValidationResult(FidelityResult.PASS)


__all__ = [
    "BoundaryMode", "CaptureStatus", "CompressionBoundary", "ContractValidationError",
    "FactKind", "FactProvenancePointer", "FidelityReason", "FidelityResult",
    "PathIdentity", "ProviderFinalizationStatus", "ProtectedBlock", "ProtectedFact",
    "ProtectedStateSnapshot", "RuntimeCaptureSupport", "Supersession", "ValidationResult",
    "boundary_identity", "canonical_bytes", "canonical_json", "fact_id", "make_boundary",
    "parse_canonical_json", "provider_replay_semantics", "runtime_support_for", "sha256_hex",
    "validate_protected_block", "validate_provider_transition", "validate_recovery_limits",
]
