"""Payload codecs, chosen from a handler's type hints.

On the wire a payload is just bytes (asynq has no opinion), so Go
handlers can read what Python enqueues as long as both agree on the
encoding. JSON is the default; `bytes` passes through untouched.
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from typing import Any, Protocol

CONTENT_TYPE = "content-type"


class Codec(Protocol):
    @property
    def content_type(self) -> str | None: ...

    def encode(self, value: Any) -> bytes: ...

    def decode(self, data: bytes) -> Any: ...


class RawCodec:
    content_type: str | None = None

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b""
        if isinstance(value, str):
            return value.encode()
        return bytes(value)

    def decode(self, data: bytes) -> bytes:
        return data


def _to_jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: _to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    return value


def dumps(value: Any) -> bytes:
    return json.dumps(_to_jsonable(value), separators=(",", ":")).encode()


class JsonCodec:
    content_type: str | None = "application/json"

    def encode(self, value: Any) -> bytes:
        return dumps(value)

    def decode(self, data: bytes) -> Any:
        return json.loads(data) if data else None


class PydanticCodec(JsonCodec):
    def __init__(self, model: Any):
        self.model = model

    def encode(self, value: Any) -> bytes:
        if not isinstance(value, self.model):
            value = self.model.model_validate(value)
        return bytes(value.model_dump_json().encode())

    def decode(self, data: bytes) -> Any:
        return self.model.model_validate_json(data)


def _from_jsonable(tp: Any, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(tp, type) and dataclasses.is_dataclass(tp):
        hints = typing.get_type_hints(tp)
        kwargs = {
            f.name: _from_jsonable(hints.get(f.name), value[f.name])
            for f in dataclasses.fields(tp)
            if f.name in value
        }
        return tp(**kwargs)
    origin = typing.get_origin(tp)
    args = typing.get_args(tp)
    if origin in (list, tuple, set) and args:
        return origin(_from_jsonable(args[0], v) for v in value)
    if origin is dict and len(args) == 2:
        return {k: _from_jsonable(args[1], v) for k, v in value.items()}
    if origin is typing.Union or origin is types.UnionType:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _from_jsonable(non_none[0], value)
    return value


class DataclassCodec(JsonCodec):
    def __init__(self, cls: type):
        self.cls = cls

    def decode(self, data: bytes) -> Any:
        return _from_jsonable(self.cls, json.loads(data))


def codec_for(annotation: Any) -> Codec:
    if annotation in (bytes, bytearray, memoryview):
        return RawCodec()
    if isinstance(annotation, type):
        if hasattr(annotation, "model_validate_json"):
            return PydanticCodec(annotation)
        if dataclasses.is_dataclass(annotation):
            return DataclassCodec(annotation)
    return JsonCodec()


def encode_result(value: Any) -> bytes | None:
    """Handler return values become the task result: bytes as-is,
    anything else as JSON. None stores nothing."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return dumps(value)
