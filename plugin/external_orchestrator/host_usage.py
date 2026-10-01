"""Host-only canonical token observations, never model-authored metrics."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

_FIELDS = (
    ("input_tokens", "session_input_tokens"),
    ("cache_read_tokens", "session_cache_read_tokens"),
    ("cache_write_tokens", "session_cache_write_tokens"),
    ("output_tokens", "session_output_tokens"),
    ("reasoning_tokens", "session_reasoning_tokens"),
)
_SINK = ContextVar("external_native_usage_sink", default=None)


@dataclass(frozen=True)
class TokenUsage:
    values: tuple
    complete: bool

    def counters(self):
        result = dict(self.values)
        allowed = {name for name, _ in _FIELDS}
        if len(result) != len(self.values) or not set(result) <= allowed:
            raise ValueError("invalid host usage fields")
        if not {"input_tokens", "cache_read_tokens", "output_tokens"} <= set(result):
            raise ValueError("missing canonical host counters")
        if type(self.complete) is not bool or any(
            type(v) is not int or not 0 <= v < 2**63 for v in result.values()
        ):
            raise ValueError("invalid host counters")
        return result


def capture_usage(agent, *, complete):
    values = tuple(
        (key, getattr(agent, attr)) for key, attr in _FIELDS if hasattr(agent, attr)
    )
    usage = TokenUsage(values, complete)
    try:
        usage.counters()
    except (TypeError, ValueError):
        return None
    return usage


@contextmanager
def bind_usage_sink(callback):
    token = _SINK.set(callback)
    try:
        yield
    finally:
        _SINK.reset(token)


def get_usage_sink():
    return _SINK.get()
