from tools.environments.local import LocalEnvironment
from tools.environments.base import BaseEnvironment
from agent.delegation_context import (
    delegated_child_context,
    delegated_file_effect_context,
)
import pytest


def test_local_authority_before_spawn_and_kwargs(tmp_path, monkeypatch):
    env = object.__new__(LocalEnvironment)
    env.cwd = str(tmp_path)
    calls = []

    def spawn(self, command, cwd="", **kwargs):
        calls.append(((command,), {"cwd": cwd, **kwargs}))
        return {"output": "ok", "returncode": 0}

    monkeypatch.setattr(BaseEnvironment, "execute", spawn)
    with delegated_child_context(
        run_id="fixture",
        task_id="fixture",
        generation=1,
        capability_profile="read-only",
    ):
        with pytest.raises(PermissionError):
            env.execute("printf ok", cwd=str(tmp_path), timeout=7)
        assert not calls
        with delegated_file_effect_context():
            assert (
                env.execute(
                    "printf ok", cwd=str(tmp_path), timeout=7, stdin_data="body"
                )["output"]
                == "ok"
            )
        assert calls == [
            (("printf ok",), {"cwd": str(tmp_path), "timeout": 7, "stdin_data": "body"})
        ]
        with pytest.raises(PermissionError):
            env.execute("printf denied")
        assert len(calls) == 1
