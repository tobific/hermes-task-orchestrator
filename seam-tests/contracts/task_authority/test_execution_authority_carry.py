import json
import pytest
from agent.delegation_context import delegated_child_context
from tools import code_execution_tool as code
from tools import terminal_tool as term
from tools import process_registry as process
from tools.code_kernel import shutdown_all_kernels


@pytest.mark.parametrize("profile", ["read-only", "code-read-test", "workspace-write"])
@pytest.mark.parametrize("which", ["code", "terminal", "process"])
def test_existing_adaptive_policy_is_enforced(profile, which, tmp_path, monkeypatch):
    monkeypatch.setenv("TERMINAL_ENV", "local")
    monkeypatch.setattr(code, "_load_config", lambda: {"mode": "strict", "timeout": 30})
    monkeypatch.setattr(process, "_list_processes", lambda *_: [])
    try:
        with delegated_child_context(
            run_id="fixture",
            task_id="fixture",
            generation=1,
            capability_profile=profile,
            write_scope=(str(tmp_path),),
            write_owner_token="fixture-owner",
        ):
            if which == "code":
                result = json.loads(
                    code.execute_code("print('RAN')", task_id="guard-proof")
                )
            elif which == "terminal":
                result = json.loads(term.terminal_tool("true", task_id="guard-proof"))
            else:
                result = json.loads(
                    process._handle_process({"action": "list"}, task_id="guard-proof")
                )
        assert isinstance(result, dict) and result.get("error"), result
    finally:
        shutdown_all_kernels()
