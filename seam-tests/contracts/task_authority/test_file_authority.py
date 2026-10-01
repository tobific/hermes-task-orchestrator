"""Real file-tool effects under unchanged, existing delegated authority."""

import json
import pytest
from agent.delegation_context import delegated_child_context
from tools.file_tools import write_file_tool, patch_tool


@pytest.mark.parametrize("operation", ["write", "patch"])
@pytest.mark.parametrize("profile", ["read-only", "workspace-write"])
def test_out_of_scope_file_effect_is_denied(tmp_path, operation, profile):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = tmp_path / "outside.txt"
    target.write_text("before")
    with delegated_child_context(
        run_id="fixture",
        task_id="fixture",
        generation=1,
        capability_profile=profile,
        write_scope=(str(allowed),),
        write_owner_token="fixture-owner",
    ):
        if operation == "write":
            result = json.loads(
                write_file_tool(str(target), "after", task_id="fixture")
            )
        else:
            result = json.loads(
                patch_tool(
                    mode="replace",
                    path=str(target),
                    old_string="before",
                    new_string="after",
                    task_id="fixture",
                )
            )
    assert target.read_text() == "before", result
    assert result.get("error") or result.get("status") == "blocked", result


@pytest.mark.parametrize("delegated", [False, True])
def test_approved_write_keeps_existing_effect(tmp_path, delegated):
    target = tmp_path / "allowed.txt"
    kwargs = (
        dict(
            run_id="fixture",
            task_id="fixture",
            generation=1,
            capability_profile="workspace-write",
            write_scope=(str(tmp_path),),
            write_owner_token="fixture-owner",
        )
        if delegated
        else {}
    )
    with delegated_child_context(**kwargs):
        result = json.loads(
            write_file_tool(str(target), "exact value", task_id="fixture")
        )
    assert not result.get("error"), result
    assert target.read_text() == "exact value"
