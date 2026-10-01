"""Literal replay-preservation assertions against the unmodified routing patch."""

import asyncio
from contextlib import ExitStack
from contextvars import copy_context
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from types import SimpleNamespace as NS
from unittest.mock import patch

import pytest

ORIGINAL = {"provider": "openai-codex", "model": "bound-A", "tier": "priority"}
CHANGED = {"provider": "openrouter", "model": "decided-B", "tier": "normal"}
DISPLACED = {"provider": "openrouter", "model": "saved-displaced"}
EVENT_ID = "original-event-137"


class Runtime:
    def __init__(self, sessions, register=True):
        self.sessions = Path(sessions)
        self.register = register
        self.stack = ExitStack()
        self.pick = dict(ORIGINAL)
        self.decisions = []
        self.auth = []
        self.bound = []
        self.double = False
        self.park = False

    def __enter__(self):
        import gateway.run as g
        import hermes_cli.plugins as plugins
        import hermes_cli.runtime_provider as providers
        from gateway.config import GatewayConfig, Platform
        from gateway.session import SessionSource, SessionStore
        from gateway.route_policy import RouteProposal

        def no_network(*args, **kwargs):
            raise AssertionError("Unexpected network access in offline replay proof")

        def auth(*, requested=None, **kwargs):
            provider = requested or "openrouter"
            self.auth.append({"requested": requested, "resolved": provider})
            return {
                "provider": provider,
                "api_key": "offline-fixture",
                "base_url": "http://blocked.invalid",
            }

        self.stack.enter_context(patch.object(socket.socket, "connect", no_network))
        self.stack.enter_context(
            patch.object(providers, "resolve_runtime_provider", auth)
        )
        self.config = {
            "model": {"default": "configured-default", "provider": "openrouter"},
            "agent": {"service_tier": "normal"},
        }
        self.stack.enter_context(
            patch.object(g, "_load_gateway_config", lambda: self.config)
        )
        self.source_file = g.__file__
        self.manager = plugins.PluginManager()
        self.stack.enter_context(
            patch.object(plugins, "get_plugin_manager", lambda: self.manager)
        )
        self.store = SessionStore(sessions_dir=self.sessions, config=GatewayConfig())
        self.source = SessionSource(
            platform=Platform.TELEGRAM,
            chat_id="replay-fixture",
            user_id="u",
            user_name="fixture",
        )
        self.entry = self.store.get_or_create_session(self.source)
        self.key = self.entry.session_key
        if self.store.get_model_override(self.key) is None:
            self.store.set_model_override(self.key, DISPLACED)
        self.runner = object.__new__(g.GatewayRunner)
        self.runner.config = GatewayConfig()
        self.runner.session_store = self.store
        self.runner._session_model_overrides = {}
        self.runner._service_tier = None
        self.runner._hmwa_resolve_session = self.resolve_session
        self.runner._hmwa_prepare_turn = self.prepare
        self.generation = self.runner._begin_session_run_generation(self.key)
        self.event = NS(text="the original user message", message_id=EVENT_ID)
        self.handle = None

        def policy(request):
            self.decisions.append({
                "turn_id": request.turn_id,
                "key": request.session_key,
                "generation": request.generation,
                "result": dict(self.pick),
            })
            return RouteProposal(
                self.pick["provider"], self.pick["model"], self.pick["tier"]
            )

        if self.register:
            ctx = plugins.PluginContext(
                plugins.PluginManifest(name="replay-proof"), self.manager
            )
            self.handle = ctx.register_gateway_route_policy("select", policy)
        return self

    def __exit__(self, *exc):
        if self.handle is not None:
            self.handle.release()
        self.stack.close()

    async def resolve_session(self, event, source):
        entry = self.store.get_or_create_session(source)
        assert entry.session_id == self.entry.session_id
        return source, entry, entry.session_key

    async def prepare(self, event, source, *args):
        from gateway.route_policy import active_binding

        # This is the real hygiene settings resolver, not a policy/runtime stub.
        hs = await self.runner._hmwa_hygiene_settings(source, self.key)
        model, runtime = self.runner._resolve_session_agent_runtime(
            source=source, session_key=self.key, user_config=self.config
        )
        assert hs.model == model and hs.provider == runtime["provider"]
        tier = self.runner._resolve_session_service_tier(
            source=source, session_key=self.key
        )
        route = self.runner._resolve_turn_agent_config(event.text, model, runtime)
        value = {
            "provider": route["runtime"]["provider"],
            "model": route["model"],
            "tier": route.get("service_tier", tier) or "normal",
        }
        self.bound.append(value)
        binding = active_binding(self.runner)
        if self.park:
            self.interrupted_binding = binding
            self.interrupted_context = copy_context()
            self.started.set()
            await asyncio.Event().wait()
        if self.double:
            again_model, again_runtime = self.runner._resolve_session_agent_runtime(
                source=source, session_key=self.key, user_config=self.config
            )
            assert (again_model, again_runtime["provider"]) == (
                model,
                runtime["provider"],
            )
            self.bound.append(dict(value))
        # Stop at the route boundary. The real handler returns this preparation
        # result without constructing a model client or sending anything.
        return value, {}

    async def attempt(self):
        return await self.runner._handle_message_with_agent(
            self.event, self.source, self.key, self.generation
        )

    def observation(self, name):
        return {
            "case": name,
            "scenario_completed": True,
            "gateway_source": self.source_file,
            "pid": os.getpid(),
            "session_id": self.entry.session_id,
            "session_key": self.key,
            "event_id": EVENT_ID,
            "bound": self.bound,
            "decisions": self.decisions,
            "auth": self.auth,
            "persisted_override": self.store.get_model_override(self.key),
        }


def check_preservation(record_property, observation):
    route_equal = all(value == ORIGINAL for value in observation["bound"])
    no_redecision = len(observation["decisions"]) == 1
    observation["route_equal"] = route_equal
    observation["no_redecision"] = no_redecision
    observation["preservation_passed"] = route_equal and no_redecision
    record_property("observation", json.dumps(observation, sort_keys=True))
    assert observation["preservation_passed"], json.dumps(observation, sort_keys=True)


def test_same_scope_control(tmp_path, record_property):
    with Runtime(tmp_path / "sessions") as r:
        r.double = True
        asyncio.run(r.attempt())
        assert len(r.bound) == 2 and len(r.auth) == 1
        check_preservation(record_property, r.observation("same_scope_control"))


def test_clean_replay_stable_callback(tmp_path, record_property):
    with Runtime(tmp_path / "sessions") as r:
        asyncio.run(r.attempt())
        asyncio.run(r.attempt())
        assert len(r.bound) == 2
        check_preservation(
            record_property, r.observation("clean_replay_stable_callback")
        )


def test_clean_replay_changed_callback_output(tmp_path, record_property):
    with Runtime(tmp_path / "sessions") as r:
        asyncio.run(r.attempt())
        r.pick = dict(CHANGED)
        asyncio.run(r.attempt())
        assert len(r.bound) == 2
        check_preservation(
            record_property, r.observation("clean_replay_changed_callback_output")
        )


def test_replay_after_real_cancellation(tmp_path, record_property):
    from gateway.route_policy import active_binding, RouteDecisionError

    with Runtime(tmp_path / "sessions") as r:

        async def scenario():
            r.park = True
            r.started = asyncio.Event()
            task = asyncio.create_task(r.attempt())
            await asyncio.wait_for(r.started.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert r.interrupted_binding.revoked.is_set()
            with pytest.raises(RouteDecisionError):
                r.interrupted_context.run(active_binding, r.runner)
            r.park = False
            r.pick = dict(CHANGED)
            r.generation = r.runner._invalidate_session_run_generation(
                r.key, reason="test cancellation"
            )
            await r.attempt()

        asyncio.run(scenario())
        assert len(r.bound) == 2
        observation = r.observation("replay_after_real_cancellation")
        observation["old_scope_revoked"] = True
        check_preservation(record_property, observation)


def test_replay_without_callback(tmp_path, record_property):
    with Runtime(tmp_path / "sessions") as r:
        asyncio.run(r.attempt())
        r.handle.release()
        assert not r.manager._gateway_route_policies
        r.handle = None
        asyncio.run(r.attempt())
        assert len(r.bound) == 2
        check_preservation(record_property, r.observation("replay_without_callback"))


def test_fresh_process_resume_without_callback(tmp_path, record_property):
    with Runtime(tmp_path / "sessions") as r:
        asyncio.run(r.attempt())
        env = dict(os.environ)
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
        child = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--resume",
                str(r.sessions),
            ],
            env=env,
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            timeout=30,
        )
        record_property("child_stdout", child.stdout)
        record_property("child_stderr", child.stderr)
        record_property("child_exit", child.returncode)
        assert child.returncode == 0, child.stderr
        lines = [
            line
            for line in child.stdout.splitlines()
            if line.startswith("REPLAY_CHILD=")
        ]
        assert len(lines) == 1
        replay = json.loads(lines[0].split("=", 1)[1])
        assert replay["pid"] != os.getpid()
        assert (replay["session_id"], replay["session_key"], replay["event_id"]) == (
            r.entry.session_id,
            r.key,
            EVENT_ID,
        )
        observation = r.observation("fresh_process_resume_without_callback")
        observation["fresh_process"] = replay
        observation["bound"] = [*r.bound, *replay["bound"]]
        observation["auth"] = [*r.auth, *replay["auth"]]
        observation["decisions"] = [*r.decisions, *replay["decisions"]]
        check_preservation(record_property, observation)


if __name__ == "__main__":
    assert sys.argv[1] == "--resume"
    with Runtime(sys.argv[2], register=False) as runtime:
        asyncio.run(runtime.attempt())
        print(
            "REPLAY_CHILD="
            + json.dumps(runtime.observation("fresh_child"), sort_keys=True)
        )
