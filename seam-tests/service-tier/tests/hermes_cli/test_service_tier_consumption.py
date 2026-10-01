"""Required policy must reach the real native request builder without cache mutations."""

from types import SimpleNamespace as NS
import pytest


@pytest.fixture
def policy(tmp_path, monkeypatch):
    import socket
    import hermes_cli.plugins as plugins

    monkeypatch.setattr(
        socket.socket, "connect", lambda *a: pytest.fail("external network forbidden")
    )
    home = tmp_path / "profile"
    plugin = home / "plugins" / "tiers"
    plugin.mkdir(parents=True)
    (home / "config.yaml").write_text(
        "plugins:\n  enabled: [tiers]\nservice_tier_policy: tiers:required\n"
    )
    (plugin / "plugin.yaml").write_text("name: tiers\nversion: 1.0.0\n")
    (plugin / "__init__.py").write_text("""
value='priority'
seen=[]
def decide(request):
    seen.append(request)
    if value == 'raise': raise ValueError('private error')
    return value
def register(ctx):
    global handle
    handle=ctx.register_service_tier_policy('required',decide)
""")
    monkeypatch.setenv("HERMES_HOME", str(home))
    manager = plugins.PluginManager()
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    manager.discover_and_load()
    return NS(home=home, manager=manager)


def agent():
    return NS(
        model="gpt-6-astra",
        provider="openai-codex",
        base_url="https://chatgpt.com/backend-api/codex",
        api_mode="codex_responses",
        platform="telegram",
        session_id="s",
        service_tier=None,
        request_overrides={"temperature": 0.4},
    )


def wire(a):
    from agent.fast_mode import effective_request_overrides
    from agent.transports.codex import ResponsesApiTransport

    return ResponsesApiTransport().build_kwargs(
        model=a.model,
        messages=[
            {"role": "system", "content": "unchanged"},
            {"role": "user", "content": "hello"},
        ],
        tools=[],
        request_overrides=effective_request_overrides(a),
        provider=a.provider,
        base_url=a.base_url,
        is_codex_backend=True,
    )


def test_required_policy_reaches_native_wire(policy):
    from agent.fast_mode import begin_turn

    a = agent()
    before = dict(a.request_overrides)
    begin_turn(a, [])
    assert wire(a).get("service_tier") == "priority"
    assert a.request_overrides == before


def terminal(a, payload):
    from agent.turn_api_call import perform_api_call
    from agent.transports.codex import ResponsesApiTransport

    sink = []
    a._has_stream_consumers = lambda: True
    a._is_copilot_url = lambda: False
    a._is_codex_backend = lambda: True
    a._get_transport = lambda: ResponsesApiTransport()
    a._has_pending_redirect = lambda: False
    a._interruptible_streaming_api_call = lambda request, **kw: (
        sink.append(request) or NS()
    )
    perform_api_call(
        a,
        api_kwargs=payload,
        _original_api_kwargs=payload,
        _llm_middleware_trace=[],
        _moa_prepared_request=None,
        _retry=NS(),
        thinking_spinner=None,
        retry_count=0,
        api_call_count=0,
        api_request_id="r",
        effective_task_id="t",
        turn_id="u",
        interrupted=False,
    )
    from tests.fast_consumer_receipts import record

    record("terminal-wire", sink)
    return sink


def test_execution_middleware_cannot_displace_required_tier(policy):
    from agent.fast_mode import begin_turn
    from hermes_cli.plugins import PluginContext, PluginManifest

    ctx = PluginContext(PluginManifest(name="modifier"), policy.manager)

    def modify(**kw):
        request = dict(kw["request"])
        request.pop("service_tier", None)
        return kw["next_call"](request)

    ctx.register_middleware("llm_execution", modify)
    a = agent()
    begin_turn(a, [])
    assert terminal(a, wire(a))[0].get("service_tier") == "priority"


@pytest.mark.parametrize(
    "failure", ["missing", "exception", "malformed", "revoked", "profile"]
)
def test_required_callback_failures_never_reach_wire(policy, monkeypatch, failure):
    from agent.fast_mode import begin_turn
    from agent.service_tier_policy import ServiceTierPolicyError

    a = agent()
    callback = policy.manager._service_tier_policies["tiers:required"]
    namespace = callback.__globals__
    if failure == "exception":
        namespace["value"] = "raise"
    if failure == "malformed":
        namespace["value"] = {"service_tier": "priority"}
    if failure == "missing":
        namespace["handle"].release()
    if failure in ("revoked", "profile"):
        begin_turn(a, [])
        if failure == "revoked":
            namespace["handle"].release()
        else:
            monkeypatch.setenv("HERMES_HOME", str(policy.home / "different"))
        with pytest.raises(ServiceTierPolicyError):
            terminal(a, wire(a))
    else:
        with pytest.raises(ServiceTierPolicyError):
            begin_turn(a, [])


def test_wire_model_override_cannot_reuse_included_tier(policy):
    from agent.fast_mode import begin_turn
    from agent.service_tier_policy import ServiceTierPolicyError
    from hermes_cli.plugins import PluginContext, PluginManifest

    ctx = PluginContext(PluginManifest(name="model-modifier"), policy.manager)
    ctx.register_middleware(
        "llm_execution",
        lambda **kw: kw["next_call"](dict(kw["request"], model="unlisted")),
    )
    a = agent()
    begin_turn(a, [])
    with pytest.raises(ServiceTierPolicyError):
        terminal(a, wire(a))
