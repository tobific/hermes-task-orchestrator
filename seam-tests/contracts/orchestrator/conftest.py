"""Explicit offline quota data for these contract tests.

No production code consumes this fixture. Native bindings still enforce quota;
quota-specific tests replace the account data and retain default preflight.
Legacy pure scheduler doubles have no account/profile and use an injected host
preflight, just as their workers and route receipts are controlled fixtures.
"""

from datetime import datetime, timezone
import pytest


@pytest.fixture(autouse=True)
def offline_account_quota(request, monkeypatch):
    import agent.account_usage as usage

    def available(provider, **kwargs):
        return usage.AccountUsageSnapshot(
            provider=provider,
            source="offline-test-fixture",
            fetched_at=datetime.now(timezone.utc),
            windows=(
                usage.AccountUsageWindow(label="session", used_percent=20.0),
                usage.AccountUsageWindow(label="weekly", used_percent=20.0),
            ),
        )

    monkeypatch.setattr(usage, "fetch_account_usage", available)
    if not request.node.path.name.startswith("test_quota"):
        from external_orchestrator.scheduler import ExternalScheduler

        monkeypatch.setattr(
            ExternalScheduler,
            "_default_quota_preflight",
            staticmethod(lambda packet: None),
        )
