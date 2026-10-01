# Notice and provenance

This package is released under the MIT License (see `LICENSE`).

| Files | Origin | License / notice |
|---|---|---|
| `patches/*.patch` | Changes to Hermes Agent at upstream commit 8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96. The unchanged context lines and the files they modify are Hermes Agent code, Copyright (c) 2025 Nous Research. The added lines are the work of this package. | MIT; the upstream notice "Copyright (c) 2025 Nous Research" is kept in `LICENSE`. |
| `plugin/external_orchestrator/` | Written for this package as a Hermes user plugin. It imports Hermes modules at run time; it does not copy them. | MIT, Copyright (c) 2026 tobific |
| `plugin-tests/`, `seam-tests/`, `run_tests.py` | Written for this package to test the plugin and the patches. The tests import Hermes modules at run time. `seam-tests/service-tier/tests/fixtures/` is a synthetic example policy written for these tests. | MIT, Copyright (c) 2026 tobific |
| `docs/`, `README.md`, `TESTING.md`, `NOTICE.md` | Written for this package. | MIT, Copyright (c) 2026 tobific |

Hermes Agent: https://github.com/NousResearch/hermes-agent (MIT). This package is not affiliated with or endorsed by
Nous Research.

No third-party source code other than Hermes Agent is included. The plugin and the tests use only the Python standard
library and packages that Hermes itself depends on (see TESTING.md).
