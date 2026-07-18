# Local verification snapshot — 2026-07-18

This file records one local development run so contributors can understand the repository's
tested baseline. It is not production-scale evidence and does not satisfy the adapter, target
namespace, representative-history, telemetry, security, or deployment requirements in the
[production-readiness guide](../docs/architecture-production-readiness.md).

## Environment

- Virtual-environment Python: 3.13.5
- Temporal Python SDK: 1.30.0
- System Temporal CLI: 1.7.2
- SDK-downloaded Temporal CLI / ephemeral server: 1.8.0 / 1.31.2
- pytest: 8.4.2
- Ruff: 0.15.22

## Results

| Verification | Result |
|---|---:|
| Ruff lint | clean |
| Ruff format check | clean |
| Python compileall | clean |
| Default test suite | 147 passed, 6 skipped |
| Temporal integration suite, including full topology | 5 passed |
| Checked-in local `RootSyncWorkflow` history replay | 1 passed |
| Opt-in Temporal load harness | 1 passed |

The default skips were the five opt-in Temporal integration tests and one opt-in load test. Those
suites were enabled and run separately for this snapshot. The default suite replays
`artifacts/histories/root-sync-replay-smoke.json`.

The full-topology scenario uses a scripted provider response and a staged document. It traverses
root, user, resource, page, file, and document workflows and verifies the generation-fenced
repository commit.

The load scenario used 30 Signal operations and a synthetic two-scope fairness workload with 20
large-scope and 10 small-scope operations. It validates the harness and local execution path; it
does not establish production capacity, fairness guarantees, or SLOs.

Run the commands in the root [README](../README.md#test-and-validate) to reproduce the current
suite. Results may differ when dependency, SDK, CLI, or server versions change.
