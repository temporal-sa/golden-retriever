# Local verification report — 2026-07-18

This report records development verification only. It is not production-scale evidence and
does not close the production adapter, exported-history, namespace-capability, or deployment
gates in `docs/architecture-production-readiness.md`.

## Environment

- Virtual-environment Python: 3.13.5
- Temporal Python SDK: 1.30.0
- Temporal CLI: 1.7.2
- Ephemeral test server advertised by the CLI: 1.31.1
- pytest: 8.4.2
- Ruff: 0.15.22

## Results

| Verification | Result |
|---|---:|
| Ruff lint | clean |
| Ruff format check | 65 files formatted |
| Python compileall | clean |
| Default test suite | 134 passed, 5 skipped |
| Real Temporal integration and full topology | 3 passed |
| Opt-in Temporal load harness | 1 passed |

The default skips were the three opt-in Temporal integration tests, the opt-in load test,
and the exported-history replay test. The opt-in Temporal and load tests were then run
separately and passed. The replay test remains skipped because the greenfield repository was
not supplied with representative production histories.

The load run used 30 signal operations and a synthetic two-scope fairness workload with 20
large-scope and 10 small-scope operations. It validates the harness and local execution path;
it does not establish production SLOs or capacity.
