# Temporal integration tests

Fast fake-provider contract tests run without Temporal. The real environment scenarios are
opt-in:

```shell
RUN_TEMPORAL_INTEGRATION=1 pytest -m integration tests/integration
```

Without an address, the SDK starts a local Temporal dev server and may download the matching CLI
binary. To use an existing namespace, set `TEMPORAL_INTEGRATION_ADDRESS` and optionally
`TEMPORAL_INTEGRATION_NAMESPACE` and `TEMPORAL_INTEGRATION_API_KEY`.

The scenarios exercise shared credentials, atomic quota Workflow reuse, structured 429 handling,
delayed responses, cancellation with an in-flight short Activity, and non-retryable authentication
failure.
