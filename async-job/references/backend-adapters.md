# Backend adapters

The watcher owns lifecycle persistence, polling, backoff, and callback delivery. A job-specific adapter owns submission and turns backend output into the probe JSON contract in [watcher-contract.md](watcher-contract.md).

## Local process

Record the PID and a process-start fingerprint. A PID by itself is unsafe because it can be reused. Probe `/proc/<pid>` on Linux or `kill -0` plus a recorded start time on macOS; report `running` until the child has exited, then report `succeeded` or `failed` with its exit code.

## SSH

Keep the watcher on the Codex host. Reconnect for each probe and treat timeout, refused connection, DNS, host-key, and authentication errors as observation errors with the last known execution state preserved. The remote job id and command must be persisted separately from SSH transport state.

## Slurm or another scheduler

Persist the scheduler job id immediately after a definitive submission response. Query pending/running state first, then query the accounting endpoint for terminal state. A lost SSH response is `SUBMISSION_UNKNOWN`; query by an idempotency marker or job name before submitting again. Do not map a transient missing accounting record to failure without a grace period. Report `succeeded` only when the scheduler confirms completion and the exit code is zero; missing or nonzero exit codes are not success evidence.

## Docker and CI

Probe the container or run API by immutable id, not by a mutable name. Keep API errors in observation state and preserve the last execution state. Do not put response bodies or logs in the callback message.

All adapters should use bounded output, redact credentials, and return a stable JSON object even when the backend is temporarily unavailable.
