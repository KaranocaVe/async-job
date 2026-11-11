# Watcher contract

The agent creates one spec file per job with mode `0600` and an owner-only parent directory. Keep credentials out of argv and persisted metadata; use the host's credential store or existing environment. Use absolute paths so the detached watcher does not depend on a later working directory.

```json
{
  "job_id": "opaque-backend-id",
  "backend": "slurm",
  "backend_ref": "cluster:12345",
  "submission_state": "ACCEPTED",
  "thread_id": "CODEX_THREAD_ID captured before detaching",
  "probe": ["/absolute/path/probe-job.sh"],
  "probe_cwd": "/absolute/path/to/workdir",
  "log_paths": ["/absolute/path/to/job.log"],
  "state_file": "/absolute/path/to/async-job-state.json",
  "poll_interval": 30,
  "max_interval": 300,
  "probe_timeout": 45,
  "callback_timeout": 30
}
```

`submission_state` is `NOT_REQUIRED` for work started locally, `ACCEPTED`
after a definitive backend response, or `SUBMISSION_UNKNOWN` when a submit
response was lost. For an ambiguous submission, use a stable lookup key as the
job id and make the probe reconcile it with the backend. A recognized probe
state moves submission state to `RECONCILED`; `unknown` or a probe error does
not. The watcher never resubmits an ambiguous submission.

The probe receives `ASYNC_JOB_ID` in its environment and prints exactly one JSON object to stdout. Exit nonzero or malformed JSON means an observation error, not a failed job. Example responses:

```json
{"state":"running","raw_state":"R"}
{"state":"succeeded","raw_state":"COMPLETED","exit_code":0}
{"state":"failed","raw_state":"FAILED","exit_code":1}
```

The watcher normalizes the `state` field, stores bounded metadata and a hash of the probe response, and never forwards probe output or logs in the callback. It uses a lock next to the state file to prevent concurrent watchers. State writes are atomic and mode `0600`. A scheduler adapter must report `succeeded` only when the accounting record confirms success and the exit code is zero; missing or nonzero exit codes are not success evidence.

Run in the foreground for diagnosis:

```bash
python3 /path/to/async-job/scripts/async_watch.py --spec /absolute/path/job-spec.json --once
```

Run continuously under a host supervisor. A basic POSIX shell launch is:

```bash
nohup python3 /path/to/async-job/scripts/async_watch.py \
  --spec /absolute/path/job-spec.json \
  </dev/null >>/absolute/path/async-job-watcher.log 2>&1 &
watcher_pid=$!
```

Do not end the turn until the supervisor or process is independently visible and the state file has been initialized. `nohup` does not provide survival across host restart, sleep policy, or every execution sandbox; use `launchd`, `systemd`, or an equivalent when that lifetime matters.

After persisting a terminal state, the watcher invokes:

```bash
codex queue --resume --thread "$CODEX_THREAD_ID" --message "async-job event <event-id> reached <terminal-state>; inspect the persisted state for this task and continue."
```

The event id is a generated UUID; only the normalized terminal enum is included. `ACCEPTED` means the command returned zero, not that an agent resumed. A crash while `IN_FLIGHT` becomes `UNKNOWN` on restart and is not resent. An explicit retry may be made with `--retry-delivery` only after checking whether the original message already reached the thread.
