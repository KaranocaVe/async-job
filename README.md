# async-job

`async-job` teaches Codex to hand long-running work to a POSIX host watcher and request continuation in the originating session after a terminal observation. It is backend-neutral: the agent generates a small probe adapter for a local process, SSH job, Slurm allocation, Docker container, CI run, or data pipeline.

The watcher is [async-job/scripts/async_watch.py](async-job/scripts/async_watch.py). It accepts a JSON spec and a probe argv. The probe prints a JSON object such as `{"state":"running"}` or `{"state":"succeeded","exit_code":0}`. State is written atomically, probe failures are retried without changing the execution result, and callback delivery is claimed durably before invoking `codex queue --resume`.

## Install

```bash
git clone https://github.com/KaranocaVe/async-job.git
cd async-job
./install.sh
```

The installer copies the skill to `${CODEX_HOME:-$HOME/.codex}/skills/async-job` and does not require a daemon or MCP server.

## Callback semantics

The watcher records `ACCEPTED` when the `codex queue` process exits successfully. That means only that the CLI accepted the message; it does not prove a new Codex turn ran. A failed or interrupted callback is `UNKNOWN` and is not retried automatically. Use an explicit retry only after checking whether the original message reached the thread.

Run the focused tests with:

```bash
python3 -m unittest discover -s tests -v
```
