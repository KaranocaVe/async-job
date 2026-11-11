import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WATCHER = ROOT / "async-job" / "scripts" / "async_watch.py"


def executable(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env python3\n" + textwrap.dedent(body), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class AsyncWatchTests(unittest.TestCase):
    def make_spec(self, directory: Path, probe: Path, codex: Path) -> tuple[Path, Path]:
        state = directory / "state.json"
        spec = directory / "spec.json"
        spec.write_text(
            json.dumps(
                {
                    "job_id": "job-1",
                    "backend": "test",
                    "backend_ref": "opaque-ref",
                    "thread_id": "thread-1",
                    "probe": [str(probe)],
                    "state_file": str(state),
                    "codex_command": [str(codex)],
                    "poll_interval": 0.01,
                    "probe_timeout": 2,
                    "callback_timeout": 2,
                }
            ),
            encoding="utf-8",
        )
        return spec, state

    def run_watcher(self, spec: Path, *extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(WATCHER), "--spec", str(spec), "--once", *extra],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_terminal_state_is_persisted_before_callback_and_message_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            captured = directory / "callback.json"
            executable(probe, 'import json; print(json.dumps({"state":"succeeded","raw_state":"COMPLETED; rm -rf /","exit_code":0}))')
            executable(
                codex,
                """
                import json, pathlib, sys
                saved = json.loads(pathlib.Path(sys.argv[2]).read_text())
                pathlib.Path(sys.argv[1]).write_text(json.dumps({"args":sys.argv[3:],"state":saved["execution_state"]}))
                """,
            )
            # The fake command receives its capture path as the first argv item,
            # while the watcher appends the queue arguments after it.
            spec, state = self.make_spec(directory, probe, codex)
            value = json.loads(spec.read_text())
            value["codex_command"] = [str(codex), str(captured), str(state)]
            spec.write_text(json.dumps(value))
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["execution_state"], "SUCCEEDED")
            self.assertEqual(saved["delivery_state"], "ACCEPTED")
            self.assertEqual(saved["log_paths"], [])
            recorded = json.loads(captured.read_text())
            self.assertEqual(recorded["state"], "SUCCEEDED")
            message = recorded["args"][-1]
            self.assertIn("SUCCEEDED", message)
            self.assertNotIn("COMPLETED; rm -rf /", message)
            self.assertEqual(stat.S_IMODE(state.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(spec.stat().st_mode), 0o600)

    def test_probe_failure_retries_without_marking_job_failed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            executable(probe, "raise SystemExit(7)")
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["execution_state"], "UNKNOWN")
            self.assertEqual(saved["observation_state"], "RETRYING")
            self.assertEqual(saved["delivery_state"], "PENDING")

    def test_ambiguous_submission_is_reconciled_by_a_known_probe(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            executable(probe, 'import json; print(json.dumps({"state":"running"}))')
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            value = json.loads(spec.read_text())
            value["submission_state"] = "SUBMISSION_UNKNOWN"
            value["log_paths"] = [str(directory / "job.log")]
            spec.write_text(json.dumps(value))
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["submission_state"], "RECONCILED")
            self.assertEqual(saved["execution_state"], "RUNNING")
            self.assertEqual(saved["log_paths"], value["log_paths"])

    def test_unknown_probe_does_not_resolve_ambiguous_submission(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            executable(probe, 'import json; print(json.dumps({"state":"unknown"}))')
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            value = json.loads(spec.read_text())
            value["submission_state"] = "SUBMISSION_UNKNOWN"
            spec.write_text(json.dumps(value))
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["submission_state"], "SUBMISSION_UNKNOWN")
            self.assertEqual(saved["execution_state"], "UNKNOWN")
            self.assertEqual(saved["delivery_state"], "PENDING")

    def test_unknown_delivery_is_not_resent_without_explicit_retry(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            calls = directory / "calls"
            executable(probe, 'import json; print(json.dumps({"state":"failed","exit_code":3}))')
            executable(
                codex,
                f"""
                import pathlib, sys
                p = pathlib.Path({str(calls)!r})
                p.write_text(str(int(p.read_text() or '0') + 1) if p.exists() else '1')
                print('callback secret', file=sys.stderr)
                raise SystemExit(9)
                """,
            )
            spec, state = self.make_spec(directory, probe, codex)
            first = self.run_watcher(spec)
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertEqual(json.loads(state.read_text())["delivery_state"], "UNKNOWN")
            self.assertNotIn("callback secret", state.read_text())
            second = self.run_watcher(spec)
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(calls.read_text(), "1")
            third = self.run_watcher(spec, "--retry-delivery")
            self.assertEqual(third.returncode, 0, third.stderr)
            self.assertEqual(calls.read_text(), "2")

    def test_invalid_probe_json_is_an_observation_error(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            executable(probe, 'print("not-json")')
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["observation_state"], "RETRYING")
            self.assertIn("valid JSON", saved["last_error"])

    def test_restart_with_in_flight_delivery_marks_unknown_without_resending(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            calls = directory / "calls"
            executable(probe, 'import json; print(json.dumps({"state":"succeeded"}))')
            executable(
                codex,
                f"""
                import pathlib
                p = pathlib.Path({str(calls)!r})
                p.write_text('called')
                """,
            )
            spec, state = self.make_spec(directory, probe, codex)
            saved = {
                "schema_version": 1,
                "event_id": "0b2204c0-a62c-4804-96cf-cfd4a6dcf9f1",
                "job_id": "job-1",
                "thread_id": "thread-1",
                "execution_state": "SUCCEEDED",
                "observation_state": "HEALTHY",
                "watcher_status": "COMPLETE",
                "delivery_state": "IN_FLIGHT",
                "probe_attempts": 1,
                "callback_attempts": 1,
            }
            state.write_text(json.dumps(saved), encoding="utf-8")
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(state.read_text())["delivery_state"], "UNKNOWN")
            self.assertFalse(calls.exists())

    def test_continuous_watcher_polls_until_terminal_and_exits(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            counter = directory / "counter"
            executable(
                probe,
                f"""
                import json, pathlib
                p = pathlib.Path({str(counter)!r})
                count = int(p.read_text()) + 1 if p.exists() else 1
                p.write_text(str(count))
                print(json.dumps({{"state":"succeeded" if count >= 2 else "running"}}))
                """,
            )
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            value = json.loads(spec.read_text())
            value["poll_interval"] = 0.05
            spec.write_text(json.dumps(value))
            process = subprocess.Popen(
                [sys.executable, str(WATCHER), "--spec", str(spec)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, stderr or stdout)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["execution_state"], "SUCCEEDED")
            self.assertEqual(saved["delivery_state"], "ACCEPTED")
            self.assertGreaterEqual(int(counter.read_text()), 2)

    def test_probe_timeout_kills_descendant_processes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            marker = directory / "child-survived"
            executable(
                probe,
                f"""
                import subprocess, sys, time
                subprocess.Popen([sys.executable, "-c", "import pathlib,time; time.sleep(0.8); pathlib.Path({str(marker)!r}).write_text('alive')"])
                time.sleep(5)
                """,
            )
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            value = json.loads(spec.read_text())
            value["probe_timeout"] = 0.15
            spec.write_text(json.dumps(value))
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(state.read_text())["observation_state"], "RETRYING")
            time.sleep(1)
            self.assertFalse(marker.exists())

    def test_probe_output_is_capped(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            executable(probe, 'print("x" * 1000000)')
            executable(codex, "raise SystemExit(0)")
            spec, state = self.make_spec(directory, probe, codex)
            result = self.run_watcher(spec)
            self.assertEqual(result.returncode, 0, result.stderr)
            saved = json.loads(state.read_text())
            self.assertEqual(saved["observation_state"], "RETRYING")
            self.assertIn("output exceeded", saved["last_error"])

    def test_non_finite_poll_override_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            probe = directory / "probe.py"
            codex = directory / "codex.py"
            executable(probe, 'import json; print(json.dumps({"state":"running"}))')
            executable(codex, "raise SystemExit(0)")
            spec, _state = self.make_spec(directory, probe, codex)
            result = subprocess.run(
                [sys.executable, str(WATCHER), "--spec", str(spec), "--poll-interval", "nan"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("finite and positive", result.stderr)


if __name__ == "__main__":
    unittest.main()
