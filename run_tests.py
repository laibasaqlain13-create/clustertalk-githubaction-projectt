"""
Run every ClusterTalk test suite and report a single pass/fail summary.

    python run_tests.py

Each test module is a plain script that prints PASS lines and exits
non-zero if any assertion fails. This runner just invokes them all with
the current interpreter (use the project venv) and aggregates the result.
"""

# run_tests.py
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable

# (directory, filename) — ordered cheapest/most-fundamental first.
SUITES = [
    ("protocol", "test_framing.py"),
    ("node", "test_session_state.py"),
    ("node", "test_persistence.py"),
    ("node", "test_session_manager.py"),
    ("node", "test_server.py"),
    ("node", "test_mesh.py"),
    ("node", "test_session_recovery.py"),
    ("node", "test_dynamic_registration.py"),
    ("ingress", "test_lb.py"),
]


def main() -> int:
    failed: list[str] = []
    started = time.time()

    for directory, filename in SUITES:
        label = f"{directory}/{filename}"
        print(f"\n{'=' * 60}\n  {label}\n{'=' * 60}")
        result = subprocess.run([PY, "-u", filename], cwd=str(ROOT / directory))
        if result.returncode != 0:
            failed.append(label)

    elapsed = time.time() - started
    print(f"\n{'=' * 60}")
    if failed:
        print(f"  {len(failed)} SUITE(S) FAILED in {elapsed:.1f}s:")
        for label in failed:
            print(f"    - {label}")
        print("=" * 60)
        return 1

    print(f"  ALL {len(SUITES)} TEST SUITES PASSED in {elapsed:.1f}s")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
