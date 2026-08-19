#!/usr/bin/env python3
"""Import each service using only the paths its container actually has.

A guardian that imports a module from services/detector works fine on a
developer machine, where everything is on sys.path, and then crash-loops in
its container. That failure is invisible to unit tests and to the eval
harness, both of which run with every directory importable.

This reproduces the container's import environment exactly: the shared
platform package plus the one service directory, nothing else.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# service directory -> modules its entrypoint pulls in
SERVICES = {
    "fleet": ["simulation"],
    "detector": ["detection"],
    "chaos": ["scenarios"],
    "guardian": ["planner_offline", "planner_llm", "policy", "budget",
                 "memory", "agent", "actuators"],
    "api": [],
    "mcp_kafka_ops": [],
}

CHILD = """
import sys
sys.path[:0] = [{platform!r}, {service!r}]
import importlib
for name in {modules!r}:
    importlib.import_module(name)
print("ok")
"""


def main() -> int:
    failures: list[str] = []
    for service, modules in SERVICES.items():
        service_dir = ROOT / "services" / service
        if not service_dir.exists():
            continue
        code = CHILD.format(
            platform=str(ROOT / "platform"),
            service=str(service_dir),
            modules=modules,
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"  ok    {service}")
        else:
            last = result.stderr.strip().splitlines()[-1] if result.stderr else "?"
            print(f"  FAIL  {service}: {last}")
            failures.append(service)

    if failures:
        print(f"\n{len(failures)} service(s) would crash-loop in their container: "
              f"{', '.join(failures)}")
        return 1
    print("\nall services import cleanly under container paths")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
