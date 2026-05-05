#!/usr/bin/env python3
"""
Build the daily replication summary from TrueNAS task states.

Reads:  /tmp/task_states.json  (output of truenas_ws.py --action list-tasks)
Writes: /tmp/summary_result.json  {message: str, has_failed: "yes"|"no"|"no_tasks"}

has_failed values:
    "yes"      — one or more enabled tasks are FAILED/ERROR
    "no"       — all enabled tasks are healthy (or running/unknown)
    "no_tasks" — task list is empty; no tasks configured on TrueNAS
"""

import json
import sys

try:
    tasks = json.loads(open("/tmp/task_states.json").read())
except (json.JSONDecodeError, OSError) as e:
    print(f"ERROR: Could not parse task list: {e}", file=sys.stderr)
    sys.exit(1)

if not isinstance(tasks, list):
    print(f"ERROR: Unexpected task list format: {type(tasks).__name__}", file=sys.stderr)
    sys.exit(1)

# Empty array — no tasks configured at all
if not tasks:
    result = {
        "message": "WARNING: No replication tasks found on TrueNAS.",
        "has_failed": "no_tasks",
    }
    json.dump(result, open("/tmp/summary_result.json", "w"))
    print("No tasks found — wrote no_tasks result.")
    sys.exit(0)

ok       = []
failed   = []
running  = []
disabled = []
other    = []

for t in tasks:
    task_id = t.get("id", "?")
    name    = t.get("name", f"Task {task_id}")
    state   = t.get("state", "UNKNOWN")
    enabled = t.get("enabled", True)

    if not enabled:
        disabled.append(f"  (disabled) {name}")
        continue

    if state in ("FINISHED", "SUCCESS"):
        ok.append(f"  OK  {name}")
    elif state in ("FAILED", "ERROR"):
        failed.append(f"  FAILED  {name}")
    elif state in ("RUNNING", "PENDING"):
        running.append(f"  RUNNING  {name}")
    else:
        other.append(f"  UNKNOWN  {name} ({state})")

# enabled_total excludes disabled — the fraction in the header
# counts only tasks that are expected to run.
enabled_total = len(ok) + len(failed) + len(running) + len(other)

sections = []
if failed:
    sections.append("FAILED:\n" + "\n".join(failed))
if running:
    sections.append("Running:\n" + "\n".join(running))
if ok:
    sections.append("Healthy:\n" + "\n".join(ok))
if disabled:
    sections.append("Disabled (not monitored):\n" + "\n".join(disabled))
if other:
    sections.append("Unknown:\n" + "\n".join(other))

status = "Issues detected" if failed else "All healthy"
header = f"{status} -- {len(ok)}/{enabled_total} enabled tasks OK"
if disabled:
    header += f" ({len(disabled)} disabled)"

message    = header + "\n\n" + "\n\n".join(sections)
has_failed = "yes" if failed else "no"

result = {"message": message, "has_failed": has_failed}
json.dump(result, open("/tmp/summary_result.json", "w"))
print(f"Summary built. has_failed={has_failed}")