#!/usr/bin/env python3
"""
Evaluate TrueNAS replication task states and determine which need recovery.

Reads:  /tmp/task_states.json  (output of truenas_ws.py --action list-tasks)
Writes: /tmp/tasks_to_dispatch.json  [{id: N}, ...] for tasks needing recovery

Environment variables required:
    CI_URL, CI_TOKEN, CI_REPO   Forgejo API access
    NTFY_URL, NTFY_TOPIC        Ntfy notification target
    TRUENAS_HOST                Used in notification text only
"""

import json
import os
import sys
import urllib.request
import urllib.error

ci_url       = os.environ["CI_URL"].rstrip("/")
ci_token     = os.environ["CI_TOKEN"]
ci_repo      = os.environ["CI_REPO"]
ntfy_url     = os.environ["NTFY_URL"]
ntfy_topic   = os.environ["NTFY_TOPIC"]
truenas_host = os.environ["TRUENAS_HOST"]

# Sentinel returned by forgejo_get_variable() when the lock state cannot be
# determined due to a network, decode, or unexpected API error.
# Distinct from None (404 → lock confirmed absent) and "true" (lock confirmed set).
#
# Contract difference vs truenas_ws.py's forgejo_get_variable():
#   - This implementation uses a 3-state return (value | None | LOCK_UNKNOWN)
#     so callers can distinguish "confirmed absent" from "uncertain."
#     Uncertain → skip dispatch + warn. Never fail open on the recovery lock.
#   - truenas_ws.py's version calls sys.exit(1) on non-404 errors because it
#     runs inside a CLI tool where aborting the process is the right response.
#   These two contracts are intentionally different. Do not consolidate them.
LOCK_UNKNOWN = object()


def ntfy(title, priority, message):
    import subprocess
    subprocess.run([
        "curl", "-sf",
        "-H", f"Title: {title}",
        "-H", f"Priority: {priority}",
        "-H", "Tags: warning,truenas",
        "-d", message,
        f"{ntfy_url}/{ntfy_topic}"
    ], check=False)


def forgejo_get_variable(name):
    """
    Read a Forgejo repo variable. Three possible return values:

        str   — variable exists; returns the value string
        None  — variable does not exist (HTTP 404); lock confirmed absent
        LOCK_UNKNOWN — network error, decode error, unexpected HTTP status,
                       or malformed response; lock state uncertain

    Callers must treat LOCK_UNKNOWN as fail-closed: skip dispatch and warn.
    Never treat an uncertain lock state as "no lock set."
    """
    url = f"{ci_url}/api/v1/repos/{ci_repo}/actions/variables/{name}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {ci_token}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if not isinstance(data, dict):
                print(f"WARNING: GET variable {name}: unexpected response type "
                      f"{type(data).__name__}", file=sys.stderr)
                return LOCK_UNKNOWN
            value = data.get("data")
            if not isinstance(value, str):
                print(f"WARNING: GET variable {name}: 'data' field missing or not a string "
                      f"(got {type(value).__name__})", file=sys.stderr)
                return LOCK_UNKNOWN
            return value
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"WARNING: GET variable {name} failed: HTTP {e.code}", file=sys.stderr)
        return LOCK_UNKNOWN
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        print(f"WARNING: GET variable {name} failed: {e}", file=sys.stderr)
        return LOCK_UNKNOWN


try:
    tasks = json.loads(open("/tmp/task_states.json").read())
except (json.JSONDecodeError, OSError) as e:
    print(f"ERROR: Could not parse task list: {e}", file=sys.stderr)
    sys.exit(1)

if not isinstance(tasks, list):
    print(f"ERROR: Unexpected task list format: {type(tasks).__name__}", file=sys.stderr)
    sys.exit(1)

if not tasks:
    print("No replication tasks found on TrueNAS.")
    ntfy("No Replication Tasks Found", "default",
         f"No replication tasks found on TrueNAS at {truenas_host}.")
    json.dump([], open("/tmp/tasks_to_dispatch.json", "w"))
    sys.exit(0)

to_dispatch = []

for t in tasks:
    task_id   = t.get("id")
    task_name = t.get("name", f"Task {task_id}")
    state     = t.get("state", "UNKNOWN")
    enabled   = t.get("enabled", True)

    if not enabled:
        print(f"Task [{task_id}] '{task_name}': disabled — skipping")
        continue

    print(f"Task [{task_id}] '{task_name}': {state}")

    if state in ("FINISHED", "SUCCESS"):
        print("  -> OK")
    elif state in ("RUNNING", "PENDING"):
        print("  -> Running/pending, skipping")
    elif state in ("FAILED", "ERROR"):
        recovering = forgejo_get_variable(f"RECOVERING_{task_id}")
        if recovering is LOCK_UNKNOWN:
            print(f"  -> FAILED but lock state uncertain — skipping dispatch")
            ntfy(
                "Recovery Not Dispatched — Lock Check Failed",
                "high",
                f"Task '{task_name}' (ID {task_id}) is FAILED but recovery was NOT dispatched "
                f"\u2014 could not verify Forgejo lock state (RECOVERING_{task_id}) due to API error. "
                f"Check whether recovery is already running before dispatching manually."
            )
        elif isinstance(recovering, str):
            print(f"  -> FAILED but recovery already in progress (RECOVERING_{task_id}={recovering!r}) — skipping")
        else:
            print("  -> FAILED \u2014 queuing for recovery dispatch")
            to_dispatch.append({"id": task_id})
    else:
        print(f"  -> Unknown state: {state}")
        ntfy("Unknown Replication State", "default",
             f"Task '{task_name}' (ID {task_id}) has unexpected state: {state}")

json.dump(to_dispatch, open("/tmp/tasks_to_dispatch.json", "w"))
print(f"Done evaluating. {len(to_dispatch)} task(s) queued for recovery.")