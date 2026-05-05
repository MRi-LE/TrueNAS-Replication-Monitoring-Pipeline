#!/usr/bin/env python3
"""
Dispatch replication-recover.yml for each task in /tmp/tasks_to_dispatch.json.

Reads:  /tmp/tasks_to_dispatch.json  [{id: N}, ...] (IDs only)
        Task names are NOT passed — recover workflow resolves the name live
        from TrueNAS via get-name to avoid stale-name notifications.

Environment variables required:
    CI_URL, CI_TOKEN, CI_REPO   Forgejo API access
    NTFY_URL, NTFY_TOPIC        Ntfy notification target
"""

import datetime
import json
import os
import sys
import urllib.request
import urllib.error

ci_url     = os.environ["CI_URL"].rstrip("/")
ci_token   = os.environ["CI_TOKEN"]
ci_repo    = os.environ["CI_REPO"]
ntfy_url   = os.environ["NTFY_URL"]
ntfy_topic = os.environ["NTFY_TOPIC"]


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


def forgejo_set_variable(name, value):
    """
    Create or update a Forgejo repo variable.

    Returns True on success, False on any error.

    POST /variables/{name} with {"value": value} is a upsert in Forgejo v15:
    creates if absent, updates if present, always returns 204.
    The collection POST (/variables) does not exist — 405.
    PUT /variables/{name} is update-only — 404 if variable does not exist.
    """
    url = f"{ci_url}/api/v1/repos/{ci_repo}/actions/variables/{name}"
    headers = {
        "Authorization": f"Bearer {ci_token}",
        "Content-Type": "application/json",
    }
    payload = json.dumps({"value": value}).encode()
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 201, 204)
    except urllib.error.HTTPError as e:
        print(f"ERROR: POST variable {name} failed: HTTP {e.code}", file=sys.stderr)
        return False
    except (urllib.error.URLError, OSError) as e:
        print(f"ERROR: POST variable {name} failed: {e}", file=sys.stderr)
        return False


try:
    to_dispatch = json.loads(open("/tmp/tasks_to_dispatch.json").read())
except (json.JSONDecodeError, OSError) as e:
    print(f"ERROR: Could not read dispatch list: {e}", file=sys.stderr)
    sys.exit(1)

if not isinstance(to_dispatch, list):
    print(f"ERROR: Dispatch list is not a list (got {type(to_dispatch).__name__})", file=sys.stderr)
    sys.exit(1)

if not to_dispatch:
    print("No tasks to dispatch.")
    sys.exit(0)

dispatch_url = (
    f"{ci_url}/api/v1/repos/{ci_repo}"
    f"/actions/workflows/replication-recover.yml/dispatches"
)
headers = {
    "Authorization": f"Bearer {ci_token}",
    "Content-Type": "application/json",
}

failed_dispatches = 0

for entry in to_dispatch:
    if not isinstance(entry, dict) or "id" not in entry:
        print(f"WARNING: Skipping malformed dispatch entry: {entry!r}", file=sys.stderr)
        failed_dispatches += 1
        continue
    task_id = entry["id"]
    if not (isinstance(task_id, int) and not isinstance(task_id, bool)) and not (isinstance(task_id, str) and task_id.isdigit()):
        print(f"WARNING: Skipping entry with invalid task_id type: {task_id!r}", file=sys.stderr)
        failed_dispatches += 1
        continue

    payload = json.dumps({
        "ref": "main",
        "inputs": {
            "task_id": str(task_id),
        }
    }).encode()

    req = urllib.request.Request(dispatch_url, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
    except urllib.error.HTTPError as e:
        status = e.code
    except (urllib.error.URLError, OSError) as e:
        print(f"ERROR: Dispatch for task {task_id} failed: network error: {e}", file=sys.stderr)
        failed_dispatches += 1
        ntfy("Recovery Dispatch Failed", "high",
             f"Task ID {task_id} failed but recovery workflow could not be triggered: {e}")
        continue

    if status in (200, 204):
        print(f"Task {task_id}: recovery dispatched (HTTP {status})")
        # Write the lock immediately so the next watch run skips re-dispatch
        # even while the recovery job is still queued and hasn't reached Step 3.
        # Value includes timestamp for easier stale-lock debugging.
        lock_value = f"watch-dispatched:{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        lock_name  = f"RECOVERING_{task_id}"
        if forgejo_set_variable(lock_name, lock_value):
            print(f"Task {task_id}: lock written ({lock_name}={lock_value})")
        else:
            print(f"ERROR: Dispatch succeeded but lock write failed for task {task_id} "
                  f"({lock_name})", file=sys.stderr)
            failed_dispatches += 1
            ntfy("Recovery Lock Write Failed", "high",
                 f"Task ID {task_id}: recovery was dispatched but the watch-side lock "
                 f"({lock_name}) could not be written. Duplicate dispatch is possible "
                 f"on the next watch run. Check Forgejo API.")
    else:
        print(f"ERROR: Dispatch for task {task_id} failed: HTTP {status}", file=sys.stderr)
        failed_dispatches += 1
        ntfy("Recovery Dispatch Failed", "high",
             f"Task ID {task_id} failed but recovery workflow could not be triggered (HTTP {status}).")

total = len(to_dispatch)
print(f"Dispatch complete. {total - failed_dispatches}/{total} succeeded.")
if failed_dispatches > 0:
    sys.exit(1)