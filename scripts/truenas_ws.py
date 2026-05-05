#!/usr/bin/env python3
"""
TrueNAS WebSocket JSON-RPC 2.0 helper for CI workflows.

Usage:
    python3 truenas_ws.py --action list-tasks
    python3 truenas_ws.py --action get-name --id 4
    python3 truenas_ws.py --action run-task --id 4
    python3 truenas_ws.py --action get-state --id 4
    python3 truenas_ws.py --action check-enabled --id 4
    python3 truenas_ws.py --action set-full --id 4
    python3 truenas_ws.py --action set-incremental --id 4

    list-tasks:
        Prints a JSON array of task objects to stdout.
        Each object has keys: id, name, state, enabled.
        Prints [] if no tasks exist. Never prints NO_TASKS.

    get-name:
        Prints the task name for the given --id.
        Exit code 1 if task not found.

    check-enabled:
        Prints True or False.
        Exit code 3 if task is disabled.

    set-full:
        Reads the original values of 'replicate' and 'allow_from_scratch'
        from TrueNAS, stores them in a Forgejo repo variable
        TASK_POLICY_<id> (e.g. TASK_POLICY_4), then patches the task
        to full replication. Requires CI_URL, CI_TOKEN, CI_REPO env vars.

    set-incremental:
        Reads the original policy from the Forgejo variable TASK_POLICY_<id>,
        restores those values to TrueNAS, then deletes the variable.
        Falls back to a safe warning (no change) if variable is missing.
        Requires CI_URL, CI_TOKEN, CI_REPO env vars.

Environment variables (required):
    TRUENAS_HOST      e.g. YOUR_TRUENAS_IP
    TRUENAS_PORT      e.g. 4443
    TRUENAS_API_KEY   your API key

Environment variables (required for set-full / set-incremental):
    CI_URL            e.g. http://YOUR_FORGEJO_IP:YOUR_FORGEJO_PORT
    CI_TOKEN          Forgejo API token with repo write access
    CI_REPO           e.g. youruser/your-repo

Exit codes:
    0  success
    1  error / unexpected state
    2  task state is FAILED or ERROR
    3  task is disabled
"""

import argparse
import json
import os
import ssl
import sys
import urllib.request
import urllib.error
import websocket


# ── Forgejo variable helpers ──────────────────────────────────────────────────

def forgejo_get_variable(ci_url: str, ci_token: str, ci_repo: str, name: str) -> dict | None:
    """
    Read a Forgejo repo variable. Returns parsed JSON dict or None if not found.
    Exits with code 1 on non-404 HTTP errors, network errors, decode errors, or
    unexpected response shape.

    Contract difference vs watch_evaluate.py's forgejo_get_variable():
      - This version calls sys.exit(1) on any error because it runs inside a
        CLI tool where aborting the process is the correct response.
      - watch_evaluate.py's version returns a LOCK_UNKNOWN sentinel instead,
        so the watch workflow can distinguish "confirmed absent" from "uncertain"
        and fail closed on the recovery lock check.
      These two contracts are intentionally different. Do not consolidate them.
    """
    url = f"{ci_url}/api/v1/repos/{ci_repo}/actions/variables/{name}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {ci_token}"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if not isinstance(data, dict):
                print(f"ERROR: GET variable {name}: unexpected response type "
                      f"{type(data).__name__}: {data!r}", file=sys.stderr)
                sys.exit(1)
            return data
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"ERROR: GET variable {name} failed: HTTP {e.code}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"ERROR: GET variable {name} failed: network error: {e.reason}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: GET variable {name} failed: could not decode response: {e}", file=sys.stderr)
        sys.exit(1)


def forgejo_set_variable(ci_url: str, ci_token: str, ci_repo: str, name: str, value: str) -> None:
    """Create or update a Forgejo repo variable."""
    base_url = f"{ci_url}/api/v1/repos/{ci_repo}/actions/variables/{name}"
    data = json.dumps({"name": name, "value": value}).encode()
    headers = {
        "Authorization": f"Bearer {ci_token}",
        "Content-Type": "application/json",
    }

    # Try PUT (update) first
    req = urllib.request.Request(base_url, data=data, headers=headers, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=10):
            print(f"Updated Forgejo variable: {name}")
            return
    except urllib.error.HTTPError as e:
        if e.code not in (404, 422):
            print(f"ERROR: PUT variable {name} failed: HTTP {e.code}", file=sys.stderr)
            sys.exit(1)
    except urllib.error.URLError as e:
        print(f"ERROR: PUT variable {name} failed: network error: {e.reason}", file=sys.stderr)
        sys.exit(1)

    # Variable doesn't exist — POST (create)
    req = urllib.request.Request(base_url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10):
            print(f"Created Forgejo variable: {name}")
    except urllib.error.HTTPError as e:
        print(f"ERROR: POST variable {name} failed: HTTP {e.code}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"ERROR: POST variable {name} failed: network error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def forgejo_delete_variable(ci_url: str, ci_token: str, ci_repo: str, name: str) -> None:
    """Delete a Forgejo repo variable. Ignores 404."""
    url = f"{ci_url}/api/v1/repos/{ci_repo}/actions/variables/{name}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {ci_token}"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            print(f"Deleted Forgejo variable: {name}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            pass  # Already gone
        else:
            print(f"WARNING: DELETE variable {name} failed: HTTP {e.code}", file=sys.stderr)
    except urllib.error.URLError as e:
        # Delete is best-effort — warn but do not exit 1.
        # The variable will remain in Forgejo as a manual recovery reference,
        # which is acceptable; the workflow will log the warning.
        print(f"WARNING: DELETE variable {name} failed: network error: {e.reason}", file=sys.stderr)


def policy_variable_name(task_id: int) -> str:
    return f"TASK_POLICY_{task_id}"


# ── TrueNAS WebSocket helpers ─────────────────────────────────────────────────

def connect(host: str, port: str, api_key: str) -> websocket.WebSocket:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    url = f"wss://{host}:{port}/api/current"
    try:
        ws = websocket.create_connection(url, timeout=15, sslopt={"context": ctx})
    except ssl.SSLError as e:
        print(f"ERROR: SSL error connecting to {url}: {e}", file=sys.stderr)
        sys.exit(1)
    except websocket.WebSocketException as e:
        print(f"ERROR: WebSocket connection to {url} failed: {e}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"ERROR: Could not connect to {url}: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        ws.send(json.dumps({
            "jsonrpc": "2.0",
            "method": "auth.login_with_api_key",
            "params": [api_key],
            "id": 1,
        }))
        resp = json.loads(ws.recv())
    except websocket.WebSocketException as e:
        print(f"ERROR: WebSocket error during authentication: {e}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"ERROR: OS error during authentication: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Could not decode authentication response: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(resp, dict):
        print(f"ERROR: Unexpected authentication response type: {type(resp).__name__}: {resp!r}", file=sys.stderr)
        sys.exit(1)
    if not resp.get("result"):
        print("ERROR: Authentication failed. Check your API key.", file=sys.stderr)
        sys.exit(1)

    return ws


def call(ws: websocket.WebSocket, method: str, params: list, req_id: int = 2) -> object:
    try:
        ws.send(json.dumps({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": req_id,
        }))
    except websocket.WebSocketException as e:
        print(f"ERROR: WebSocket send failed for method {method!r}: {e}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"ERROR: OS error sending method {method!r}: {e}", file=sys.stderr)
        sys.exit(1)

    while True:
        try:
            msg = json.loads(ws.recv())
        except websocket.WebSocketException as e:
            print(f"ERROR: WebSocket recv failed for method {method!r}: {e}", file=sys.stderr)
            sys.exit(1)
        except OSError as e:
            print(f"ERROR: OS error receiving response for method {method!r}: {e}", file=sys.stderr)
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"ERROR: Could not decode response for method {method!r}: {e}", file=sys.stderr)
            sys.exit(1)

        if not isinstance(msg, dict):
            print(f"ERROR: Unexpected response type for method {method!r}: {type(msg).__name__}: {msg!r}", file=sys.stderr)
            sys.exit(1)
        if msg.get("id") == req_id:
            if "error" in msg:
                print(f"ERROR: API error: {msg['error']}", file=sys.stderr)
                sys.exit(1)
            return msg.get("result")


def get_task(ws: websocket.WebSocket, task_id: int) -> dict:
    tasks = call(ws, "replication.query", [[["id", "=", task_id]]])
    if not tasks:
        print(f"ERROR: Task {task_id} not found", file=sys.stderr)
        sys.exit(1)
    return tasks[0]


def get_forgejo_env() -> tuple[str, str, str]:
    """Read and validate Forgejo env vars. Exits on missing."""
    ci_url   = os.environ.get("CI_URL", "").rstrip("/")
    ci_token = os.environ.get("CI_TOKEN", "")
    ci_repo  = os.environ.get("CI_REPO", "")
    if not ci_url or not ci_token or not ci_repo:
        print("ERROR: CI_URL, CI_TOKEN, and CI_REPO must be set for this action.", file=sys.stderr)
        sys.exit(1)
    return ci_url, ci_token, ci_repo


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TrueNAS WS JSON-RPC 2.0 helper")
    parser.add_argument("--action", required=True,
        choices=["list-tasks", "get-name", "run-task", "get-state", "check-enabled",
                 "set-full", "set-incremental"])
    parser.add_argument("--id", type=int, help="Task ID (required for most actions)")
    args = parser.parse_args()

    host    = os.environ.get("TRUENAS_HOST", "")
    port    = os.environ.get("TRUENAS_PORT", "4443")
    api_key = os.environ.get("TRUENAS_API_KEY", "")

    if not host or not api_key:
        print("ERROR: TRUENAS_HOST and TRUENAS_API_KEY must be set.", file=sys.stderr)
        sys.exit(1)

    ws = connect(host, port, api_key)

    try:
        # ── list-tasks ────────────────────────────────────────────────────────
        if args.action == "list-tasks":
            tasks = call(ws, "replication.query", [[]])
            result = []
            for t in (tasks or []):
                task_id = t.get("id")
                result.append({
                    "id":      task_id,
                    "name":    t.get("name", f"Task {task_id}"),
                    "state":   (t.get("state") or {}).get("state", "UNKNOWN"),
                    "enabled": t.get("enabled", True),
                })
            print(json.dumps(result))

        # ── get-name ──────────────────────────────────────────────────────────
        elif args.action == "get-name":
            if not args.id:
                print("ERROR: --id required for get-name", file=sys.stderr)
                sys.exit(1)
            task = get_task(ws, args.id)
            print(task.get("name", f"Task {args.id}"))

        # ── check-enabled ─────────────────────────────────────────────────────
        elif args.action == "check-enabled":
            if not args.id:
                print("ERROR: --id required for check-enabled", file=sys.stderr)
                sys.exit(1)
            task    = get_task(ws, args.id)
            enabled = task.get("enabled", True)
            print(enabled)
            if not enabled:
                sys.exit(3)

        # ── get-state ─────────────────────────────────────────────────────────
        elif args.action == "get-state":
            if not args.id:
                print("ERROR: --id required for get-state", file=sys.stderr)
                sys.exit(1)
            task  = get_task(ws, args.id)
            state = (task.get("state") or {}).get("state", "UNKNOWN")
            print(state)
            if state in ("FAILED", "ERROR"):
                sys.exit(2)

        # ── run-task ──────────────────────────────────────────────────────────
        elif args.action == "run-task":
            if not args.id:
                print("ERROR: --id required for run-task", file=sys.stderr)
                sys.exit(1)
            result = call(ws, "replication.run", [args.id])
            print(f"Triggered task {args.id}, job ID: {result}")

        # ── set-full ──────────────────────────────────────────────────────────
        elif args.action == "set-full":
            if not args.id:
                print("ERROR: --id required for set-full", file=sys.stderr)
                sys.exit(1)

            ci_url, ci_token, ci_repo = get_forgejo_env()
            var_name = policy_variable_name(args.id)

            # Read original policy from TrueNAS
            task = get_task(ws, args.id)
            original_replicate          = task.get("replicate", False)
            original_allow_from_scratch = task.get("allow_from_scratch", False)

            print(f"Original policy: replicate={original_replicate}, "
                  f"allow_from_scratch={original_allow_from_scratch}")

            # Save to Forgejo variable — survives container restarts
            policy_json = json.dumps({
                "replicate": original_replicate,
                "allow_from_scratch": original_allow_from_scratch,
            })
            forgejo_set_variable(ci_url, ci_token, ci_repo, var_name, policy_json)

            # Patch task to full replication
            print(f"Switching task {args.id} to full replication...")
            call(ws, "replication.update", [args.id, {
                "replicate": True,
                "allow_from_scratch": True,
            }])
            print(f"Task {args.id} switched to full replication.")

        # ── set-incremental ───────────────────────────────────────────────────
        elif args.action == "set-incremental":
            if not args.id:
                print("ERROR: --id required for set-incremental", file=sys.stderr)
                sys.exit(1)

            ci_url, ci_token, ci_repo = get_forgejo_env()
            var_name = policy_variable_name(args.id)

            # Read original policy from Forgejo variable
            var = forgejo_get_variable(ci_url, ci_token, ci_repo, var_name)

            if var:
                try:
                    original = json.loads(var["data"])
                    replicate          = original["replicate"]
                    allow_from_scratch = original["allow_from_scratch"]
                    print(f"Restoring original policy from Forgejo variable {var_name}: "
                          f"replicate={replicate}, allow_from_scratch={allow_from_scratch}")
                except (KeyError, json.JSONDecodeError) as e:
                    print(f"ERROR: Could not parse policy variable {var_name}: {e}", file=sys.stderr)
                    print("WARNING: Task left in current state — manual check required.", file=sys.stderr)
                    sys.exit(1)
            else:
                # Variable missing — do NOT silently apply wrong defaults
                # Warn loudly and exit without changing TrueNAS
                print(f"ERROR: Policy variable {var_name} not found in Forgejo.", file=sys.stderr)
                print("WARNING: Cannot restore original policy — task left in current state.", file=sys.stderr)
                print("WARNING: Manual check required to verify task replication settings.", file=sys.stderr)
                sys.exit(1)

            # Restore original policy to TrueNAS
            call(ws, "replication.update", [args.id, {
                "replicate": replicate,
                "allow_from_scratch": allow_from_scratch,
            }])
            print(f"Task {args.id} reverted to original replication policy.")

            # Clean up Forgejo variable
            forgejo_delete_variable(ci_url, ci_token, ci_repo, var_name)

    finally:
        ws.close()


if __name__ == "__main__":
    main()