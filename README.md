# TrueNAS Replication CI/CD — Setup Guide

## Overview

This pipeline monitors all TrueNAS replication tasks via the WebSocket JSON-RPC 2.0 API,
sends push notifications via Ntfy on failure, retries automatically, and escalates to full
replication if all retries fail — then auto-reverts back to the original replication policy
on success.

```
replication-watch.yml  (every 15 min)
│
│  Fetch ALL tasks from TrueNAS API (JSON array)
│  Loops over each task independently:
│
├── disabled            → skip silently
├── FINISHED / SUCCESS  → log, do nothing
├── RUNNING / PENDING   → skip (already in progress)
├── UNKNOWN / other     → ntfy warning
└── FAILED / ERROR      → check per-task RECOVERING_<id> variable
                          ├── already recovering → skip
                          └── not recovering → dispatch replication-recover.yml
                                  │  (task_id only — name resolved live in recover)
                                  │  verify HTTP 200/204 response
                                  │  write RECOVERING_<id> = "watch-dispatched:<timestamp>"
                                  │  (written here, before recover job starts,
                                  │   to close the dispatch-to-lock race window)
                                  │
                                  ├── resolve task name from TrueNAS (get-name)
                                  ├── check enabled → disabled: ntfy + abort
                                  ├── check not running → running: ntfy + abort
                                  ├── update RECOVERING_<id> = "recovering" (or similar)
                                  ├── ntfy: failure detected ❌
                                  ├── retry 1 incremental ──→ success → ntfy ✅
                                  ├── retry 2 incremental ──→ success → ntfy ✅
                                  ├── retry 3 incremental ──→ success → ntfy ✅
                                  │   (all 3 failed)
                                  ├── ntfy: escalating 
                                  ├── save original policy → Forgejo variable TASK_POLICY_<id>
                                  ├── switch to full replication
                                  ├── run + poll
                                  │   ├── success → restore policy from variable → ntfy ✅
                                  │   │            delete TASK_POLICY_<id> variable
                                  │   └── failed  → ntfy  CRITICAL + job fails red
                                  └── delete RECOVERING_<id> (always)

replication-summary.yml  (daily at 08:00)
└── Sends one ntfy message summarising all task states
    🟢 low priority if all healthy
    🔴 high priority if any failed
```

---

## 1. Repo Structure

```
your-repo/
├── scripts/
│   ├── truenas_ws.py              ← WebSocket JSON-RPC 2.0 helper
│   ├── watch_evaluate.py          ← task evaluation + RECOVERING check
│   ├── watch_dispatch.py          ← recovery workflow dispatch
│   └── watch_summary.py           ← daily summary builder
└── .forgejo/
    └── workflows/
        ├── replication-watch.yml
        ├── replication-recover.yml
        └── replication-summary.yml
```

Create files directly in the Forgejo UI: click **New File**, type the full path including
folders. Forgejo creates the directories automatically. The `.forgejo` folder with the dot
prefix is created the same way — just type it in the filename field.

---

## 2. TrueNAS Setup

### 2.1 TrueNAS Ports

TrueNAS needs two ports configured:
- **HTTP port** (e.g. `8080`) — standard WebUI
- **HTTPS port** (e.g. `4443`) — **required** for API authentication

Configure under: **System → General → GUI**

> **Critical:** The TrueNAS JSON-RPC 2.0 API automatically and permanently revokes
> API keys used over plain HTTP (`ws://`). Always connect via the HTTPS port (`wss://`).
> A revoked key shows "Revoked: Yes" in the API Keys list and returns `"result": false`
> on auth. Delete it and create a new one — always over HTTPS.

### 2.2 WebSocket API Endpoint

TrueNAS 25.04+ uses JSON-RPC 2.0 over WebSocket. The REST API (`/api/v2.0`) is
deprecated since 25.04 and will be removed in TrueNAS 26.

**Correct endpoint:**
```
wss://YOUR_TRUENAS_IP:YOUR_HTTPS_PORT/api/current
```

**Test connection from TrueNAS Shell:**
```bash
python3 -c "
import websocket, json, ssl

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

ws = websocket.create_connection(
    'wss://YOUR_TRUENAS_IP:YOUR_HTTPS_PORT/api/current',
    timeout=5,
    sslopt={'context': ctx}
)
ws.send(json.dumps({
    'jsonrpc': '2.0',
    'method': 'auth.login_with_api_key',
    'params': ['YOUR_API_KEY'],
    'id': 1
}))
print(ws.recv())
ws.close()
"
```

Expected: `{"jsonrpc": "2.0", "result": true, "id": 1}`

**Common connection errors:**
| Error | Cause | Fix |
|---|---|---|
| `result: false` | Key revoked or invalid | Delete key, create new one, use HTTPS port |
| `TLSV1_UNRECOGNIZED_NAME` | Using `localhost` for TLS | Use actual IP address |
| `WRONG_VERSION_NUMBER` | Port is HTTP not HTTPS | Use the HTTPS port |
| Hangs on recv() | New API doesn't send greeting | Send a message first |
| `404 Not Found` | Wrong port (hitting Nginx) | Use the correct HTTPS port |

### 2.3 API Method Notes (TrueNAS 25.10.2)

**`replication.run`** accepts exactly one parameter — the task ID:
```json
{"method": "replication.run", "params": [42]}
```
There is no second boolean argument in 25.10.2. Passing an extra argument causes the
job to hang at 0.00% indefinitely.

**`replication.update`** accepts task ID and a patch object:
```json
{"method": "replication.update", "params": [42, {"replicate": true, "allow_from_scratch": true}]}
```

**`replication.query`** with filter:
```json
{"method": "replication.query", "params": [[["id", "=", 42]]]}
```

### 2.4 Create a Service Account

Create a dedicated user for CI instead of using `truenas_admin`:

1. Go to **Credentials → Users → Add**
2. Set **Username:** `ci-pipeline` (or any name)
3. Set **Shell:** `nologin`
4. Go to **Credentials → Groups** → find the user's primary group → **Edit**
5. Set **Privileges:** `Local Administrator`
   - Full Admin is required — there is no role scoped to replication tasks only
6. Click **Save**

### 2.5 Generate an API Key

1. Go to **top-right account icon → API Keys → Add**
2. Link it to your `ci-pipeline` user
3. Copy the key immediately — shown only once
4. Verify it shows **Revoked: No** in the list

> If a key shows Revoked: Yes immediately after creation, you used it over HTTP.
> Delete it, create a new one, and only use it over the HTTPS port.

### 2.6 Task State Values

| State | Meaning |
|---|---|
| `FINISHED` | Completed successfully |
| `SUCCESS` | Completed successfully (job-level) |
| `RUNNING` | Currently in progress |
| `PENDING` | Queued, about to start |
| `FAILED` | Failed — triggers recovery |
| `ERROR` | Error — triggers recovery |

### 2.7 Replication Policy Fields

The helper script reads and preserves these fields before switching to full replication:

| API Field | UI Label | Notes |
|---|---|---|
| `replicate` | Full Filesystem Replication | Saved to Forgejo variable, restored after |
| `allow_from_scratch` | Replication from scratch | Saved to Forgejo variable, restored after |

All other task settings (`recursive`, `readonly`, `retention_policy`, etc.) are never
modified by the pipeline — they remain exactly as configured in TrueNAS.

---

## 3. Ntfy Setup

### 3.1 Install Ntfy on TrueNAS

1. Go to **Apps → Discover Apps** → search `ntfy` → **Install**
2. Set a port that isn't already in use
   - Check used ports: `ss -tlnp`
   - Port 8080 is often taken by TrueNAS WebUI or Nginx
3. Note the assigned port after install

### 3.2 Install Ntfy on your Phone

- **Android:** Search "ntfy" on Google Play Store
- **iOS:** Search "ntfy" on App Store

### 3.3 Subscribe to your Topic

1. Open the Ntfy app → tap **+**
2. Set **Server URL:** `http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT`
3. Set **Topic:** `truenas-alerts` (or any name)
4. Tap **Subscribe**

> The Ntfy web UI shows a browser warning about HTTPS — this only affects the browser
> interface, not the phone app. Ignore it.

### 3.4 Test Ntfy

```bash
curl -d "Test notification" http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT/truenas-alerts
```

> **Important:** The `NTFY_URL` secret must not have a trailing slash.
> `http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT` ✅ — `http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT/` ❌
> A trailing slash causes a double-slash in the URL (`//topic`) which returns
> `301 Moved Permanently` — curl doesn't follow it, silently dropping the notification.

---

## 4. Forgejo Runner Setup

### 4.1 Install

Install `forgejo-runner` via **Apps → Discover Apps** on TrueNAS.

### 4.2 How the Runner Executes Jobs

When the runner picks up a workflow job, it spins up a **fresh Docker container** to
execute the steps. That container is isolated by default and gets its own temporary
network (`WORKFLOW-xxxxxxxx`).

This container needs to do two things:
1. **Clone the repo from Forgejo** — requires network access to the Forgejo container
2. **Reach the TrueNAS API** — requires network access to `YOUR_TRUENAS_IP:HTTPS_PORT`

By default, neither works because the job container is on an isolated network.

### 4.3 Runner Network Configuration

The solution is to attach job containers to the same Docker network as Forgejo
(`YOUR_FORGEJO_NETWORK_NAME`). This network:
- Is not internal-only (`Internal: false`) so it can reach the host (TrueNAS IP)
- Contains the Forgejo container so checkout works
- Has a route to TrueNAS so the WebSocket API calls work

**Get the network ID:**
```bash
sudo docker network inspect YOUR_FORGEJO_NETWORK_NAME | grep '"Id"'
```

> Use the full network ID (not the name) in the config — the name may not resolve
> correctly inside the runner container.

Also add the network in **Apps → forgejo-runner → Edit → Network Configuration:**
- Network: `YOUR_FORGEJO_NETWORK_NAME`
- Container: `forgejo-runner`

### 4.4 Runner Config File

Location: `/mnt/YOUR_POOL/forgejo/runner/config.yaml`

This file is mounted as `/data/config.yaml` inside the runner container and read on startup.

**Working minimal config:**
```yaml
runner:
  labels:
    - "truenas-runner"

container:
  network: "YOUR_NETWORK_ID"
  valid_volumes:
    - "**"
```

- `labels` — must match `runs-on:` in both workflow files exactly
- `network` — the Forgejo Docker network ID (not name)
- `valid_volumes: ["**"]` — allows the runner to mount workspace volumes into job containers

> **Note:** The `default_image` config setting is ignored by this runner version.
> Job containers always use `node:22-bookworm`. Python and pip are installed
> at runtime via `apt-get` in the workflow instead.

> **Note:** The `:host` label suffix is stripped by the TrueNAS App UI and does not
> persist. Docker container mode is used instead.

After editing the config file, restart the runner:
```bash
sudo docker restart YOUR_RUNNER_CONTAINER_NAME
```

### 4.5 Verify Runner Label

Go to `/admin/actions/runners` in Forgejo.
The label shown must exactly match the `runs-on:` value in both workflow files.

### 4.6 Scheduled vs Manual Runs

**Important:** Scheduled runs (`cron:`) use the workflow version from the commit at
the time the schedule was queued — this can be an old commit. Always use
**Actions → Run workflow** (manual trigger) when testing changes.

---

## 5. Python Dependencies in Workflows

The runner uses `node:22-bookworm` which has Python 3.11 but no pip.
All workflow files install dependencies at runtime:

```yaml
      - name: Install dependencies
        run: |
          apt-get update -qq
          apt-get install -y -qq python3-pip
          python3 -m pip install websocket-client --break-system-packages
```

- `apt-get` works because `node:22-bookworm` is Debian-based
- `--break-system-packages` is required on Debian 12+ for pip installs outside a venv
- Use `python3 -m pip` not `pip` or `pip3`

---

## 6. Forgejo Token

Used to trigger `replication-recover.yml`, read/write variables, and dispatch workflows.

1. Go to Forgejo → **User Settings → Applications → Generate Token**
2. Name it `ci-workflow-dispatch`
3. Set permissions:
   - **repository:** Write (Schreiben)
   - Everything else: No access (Kein Zugriff)
4. Copy the token → save as `CI_TOKEN` secret

---

## 7. Forgejo Secrets

Go to: **Repo → Settings → Secrets and Variables → Actions → Secrets tab → Add Secret**

| Secret | Example value | Notes |
|---|---|---|
| `TRUENAS_HOST` | `YOUR_TRUENAS_IP` | IP only — no protocol, no port |
| `TRUENAS_PORT` | `4443` | HTTPS port only |
| `TRUENAS_API_KEY` | `1-xxxxxxxxxxxxx` | API key, must show Revoked: No |
| `NTFY_URL` | `http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT` | Base URL — no trailing slash |
| `NTFY_TOPIC` | `truenas-alerts` | Ntfy topic name |
| `CI_URL` | `http://YOUR_FORGEJO_IP:YOUR_FORGEJO_PORT` | Forgejo URL with port |
| `CI_TOKEN` | `your-forgejo-token` | Token with repository write permission |
| `CI_REPO` | `youruser/your-repo` | Format: owner/repo |

**Secret naming rules in Forgejo:**
- Only letters, digits, underscores
- Must not start with `FORGEJO_`, `GITEA_`, `GITHUB_`, or a digit
- Forgejo auto-converts to uppercase
- This is why we use `CI_URL` instead of `FORGEJO_URL`

---

## 8. Forgejo Variables

There are no variables to create manually. All variables are fully managed by the pipeline.

**Automatically managed variables (do not create manually):**

| Variable | Created by | Value | Deleted by | Purpose |
|---|---|---|---|---|
| `RECOVERING_<id>` | `watch_dispatch.py` — immediately after confirmed dispatch (HTTP 200/204) | `watch-dispatched:<UTC timestamp>` (e.g. `watch-dispatched:20250429T141200Z`) | recover workflow (`if: always()`, cleanup step) | Guards against duplicate dispatch on the next watch run. Written before the recover job starts to close the dispatch-to-lock race window. Any string value is treated as "already recovering" — the exact value is not checked, only presence. One variable per task ID. |
| `TASK_POLICY_<id>` | recover workflow `set-full` step | JSON: `{"replicate": true/false, "allow_from_scratch": true/false}` | recover workflow `set-incremental` step (only on confirmed successful revert) | Stores the original replication policy before switching to full replication. Only exists during an active full replication escalation. |

> If a `TASK_POLICY_<id>` variable remains after a failed recovery, it means the revert
> step did not complete. Check TrueNAS manually to verify the task's replication settings,
> then delete the variable from Forgejo.

> **Forgejo v15 variable API field name:** The GET response for a variable uses `"data"`
> as the value field, not `"value"`. Example: `{"name": "TASK_POLICY_7", "data": "{...}"}`.
> Code reading variables must use `response["data"]`, not `response["value"]`.

---

## 9. Workflow Dispatch Inputs

`replication-recover.yml` uses `workflow_dispatch` inputs. Forgejo requires explicit
`type: string` on each input or the UI shows "Ungültiger Eingabetyp":

```yaml
on:
  workflow_dispatch:
    inputs:
      task_id:
        description: 'TrueNAS replication task ID'
        required: true
        type: string
      task_name:
        description: 'Task name (optional fallback — live name is resolved from TrueNAS at runtime)'
        required: false
        type: string
```

The watch workflow dispatches with `task_id` only. The recover workflow resolves the
task name live from TrueNAS via `get-name` at startup, falling back to the provided
`task_name` input if given, then `"Task <id>"` as last resort. Name lookup failure
never blocks recovery.

---

## 10. Concurrency

Both workflows use Forgejo's `concurrency:` block to prevent overlapping runs:

```yaml
# replication-watch.yml — only one monitor at a time
concurrency:
  group: replication-monitor
  cancel-in-progress: false

# replication-recover.yml — one recovery per task, parallel across tasks
concurrency:
  group: replication-recover-${{ inputs.task_id }}
  cancel-in-progress: false
```

`cancel-in-progress: false` means a new run waits for the current one to finish
rather than cancelling it. This prevents losing a recovery mid-run.

---

## 11. Recovery Guards

| Guard | Where | Behavior |
|---|---|---|
| Task disabled | watch + recover | Watch skips; recover sends ntfy and aborts |
| Task already RUNNING/PENDING | recover | Sends ntfy warning and aborts cleanly |
| Recovery already in progress | watch | Checks per-task `RECOVERING_<id>` variable — skips if any string value is present |
| Original policy preserved | recover | Saved to Forgejo variable `TASK_POLICY_<id>` before patching — survives container restarts |
| Policy restore failure | recover | Exits with error and ntfy warning — task left as-is, no silent wrong defaults |
| `RECOVERING_<id>` always deleted | recover | `if: always()` ensures `RECOVERING_<id>` is deleted even on failure |
| Dispatch verified | watch | HTTP status checked — ntfy alert if dispatch fails |
| API errors distinguished | recover | Exit codes checked at every step — API failures don't masquerade as task states |
| `get-state` exit codes | recover | Exit 0 = non-terminal state (RUNNING, FINISHED, etc.); exit 2 = FAILED or ERROR (state printed to stdout first); exit 1 = API or argument error. Step 2 must handle exit 2 explicitly as "proceed with recovery". |
| `TASK_NAME` unbound guard | recover | `TASK_NAME` is written to `$GITHUB_ENV` by the resolve step. If unbound in a subsequent step under `set -u`, the step exits immediately with no output. Guard: `TASK_NAME="${TASK_NAME:-Task ${TASK_ID}}"` as first line after `set -uo pipefail`. |
| Runner implicit `set -e` | recover | Forgejo runner v12 injects `set -e` around every `run:` block. Any `truenas_ws.py` call that exits non-zero will abort the shell before `EXIT_CODE=$?` runs — even though the step uses `set -uo pipefail` (not `-e`) explicitly. Fix: bracket every such call with `set +e` / `set -e`. Do NOT use `\|\| true` — that makes `$?` always 0 and loses the real exit code. |

---

## 12. Tunable Parameters

In `replication-recover.yml`:

| Variable | Default | Phase | Description |
|---|---|---|---|
| `MAX_RETRIES` | 3 | Incremental | Number of incremental retry attempts before escalating |
| `WAIT_BETWEEN` | 120s | Incremental | Wait between retry attempts |
| `POLL_INTERVAL` | 30s | Incremental | Status poll interval per attempt |
| `MAX_WAIT` | 3600s | Incremental | Max wait per incremental attempt (1h) |
| `POLL_INTERVAL` | 60s | Full | Status poll interval during full replication |
| `MAX_WAIT` | 86400s | Full | Max wait for full replication (24h) |

In `replication-summary.yml`:

| Setting | Default | Description |
|---|---|---|
| `cron` | `0 8 * * *` | Daily summary time (08:00) |

---

## 13. Testing

### Test watch workflow
**Actions → Replication Monitor → Run workflow**

Expected output in logs:
```
TrueNAS API query succeeded.
[{"id": 42, "name": "PUSH example-dataset", "state": "FINISHED", "enabled": true}, ...]
Task evaluation complete.
Task [42] 'PUSH example-dataset': FINISHED
  -> OK
...
Done evaluating. 0 task(s) queued for recovery.
All dispatches complete.
```

### Test recovery workflow
**Actions → Replication Recovery → Run workflow**

Fill in inputs:
- `task_id`: any valid ID from your TrueNAS task list
- `task_name`: optional — leave blank to use live name resolution from TrueNAS

### Test daily summary
**Actions → Replication Daily Summary → Run workflow**

Should receive one ntfy message summarising all task states.

### Test disabled task guard
Disable a task in TrueNAS → trigger recovery for that task ID.
Expected: "Recovery Skipped — Task Disabled" ntfy, job completes cleanly.

### Test already-running guard
Trigger recovery while a task is RUNNING.
Expected: "Recovery Aborted — Task Already Running" ntfy, job completes cleanly.

### Test Ntfy
```bash
curl -d "Test alert" http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT/truenas-alerts
```

### Test WebSocket helper directly
```bash
# List all tasks as JSON
TRUENAS_HOST=YOUR_IP TRUENAS_PORT=YOUR_HTTPS_PORT TRUENAS_API_KEY=YOUR_KEY \
  python3 scripts/truenas_ws.py --action list-tasks

# Resolve a task name
TRUENAS_HOST=YOUR_IP TRUENAS_PORT=YOUR_HTTPS_PORT TRUENAS_API_KEY=YOUR_KEY \
  python3 scripts/truenas_ws.py --action get-name --id YOUR_TASK_ID
```

### Test watch helper scripts directly
```bash
# First populate the task state file
TRUENAS_HOST=YOUR_IP TRUENAS_PORT=YOUR_HTTPS_PORT TRUENAS_API_KEY=YOUR_KEY \
  python3 scripts/truenas_ws.py --action list-tasks > /tmp/task_states.json

# Run the evaluator
CI_URL=http://YOUR_FORGEJO_IP:YOUR_FORGEJO_PORT CI_TOKEN=YOUR_TOKEN CI_REPO=youruser/your-repo \
  NTFY_URL=http://YOUR_TRUENAS_IP:YOUR_NTFY_PORT NTFY_TOPIC=truenas-alerts \
  TRUENAS_HOST=YOUR_IP \
  python3 scripts/watch_evaluate.py

# Build the daily summary
python3 scripts/watch_summary.py
cat /tmp/summary_result.json
```

---

## 14. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| API key auto-revoked | Used `ws://` (HTTP) instead of `wss://` | Always use HTTPS port |
| `result: false` on auth | Key is revoked | Delete, create new key, use HTTPS |
| Job stuck at 0.00% forever | Extra argument passed to `replication.run` | Use `[task_id]` only, no second argument |
| Workflow uses old commit | Scheduled runs cache workflow version | Use manual trigger when testing |
| `No module named pip` | `node:22-bookworm` has no pip | Use `apt-get install python3-pip` first |
| `externally-managed-environment` | Debian 12 pip restriction | Add `--break-system-packages` |
| `Name or service not known` | Wrong `TRUENAS_HOST` value | Use IP only, no protocol or port |
| Ntfy notification not received | Trailing slash in `NTFY_URL` secret | Remove trailing slash from secret |
| Ntfy curl returns 301 | Double slash in URL (`//topic`) | Remove trailing slash from `NTFY_URL` |
| `Cannot find: node in PATH` | Runner in host mode without Node.js | Use Docker container mode |
| Workflow stuck waiting for runner | Runner label mismatch | Check label in `/admin/actions/runners` |
| `Ungültiger Eingabetyp` on inputs | Missing `type: string` on dispatch inputs | Add `type: string` to each input |
| `404 Not Found` on WebSocket | Wrong port (hitting Nginx) | Use the correct HTTPS port |
| Container can't reach TrueNAS | Job container on wrong network | Set network ID in runner config.yaml |
| Container can't clone repo | Job container can't reach Forgejo | Set network ID in runner config.yaml |
| Runner ignores config.yaml | Config not at correct path | Verify: `/mnt/YOUR_POOL/forgejo/runner/config.yaml` |
| `default_image` setting ignored | Runner version limitation | Install deps via apt-get in workflow instead |
| Recovery triggered twice | Manual duplicate trigger | Concurrency block + RUNNING state check prevents re-execution |
| Step exits 2 with no output | `TASK_NAME` unbound under `set -u` | Add `TASK_NAME="${TASK_NAME:-Task ${TASK_ID}}"` as first line of run block after `set -uo pipefail` |
| `Could not parse policy variable: 'value'` | Forgejo v15 GET returns `"data"` field not `"value"` | Use `response["data"]` in all variable GET code paths (`truenas_ws.py` and `watch_evaluate.py`) |
| Step fails with exit 3 (or other non-zero) from `truenas_ws.py` before `EXIT_CODE=$?` runs | Forgejo runner v12 injects implicit `set -e` — shell aborts on non-zero exit before capture | Bracket the Python call with `set +e` / `set -e`. Never use `\|\| true` on calls whose exit code you inspect — it masks the real code to 0. |
| `POST variable failed: HTTP 405` | POSTing to collection URL (`/variables`) instead of named resource | Use `POST /variables/{name}` with `{"value": value}` — this is a upsert in Forgejo v15 |
| Task stuck PENDING after trigger | Task was disabled when triggered | Re-enable task in TrueNAS |
| `TASK_POLICY_<id>` variable remains | Revert step failed or was skipped | Check TrueNAS task settings manually, then delete variable from Forgejo |
| Task left in full replication mode | set-incremental failed | Check `TASK_POLICY_<id>` variable, run set-incremental manually or fix via TrueNAS UI |
| Recovery permanently blocked | Stale `RECOVERING_<id>` variable | See stale-lock runbook below |

### Stale `RECOVERING_<id>` — Manual Deletion Runbook

**When this happens:**
`watch_dispatch.py` writes `RECOVERING_<id>` immediately after a confirmed dispatch
(HTTP 200/204). If the recover job never starts — or starts and crashes before its
`if: always()` cleanup step runs — the variable is left set. Every subsequent watch
run sees the lock and skips re-dispatch, so recovery is permanently blocked for that
task until the variable is deleted.

**How to detect it:**
- Watch logs show `FAILED but recovery already in progress (RECOVERING_<id>=watch-dispatched:...)` on repeated runs
- No corresponding recover job is active or queued in Forgejo Actions for that task ID

**How to fix it:**
1. Go to **Forgejo → Repo → Settings → Secrets and Variables → Actions → Variables tab**
2. Find the variable named `RECOVERING_<id>` (e.g. `RECOVERING_7`)
3. Confirm there is no active or queued recover job for that task ID in **Actions**
4. Delete the variable
5. The next watch run (or a manual trigger) will re-evaluate the task and dispatch recovery if still needed

**Reading the lock value:**
A value of `watch-dispatched:<timestamp>` means the lock was written by the watch side
and the recover job never claimed it. A value like `recovering` or similar means the
recover job started but did not clean up. Both are safe to delete manually once you have
confirmed no active recover job is running for that task.

---

## 15. Backlog — Bugs, Improvements, and Design Ideas

Tracked here so nothing is lost between sessions. Items are grouped by priority.

### Done

| Item | File | Description |
|---|---|---|
| Fail-open edge case in lock check | `watch_evaluate.py` | `forgejo_get_variable()` previously returned `data.get("data")` directly. If Forgejo returned a dict without a `"data"` field, the function returned `None` — treated by the caller as "lock confirmed absent," triggering dispatch. Fixed: result is now checked with `isinstance(value, str)`; anything other than a string (including `None`) returns `LOCK_UNKNOWN` with a warning. The three-state contract (str / None / LOCK_UNKNOWN) is now fully enforced at the source. |


### Good later — performance

| Item | File | Description |
|---|---|---|
| Custom runner image | Runner infra | Every workflow run installs `python3-pip` and `websocket-client` from scratch (~9-10s). Prebaking these into a custom runner image based on `node:22-bookworm` would eliminate the apt install step across all three workflows (watch, recover, summary). Adds image maintenance burden — do after pipeline is stable. |

### Good later — correctness / hygiene

| Item | File | Description |
|---|---|---|
| Fixed WebSocket request ID | `truenas_ws.py` | Every `call()` uses `req_id=2`. Safe today because each call opens a fresh connection with no multiplexing, but wrong in principle. Replace with a generated or incrementing ID. Low urgency — not a latent bug under current connection model. |
| `sys.exit()` in helper functions | `truenas_ws.py` | Helper functions (`connect`, `call`, `get_task`, Forgejo CRUD) call `sys.exit(1)` directly, making them untestable in isolation. Refactor to raise exceptions and handle them in `main()`. Large change, lowest urgency. |

### Good later — design

| Item | File | Description |
|---|---|---|
| Job-level polling | `truenas_ws.py` + `replication-recover.yml` | Current recover workflow polls `replication.query` task state, not the specific job ID returned by `replication.run`. This is a heuristic — it can misread a stale state from a previous run. Fix: add `--action poll-job --job-id <N>` to `truenas_ws.py` using `core.get_jobs`, and capture the job ID printed by `run-task` in the recover workflow. The current approach works in practice; this closes a theoretical race window. |
| Shared `ntfy()` and Forgejo CRUD helpers | `watch_evaluate.py`, `watch_dispatch.py` | Both scripts duplicate `ntfy()`. Forgejo variable CRUD is split across `truenas_ws.py` (CLI contract, `sys.exit` on error) and `watch_evaluate.py` (LOCK_UNKNOWN sentinel contract). Factoring these into a shared module is reasonable once the contracts are clearly documented — but do not consolidate the two variable-read contracts without preserving their intentional difference. |
