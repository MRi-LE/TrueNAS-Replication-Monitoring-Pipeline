# Test Plan — TrueNAS Replication CI/CD Pipeline

Tests are ordered from least invasive to most invasive.
Run them in order on first validation; individual tests can be re-run in isolation after that.

---

## Test 1 — Basic watch (smoke test)

**Actions → Replication Monitor → Run workflow**

Expected log output:
```
TrueNAS API query succeeded.
Task [N] 'PUSH example-dataset': FINISHED
  -> OK
...
Done evaluating. 0 task(s) queued for recovery.
All dispatches complete.
```

All enabled tasks should show FINISHED or RUNNING (not FAILED).
No recovery dispatched. No ntfy alerts.

---

## Test 2 — Dispatch failure notification (broken CI_TOKEN)

Temporarily set the `CI_TOKEN` secret to a wrong value, then trigger watch manually.

Expected:
- Log shows `Dispatch failed (HTTP 401)` for each queued task
- ntfy "Recovery Dispatch Failed" notification received
- Exit code is non-zero (job shows red in Actions UI)

Restore the correct token after.

---

## Test 3 — Disabled task guard

Disable any task in TrueNAS.
Trigger **Replication Recovery** manually for that task ID.

Expected:
- ntfy "Recovery Skipped — Task Disabled" received
- Job completes cleanly (green)

Re-enable the task after.

---

## Test 4 — Already-running guard

Manually start a replication task in TrueNAS UI (pick a fast one, e.g. PUSH example-dataset).
While it is RUNNING, trigger **Replication Recovery** for that task ID.

Expected:
- ntfy "Recovery Aborted — Task Already Running" received
- Job completes cleanly (green)

---

## Test 5 — Concurrency block

Trigger **Replication Recovery** for the same task ID twice in rapid succession.

Expected:
- Second run shows "waiting" in Forgejo Actions UI until first completes
- Only one recovery actually executes (the second runs after the first finishes,
  finds the task healthy, and exits cleanly)

---

## Test 6 — Dispatch-to-lock race fix (core regression for this release)

This tests that `RECOVERING_<id>` is written by `watch_dispatch.py` immediately after
a confirmed dispatch, before the recover job starts — closing the window where a second
watch run could re-dispatch the same task.

**Step 1: Force a failed task and let watch dispatch recovery**

Trigger a task failure (see Test 8 for methods, or manually set a task to FAILED state
if your environment supports it). Let the watch run dispatch recovery.

**Step 2: Verify the lock is written immediately**

In Forgejo → Repo → Settings → Secrets and Variables → Actions → Variables:
- `RECOVERING_<task_id>` should exist with a value of `watch-dispatched:<timestamp>`
- This should be visible before the recover job has reached its first step

**Step 3: Trigger watch again while recover is queued or running**

Trigger **Replication Monitor** manually while the recover job is still active.

Expected log:
```
Task [N] 'PUSH example-dataset': FAILED
  -> FAILED but recovery already in progress (RECOVERING_N=watch-dispatched:...) — skipping
Done evaluating. 0 task(s) queued for recovery.
```

No second dispatch. No duplicate recovery job.

**Step 4: Verify cleanup**

After the recover job finishes (success or failure), `RECOVERING_<task_id>` should be gone.
The `if: always()` cleanup step in the recover workflow deletes it unconditionally.

---

## Test 7 — Lock-write failure after successful dispatch

This tests that a failed lock write after a confirmed dispatch fails red and fires ntfy,
rather than silently allowing a duplicate dispatch on the next watch run.

Simulate by temporarily revoking `CI_TOKEN` write access after dispatch succeeds but
before the lock write — or by checking that the code path exists in `watch_dispatch.py`
(look for the `forgejo_set_variable` failure branch: logs to stderr, increments
`failed_dispatches`, fires "Recovery Lock Write Failed" ntfy, exits 1).

This test is primarily a code-review check unless you have a way to inject the failure
in your environment.

---

## Test 8 — Policy variable lifecycle (TASK_POLICY_<id>)

Trigger **Replication Recovery** for a task ID.
While the `set-full` step is running, check Forgejo → Variables.

Expected:
- `TASK_POLICY_<task_id>` exists with a JSON value containing `replicate` and
  `allow_from_scratch` from the original task config

After recovery completes:
- `TASK_POLICY_<task_id>` should be gone (deleted by `set-incremental` on confirmed revert)

This is fast. Verify by checking logs for:
```
Created Forgejo variable: TASK_POLICY_7
...
Deleted Forgejo variable: TASK_POLICY_7
```

---

## Test 9 — Full end-to-end

This requires actually breaking a replication task. The safest method is to temporarily
misconfigure the SSH credentials on the remote TrueNAS (e.g. `YOUR_REMOTE_TRUENAS_IP`) to force
a failure, then let the pipeline detect and recover it.

Expected full sequence:
1. Watch detects FAILED task
2. `watch_dispatch.py` dispatches recover workflow → HTTP 200/204
3. `RECOVERING_<id>` written immediately with `watch-dispatched:<timestamp>`
4. Recover job starts, resolves task name live from TrueNAS
5. ntfy: failure detected ❌
6. Incremental retry 1 → fails (SSH still broken)
7. Incremental retry 2 → fails
8. Incremental retry 3 → fails
9. ntfy: escalating 🚨
10. `TASK_POLICY_<id>` variable created with original policy
11. Switch to full replication, run + poll
12. Restore SSH credentials during the full replication run
13. Full replication succeeds → ntfy ✅
14. Policy restored to incremental, `TASK_POLICY_<id>` deleted
15. `RECOVERING_<id>` deleted (if: always() cleanup)

Restore SSH credentials before step 11 finishes to allow recovery to complete.