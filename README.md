# MRi-LE-TrueNAS-Replication-Monitoring-Pipeline
This pipeline monitors all TrueNAS replication tasks via the WebSocket JSON-RPC 2.0 API, sends push notifications via Ntfy on failure, retries automatically, and escalates to full replication if all retries fail — then auto-reverts back to the original replication policy on success.
