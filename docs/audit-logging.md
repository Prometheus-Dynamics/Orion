# Audit Logging

`orion-node` supports an optional JSON Lines audit log for trust and transport-security lifecycle
events.

## Enable It

Set:

- `ORION_NODE_AUDIT_LOG=/path/to/events.jsonl`

Optional queue tuning:

- `ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY`

If `ORION_NODE_AUDIT_LOG` is unset, audit logging is disabled.

## What It Records

Current audit event kinds include:

- transport security failures
- peer enrollment
- peer revocation
- peer identity replacement
- peer TLS pretrust
- local auto-HTTP-TLS rotation

Records are written as one JSON object per line and include:

- `timestamp_ms`
- `node_id`
- `kind`
- `subject`
- `message`

## Runtime Behavior

- The audit log is best-effort and asynchronous.
- The node creates the parent directory if needed.
- If the audit-log queue fills, new audit events are dropped and the runtime emits sampled warnings.
- If the worker disconnects or the file cannot be opened, the runtime surfaces a storage/startup
  error instead of silently ignoring it.

## When To Use It

Enable audit logging when you need:

- an operator-visible trail of trust changes
- evidence of TLS/bootstrap and transport-security failures
- local forensics around peer enrollment and certificate rotation

It is especially useful for release candidates, security-sensitive deployments, and debugging peer
trust issues.

## Example Record

```json
{"timestamp_ms":1710000000000,"node_id":"node-a","kind":"peer_enrolled","subject":"node-b","message":"enrolled peer and trusted configured identity"}
```
