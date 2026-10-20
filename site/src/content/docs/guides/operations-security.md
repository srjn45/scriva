---
title: Operations & security
description: Deploy listeners safely, secure replication, and recover data without making a bad incident worse.
---

This guide is the short operational checklist. For an incident or a production
change, follow the linked runbooks: they are the canonical procedures.

## Expose listeners deliberately

gRPC, REST, and metrics bind to loopback by default. A non-loopback listener
(`0.0.0.0`, `[::]`, `:port`, or an external address) with neither TLS nor an API
key is refused at startup. Prefer TLS and authentication; bind metrics locally
and scrape through a local agent where possible.

```bash
scriva serve --data ./data \
  --grpc-addr 0.0.0.0:5433 --rest-addr 0.0.0.0:8080 \
  --tls-cert ./server.pem --tls-key ./server-key.pem \
  --api-key "$SCRIVA_API_KEY"
```

For client-certificate authentication, add `--tls-client-ca` and choose
`--tls-client-auth require` (every client certificate is required) or
`verify-if-given` (certificates are verified when supplied). mTLS needs the
server certificate and key too. The Unix socket is local-only (`0600`); its
directory permissions still determine who can reach it.

:::caution[Do not treat the override as a deployment mode]
`--insecure-allow-remote` / `insecure_allow_remote: true` permits an unauthenticated,
unencrypted remote listener and logs `INSECURE`. It is an explicit break-glass
override, not a substitute for TLS or an API key.
:::

## Browser and HTTP limits

CORS is disabled by default. Allow only the origins a browser application uses;
the CLI flag is repeatable:

```bash
scriva serve --cors-allowed-origin https://app.example.com \
  --cors-allowed-origin https://admin.example.com
```

`cors_allowed_origins: ["*"]` is a deliberate opt-in to every origin; it is
not the default. REST request bodies are capped at 32 MiB by default. Set
`max_request_body_bytes` in YAML before accepting larger bulk or `LoadJSONL`
requests, and enforce a matching proxy limit. REST and metrics also enforce
10-second header, 60-second read/write, and 120-second idle timeouts (the write
timeout is lifted for watch streams); slow clients can be disconnected.

## Secure the replication link

For a follower of a remote leader, use TLS plus a credential. A CA file implies
TLS; a client certificate and key together enable mTLS. Pin a server name when
the leader address is not the certificate name, and prefer a dedicated,
unrestricted read-scoped replication key. Snapshot and replication expose the
whole database and are refused for collection-restricted keys.

```bash
scriva serve --data ./follower --replicate-from leader.example.com:5433 \
  --replicate-ca ./leader-ca.pem --replicate-server-name leader.example.com \
  --replicate-cert ./follower.pem --replicate-key ./follower-key.pem \
  --replicate-api-key "$REPLICATION_KEY"
```

A remote plaintext leader is refused unless `--replicate-insecure` is set.
That override cannot be combined with TLS settings and sends any API key in the
clear. Once a follower has used TLS, its data directory records that fact and
refuses a later TLS-to-plaintext start. Removing `.replication-transport` is an
explicit acknowledgement of a deliberate downgrade; investigate the deployment
first rather than deleting it to make a restart pass. Protocol versions are also
negotiated; no common version fails with `REPLICATION_PROTOCOL_UNSUPPORTED`, not
a weaker wire format.

See [Replication & failover](/scriva/guides/replication/) for topology and
promotion limits, and the [secure-defaults runbook](https://github.com/srjn45/scriva/blob/main/docs/operations.md#upgrading-to-secure-server-defaults)
for rollout order.

## Back up and bootstrap safely

Use `scriva-cli backup` for an online, consistent snapshot. Do not unpack an
untrusted archive over a live data directory. Follower bootstrap extraction uses
a private `0700` staging directory and promotes it only after success; it accepts
only regular, allow-listed files at shallow paths, rejects links, duplicate,
absolute and traversal paths, and caps the archive at 1,048,576 entries, 8 GiB
per file, and 64 GiB decompressed. It also drains the gzip trailer, so a
truncated archive fails instead of installing a partial snapshot.

Those are bootstrap protections, not a reason to accept snapshots from an
untrusted source. Keep backup access and encryption keys separate; encrypted
collection segments remain ciphertext, while archive metadata and other
collections may remain readable. A lost key makes an encrypted collection
unrecoverable.

## Verify, repair, then restart

Stop the server before using the offline tools. `verify` is read-only; start
with a full verification and make repair a deliberate, backed-up action.

```bash
scriva verify --data ./data
scriva repair --data ./data --dry-run
scriva repair --data ./data
scriva verify --data ./data
```

`repair` refuses a directory held by another process and creates a verified
backup before its first change. It rebuilds derived indexes and ID counters from
segments; it does not silently resolve conflicting history. `--salvage` is for
damaged segments only: valid records are copied out and originals move to
`quarantine/` with a manifest, never deleted. Keep that quarantine and the
verified backup until the incident is closed. Exit `0` is clean/repaired, `1`
means repairable, `2` signals corruption or a conflict, and `3` is a usage,
access, or lock error.

For the exact stop → backup → repair → reverify decision tree, use the
[index recovery runbook](https://github.com/srjn45/scriva/blob/main/docs/runbook-index-recovery.md).
For batch or transaction coordinator evidence, follow their linked recovery
runbooks rather than attempting segment surgery.

## Next

- [Cross-collection transactions](/scriva/guides/cross-collection-transactions/) — resolve unknown commits; do not blindly retry.
- [Atomic batches & upgrading roots](/scriva/guides/atomic-batches/) — a one-way legacy-root conversion that requires a verified backup.
- [Durability & backup](/scriva/guides/durability-and-backup/) — sync modes and online backup.
- [Configuration](/scriva/reference/configuration/) — listener, TLS, and limit reference.
