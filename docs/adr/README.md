# Architecture Decision Records

Decisions are recorded as ADRs (see
[0000](./0000-record-architecture-decisions.md) for the process). They are
append-mostly: when a decision changes, a new ADR supersedes the old one and
explains the displacement.

| # | Title | Status |
|---|---|---|
| [0000](./0000-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0001](./0001-sync-architecture.md) | Sync architecture: single-ordered-stream over a Durable Object | Accepted (amended by 0002; D11 builder superseded by 0014; D13 base class reframed as a mixin by 0015) |
| [0002](./0002-adversarial-review-corrections.md) | Corrections from adversarial review: ordering, shaping, retention | Accepted (C5 retention refined by 0009) |
| [0003](./0003-atomic-cursor-fetch.md) | Cursor load-more is one atomic fetch, not two | Accepted (naming amended by 0005) |
| [0004](./0004-after-commit-hook.md) | Side effects go in a fire-and-forget `afterCommit`, not the transaction | Accepted |
| [0005](./0005-fetch-frame-mirrors-loadsubsetoptions.md) | The cursor `fetch` frame mirrors TanStack's `LoadSubsetOptions` | Accepted |
| [0006](./0006-server-originated-writes.md) | Server-originated writes: `runSyncedWrite` | Accepted (init caveat retired by 0007) |
| [0007](./0007-author-owned-schema-register-sync.md) | Author-owned schema; `registerSync` wires the sync | Accepted |
| [0008](./0008-orphaned-cdc-triggers.md) | Orphaned CDC triggers when a collection is removed | Accepted |
| [0009](./0009-changelog-time-retention.md) | Changelog time-based retention; reset stale reconnects | Accepted |
| [0010](./0010-typed-mutations-collection-manifest.md) | Typed mutations via a collection-row manifest on `SyncRegistry` | Accepted (manifest superseded by 0014) |
| [0011](./0011-ssr-dehydrate-hydrate.md) | SSR: dehydrate on the worker, hydrate to the cursor | Accepted (experimental; generalizes 0002 C1 → C1′; amended for merged upstream + 0015/0016 lift; D5 peer floor superseded by 0022) |
| [0012](./0012-wire-input-hardening.md) | Wire-input hardening: frame-shape guards, inbound limits, sanitized execute errors | Accepted (D1 amended by 0025 for op keys) |
| [0013](./0013-predicate-floor-one-evaluator.md) | Filtered-subscription membership: one evaluator is the source of truth; the floor is the verified-agreeing set | Accepted |
| [0014](./0014-object-sync-schema.md) | `defineSync`: one schema value, mutations on the collection, commands on the connection | Accepted (supersedes 0001 D11 builder; closes 0010 manifest) |
| [0015](./0015-syncable-mixin.md) | `Syncable` mixin: the sync core as a mixin over any DO base | Accepted (reframes 0001 D13; Actor unsupported; wake-test gap closed by 0019) |
| [0016](./0016-reconnect-policy.md) | Transport reconnect policy: injectable delay function, terminal 4xxx, jittered backoff | Accepted |
| [0017](./0017-blob-wire-normalization.md) | BLOB wire normalization: bare ArrayBuffer becomes Uint8Array at emission | Accepted |
| [0018](./0018-oversize-frames.md) | Oversize frames: client pre-send guard is the rejection surface; outbound is warn-only | Accepted |
| [0019](./0019-subscription-persistence-across-hibernation.md) | Subscriptions persist in SQLite and restore on hibernation wake | Accepted |
| [0020](./0020-connect-contract-abortable-open.md) | connect() never resolves disconnected; open() is abortable via AbortSignal | Accepted (amends 0016 + 0011 seam) |
| [0021](./0021-in-flight-settlement-unexpected-close.md) | In-flight mut/call settlement across an unexpected close: hold-and-replay + typed `ConnectionLostError` | Accepted (fixes #39; builds on 0020/0016/0011) |
| [0022](./0022-db-version-support-policy.md) | `@tanstack/db` version support: an uncapped floor, tested at both ends | Accepted (supersedes 0011 D5 peer floor in part) |
| [0025](./0025-empty-op-key-rejection.md) | An empty `mut` op key is rejected with a reply, after the dedup lookup | Accepted (amends 0012 D1 for op keys; makes 0014 D3 true) |
