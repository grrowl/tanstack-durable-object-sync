# Architecture Decision Records

Decisions are recorded as ADRs (see
[0000](./0000-record-architecture-decisions.md) for the process). They are
append-mostly: when a decision changes, a new ADR supersedes the old one and
explains the displacement.

| # | Title | Status |
|---|---|---|
| [0000](./0000-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0001](./0001-sync-architecture.md) | Sync architecture: single-ordered-stream over a Durable Object | Accepted (amended by 0002; D11 builder superseded by 0014; D13 base class reframed as a mixin by 0015; D8 on-demand subset model superseded by 0023) |
| [0002](./0002-adversarial-review-corrections.md) | Corrections from adversarial review: ordering, shaping, retention | Accepted (C5 retention refined by 0009; the upstream behaviour C2 verified changed in db 0.12, see 0028) |
| [0003](./0003-atomic-cursor-fetch.md) | Cursor load-more is one atomic fetch, not two | Accepted (naming amended by 0005; insert-if-absent also inside one page, 2026-11-17) |
| [0004](./0004-after-commit-hook.md) | Side effects go in a fire-and-forget `afterCommit`, not the transaction | Accepted |
| [0005](./0005-fetch-frame-mirrors-loadsubsetoptions.md) | The cursor `fetch` frame mirrors TanStack's `LoadSubsetOptions` | Accepted |
| [0006](./0006-server-originated-writes.md) | Server-originated writes: `runSyncedWrite` | Accepted (init caveat retired by 0007) |
| [0007](./0007-author-owned-schema-register-sync.md) | Author-owned schema; `registerSync` wires the sync | Accepted |
| [0008](./0008-orphaned-cdc-triggers.md) | Orphaned CDC triggers when a collection is removed | Accepted |
| [0009](./0009-changelog-time-retention.md) | Changelog time-based retention; reset stale reconnects | Accepted |
| [0010](./0010-typed-mutations-collection-manifest.md) | Typed mutations via a collection-row manifest on `SyncRegistry` | Accepted (manifest superseded by 0014) |
| [0011](./0011-ssr-dehydrate-hydrate.md) | SSR: dehydrate on the worker, hydrate to the cursor | Accepted (experimental; generalizes 0002 C1 → C1′; amended for merged upstream + 0015/0016 lift; D5 peer floor superseded by 0022; amended 2026-11-17: a render refuses writes, never reads) |
| [0012](./0012-wire-input-hardening.md) | Wire-input hardening: frame-shape guards, inbound limits, sanitized execute errors | Accepted (D1 amended by 0025 for op keys; D2 limit order amended by 0025; D4 accepted as a limitation, both effects named, 2026-11-17; amended 2026-11-17: a deliberate refusal is always answered and leaves nothing registered) |
| [0013](./0013-predicate-floor-one-evaluator.md) | Filtered-subscription membership: one evaluator is the source of truth; the floor is the verified-agreeing set | Accepted (amended 2026-09-29: `isNull`/`isUndefined`; amended 2026-11-17: long `in` lists, `in` compares by kind, SQLite limits refused at compile time; amended 2026-11-17: a column the table does not have is refused) |
| [0014](./0014-object-sync-schema.md) | `defineSync`: one schema value, mutations on the collection, commands on the connection | Accepted (supersedes 0001 D11 builder; closes 0010 manifest) |
| [0015](./0015-syncable-mixin.md) | `Syncable` mixin: the sync core as a mixin over any DO base | Accepted (reframes 0001 D13; Actor unsupported; wake-test gap closed by 0019) |
| [0016](./0016-reconnect-policy.md) | Transport reconnect policy: injectable delay function, terminal 4xxx, jittered backoff | Accepted (§3 amended 2026-11-17: only a connection that stayed open 5 s resets the backoff) |
| [0017](./0017-blob-wire-normalization.md) | BLOB wire normalization: bare ArrayBuffer becomes Uint8Array at emission | Accepted |
| [0018](./0018-oversize-frames.md) | Oversize frames: client pre-send guard is the rejection surface; outbound is warn-only | Accepted |
| [0019](./0019-subscription-persistence-across-hibernation.md) | Subscriptions persist in SQLite and restore on hibernation wake | Accepted |
| [0020](./0020-connect-contract-abortable-open.md) | connect() never resolves disconnected; open() is abortable via AbortSignal | Accepted (amends 0016 + 0011 seam) |
| [0021](./0021-in-flight-settlement-unexpected-close.md) | In-flight mut/call settlement across an unexpected close: hold-and-replay + typed `ConnectionLostError` | Accepted (fixes #39; builds on 0020/0016/0011) |
| [0022](./0022-db-version-support-policy.md) | `@tanstack/db` version support: an uncapped floor, tested at both ends | Accepted (supersedes 0011 D5 peer floor in part; peer range, contract table and `locked` leg amended by 0028) |
| [0023](./0023-on-demand-watches-acquisitions-holds.md) | On-demand: watches, exact acquisitions, and row holds | Accepted (supersedes 0001 D8's subset model; 0.9 adapter contracts; locale ordering of text accepted as a limit, 2026-11-17) |
| [0024](./0024-examples-single-tanstack-db.md) | Examples bundle exactly one `@tanstack/db`, and CI proves it | Accepted (examples only; amended: the examples move to the 1.0 release candidate) |
| [0025](./0025-empty-op-key-rejection.md) | An empty `mut` op key is rejected with a reply, after the dedup lookup | Accepted (amends 0012 D1 for op keys; makes 0014 D3 true; amended: every per-tx rejection runs after the dedup lookup; a duplicate of an in-flight txId waits, then replays) |
| [0026](./0026-cursor-resubscribe-round.md) | Hold the single cursor until a resubscribe round is complete | Accepted as a design, not built yet: the server close rule first, then the transport part (amends 0002 C1, 0011 D3, 0016 and 0023 D7) |
| [0028](./0028-db-1-0-release-candidates.md) | `@tanstack/db` 1.0: the peer range admits the release candidates, and the adapter follows the 1.0 sync contract | Accepted (amends 0022; 0027 is reserved for a proposed ADR not yet merged) |
