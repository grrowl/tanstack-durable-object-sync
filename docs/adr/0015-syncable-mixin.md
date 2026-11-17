# 0015 — `Syncable` mixin: the sync core as a mixin over any DO base

**Status:** Accepted — implemented. Reframes [ADR-0001](./0001-sync-architecture.md)
D13's "base class" as a mixin factory; `SyncDurableObject` is now its trivial
application over `DurableObject`. Composes with, does not change,
[ADR-0006](./0006-server-originated-writes.md) (`runSyncedWrite`),
[ADR-0007](./0007-author-owned-schema-register-sync.md) (author-driven
`registerSync`), and [ADR-0008](./0008-orphaned-cdc-triggers.md) (the GLOB trigger
namespace — the collision-safety proof cited below).

## Context

`SyncDurableObject` was an abstract base class that `extends DurableObject`
directly (`sync-do.ts:38`, 0.4.0). A JavaScript class has one base, so a DO that
already extends a framework base — the Cloudflare Agents SDK `Agent`,
`@cloudflare/think`'s `Think`, or any other DO subclass — could not *also* be a
sync source. That forced a dedicated sync DO per scope plus a mirror write from
the DO that owns the data, the exact pattern this library exists to avoid.

An audit of the class found no structural blocker to hosting both a framework's
WebSocket surface and tddc's sync WebSocket protocol on one DO. Every coupling is
host-agnostic already or a mechanical namespacing fix, and the one hard
name collision (`sql`) has a clean resolution. The direction of every required
change is tddc-side: today tddc is the bad citizen (restores all sockets on wake
untagged, claims every upgrade in `fetch`, never delegates unknown WS events to
`super`).

## The cohosting proof

The mixin is only useful if one DO can host both protocols with **zero** change
to partyserver, agents, or Think. Verified against partyserver 0.5.8, agents
0.17.3, `@cloudflare/think` 0.12.1:

- **partyserver ignores sockets it did not open.** `isPartyServerWebSocket(ws)`
  is true only if the socket attachment carries a `__pk` key; every hibernation
  handler short-circuits a non-`__pk` socket (`webSocketMessage`, `webSocketClose`,
  `webSocketError`) and connection enumeration filters the same way, so
  `broadcast()` never touches a foreign socket. tddc accepts its socket with a
  plain attachment and **no** `__pk`, so partyserver is already blind to sync
  sockets with no change on its side.
- **Agent and Think add no socket entry points.** `Agent extends Server` and
  defines no `webSocketMessage/Close/Error`; `Agent.fetch` delegates upward via
  `super.fetch`. Think's dist contains zero `acceptWebSocket`/`WebSocketPair`/
  `getWebSockets` and only wraps `onConnect`.
- **tddc defines no `alarm()`.** Compaction rides `ctx.waitUntil`
  (`mixin.ts`, `#maybeCompact`), so the DO's single alarm slot stays wholly owned
  by the host. No chaining needed.

## Decision

Ship the server as a curried mixin factory `Syncable<Env, TUser>()(Base)`
(`src/server/mixin.ts`), with `SyncDurableObject = Syncable()(DurableObject)`
re-exposing its legacy protected surface so every `extends SyncDurableObject<Env,
Claims>` keeps compiling and behaving identically to 0.4.0. The 187 tests passing
without a single assertion edit is the back-compat proof gate.

Three independent discriminators keep the two protocols apart. Any one suffices
for host-side safety; together they are belt and suspenders:

1. **Reserved hibernation tag** `SYNC_TAG = "_tddc"` on `acceptWebSocket`. Tags
   are the only server-side filter `getWebSockets` offers, so the wake-time
   restore and every handler's ownership check key off it. Without it, the
   broadcaster fans sync frames onto host sockets after a wake. **Exception for a
   bare `DurableObject`:** there is no host to share with, so tddc owns *every*
   socket — the restore uses `getWebSockets()` (all) and the ownership check
   returns true unconditionally. This is what keeps a **legacy untagged socket**,
   accepted by a pre-mixin 0.4.0 build and surviving a hibernation wake across the
   upgrade, working instead of being silently ignored. Over any other base the
   restore is tag-filtered (`getWebSockets(SYNC_TAG)`).
2. **Plain attachment, no `__pk`.** The independent discriminator that keeps a
   `__pk`-filtering host blind to sync sockets, needing no cooperation from tddc.
   This is the one discriminator an author could accidentally violate — a claims
   object that itself carries a `__pk` key would make the host mis-claim the sync
   socket. The tag (discriminator 1) keeps tddc's own side correct regardless, and
   `#acceptSyncSocket` logs a loud error if a sync attachment carries `__pk` over a
   non-DO base; the README documents the reserved key.
3. **Dedicated fetch path** (default `/_sync`). Upgrades are partitioned before
   either protocol sees them; a non-matching upgrade returns `super.fetch(request)`
   (safe because `Agent.fetch` itself delegates upward). WS events on an untagged
   socket delegate via `super.webSocketMessage?.()` — a no-op on a bare DO, and on
   Agent/Think it resolves to partyserver's handler, which re-guards on `__pk`.

### The `sql` getter is deleted (mandatory, not stylistic)

0.4.0 exposed `protected get sql(): SqlStorage` (`sync-do.ts:103`). partyserver
defines `sql` as a **tagged-template method** (`dist/index.js:557`) and agents
redefines it (`agent-tool-types` d.ts, `sql<T>(strings, …values)`). A property
getter named `sql` on the mixed class shadows that method and breaks every
`cf_agents_*` query in the host. The mixin therefore uses a private `get #sql()`
over `this.ctx.storage.sql` internally and defines **no** public/protected `sql`.
`SyncDurableObject` re-adds `protected get sql()` because a bare `DurableObject`
defines no `sql` member to shadow; over any other host, authors reach
`this.ctx.storage.sql` directly (as the host-matrix fixture does).

### The two DO-global side effects are base-dependent opt-ins

`setWebSocketAutoResponse("ping","pong")` and `PRAGMA case_sensitive_like = ON`
affect the whole DO, not just tddc's sockets or queries: the auto-response would
swallow a literal `"ping"` frame from a host's client before the host sees it, and
the pragma changes the host's own `LIKE` semantics. Both default **ON** when
`Base === DurableObject` (bit-identical 0.4.0) and **OFF** over any other base,
with `this.sync.configure({ autoResponse, caseSensitiveLike })` to opt in.

**Open questions resolved before merge (greps against the installed hosts):**

- *Does any host client send a literal `"ping"` keepalive?* No. No `"ping"`
  string frame in agents 0.17.3, partyserver 0.5.8, or think 0.12.1 dists
  (client or server). The auto-response slot is free. Default-off over a non-DO
  base is defensive belt-and-suspenders and is what the cohosting smoke confirms.
- *Do any host `LIKE` queries rely on case-insensitivity?* No. No `LIKE`
  SELECT/WHERE in the agents/partyserver/think server dists; Think's search uses
  FTS5 `MATCH`. Default-off over a non-DO base is defensive.
- *Is the facade name `sync` taken on any host?* No. Neither partyserver's
  `Server`, agents' `Agent`, nor Think expose a `sync` member (confirmed against
  their `.d.ts` type surfaces). The fallback name `tddcSync` was not needed.

### One facade to shrink the collision surface

The loose protected members (`codec`, `subs`, `registry`, `broadcaster`, and
the internal handlers) are now `#private` or live behind a single
`this.sync: SyncApi` facade (`registerSync`, `runSyncedWrite`, `parseAttachment`,
`configure`, `registry`, `drainAndBroadcast`). So the names a mixed class puts on
its prototype that could collide with an arbitrary host shrink to the four
runtime-dispatched methods (`fetch`, `webSocketMessage/Close/Error`) plus `sync`.

Two deliberate narrowings, each documented rather than hidden:

- **Numeric tuning knobs stay `protected` overridable fields** (`tickMs`,
  `compactionEvery`, `maxOpsPerMutation`, `maxSubsPerSocket`, `maxFrameBytes`,
  `changelogRetentionMs`, `dedupRetentionMs`). They are the documented
  subclass-tuning contract, they provably do not collide with any supported host
  (grep-verified), and collapsing them would break existing subclasses with no
  security benefit. The mixin's *return type* hides them (so they never widen the
  mixed-class public surface); `SyncDurableObject` re-declares them (ambient) so
  legacy `protected override readonly tickMs = …` subclasses keep compiling. Over
  a non-DO host, tune via `configure`/private fields.
- **`registry` and `drainAndBroadcast` are exposed under the facade**
  (`this.sync.registry`, `this.sync.drainAndBroadcast`) — behind `sync`, so never
  on the bare collision surface — and re-aliased as protected on
  `SyncDurableObject` for the two white-box tests and the documented manual-drain
  API.

## Trigger safety on a host with pre-existing tables

A host base (Agent, Think) owns tables the author never registers
(`cf_agents_state`, `assistant_*`, `cf_think_*`). Safety holds by construction,
needing only tests and one documented rule:

- Triggers install per **registered** collection only (`registerSync` →
  `ensureTriggers` over the declared set). An unregistered host table gets no
  trigger, so host writes emit no CDC rows.
- The reaper drops only triggers matching `GLOB '_sync_changes_*'`, and GLOB
  treats `_` literally ([ADR-0008](./0008-orphaned-cdc-triggers.md)), so it can
  never drop a host trigger.
- tddc's own tables are `_sync_`-prefixed and cannot collide with host names;
  `assertValidCollection` rejects the `_sync_` prefix and `assertSyncCompatible`
  requires a sole TEXT client-supplied pk, which `cf_agents_*` fail.

The one real constraint, documented in the README: **never register a
host-owned table as a synced collection** (the prefix guard does not catch
`cf_`/`assistant_` names). The host-matrix test pins all of the above.

## What the mixin cannot support: `@cloudflare/actors`' `Actor`

The Actors `Sockets` helper adopts **every** hibernated socket in its constructor
with an unfiltered `ctx.getWebSockets()` (`packages/sockets/src/index.ts:22–39`),
broadcasts to all of them on `message('*')` (`:43–55`), and closes foreign
sockets in `webSocketClose` (`:61–77`); `Actor.webSocketMessage` hands frames from
all sockets to the app handler. A subclass mixin can intercept the dispatch
methods, but it cannot prevent the constructor-time adoption or the fan-out
without changing the Actors package. Until Actors filters foreign sockets the way
partyserver does, `Actor` is **unsupported**. This is a host limitation, not a
tddc design gap; a PR against Actors is possible in principle but out of scope.

## Consequences

- **`Syncable()` must be applied exactly once, over the outermost DO base —
  never stacked over another `Syncable()` application.** Verified:
  `class Outer extends Syncable()(Syncable()(DurableObject)) {}` is a real
  `tsc` error (the inner application's `ctx`/`Env` typing doesn't satisfy
  `DOCtor`'s construct-signature constraint without a cast). Forcing it past
  the type checker with a cast still doesn't work at runtime: `fetch`,
  `webSocketMessage/Close/Error`, and the `sync` getter are per-class
  overrides, so the outer layer's definitions always shadow the inner
  layer's — the inner layer's `registerSync` is never reachable and it never
  dispatches. Cohost by putting a **host framework** under `Syncable()`
  (`Syncable()(Agent)`), never another `Syncable()` application.
- **Cold-snapshot row order is now deterministic.** The `#handleSub` snapshot
  path (no `since` cursor) and the paginated `#handleFetch` path both lower
  through `compileSubsetQuery` (`sql-compiler.ts`), which defaults to
  `ORDER BY rowid` when the client sends no `orderBy` — previously a bare
  `SELECT * FROM tbl` left row order as an accident of SQLite's query plan
  (field-verified: a `WHERE` touching the pk can make the planner prefer the
  pk's autoindex over a rowid scan, returning pk-sorted rows instead of
  insertion order). `rowid` matches insertion order among currently-live rows
  and needs no schema change, since `assertSyncCompatible` (ADR-0007, D9)
  already forbids the `INTEGER PRIMARY KEY` pk that would alias it. It also
  forbids the two other ways a table can lack a usable internal rowid: a
  `WITHOUT ROWID` table (none at all — the read would throw `no such column:
  rowid` and hang the subscriber) and a declared `rowid` column (which shadows
  the internal one, so `ORDER BY rowid` would silently sort by that arbitrary
  column). All three are rejected at `registerSync`, not left to surface at read
  time.
- One DO class can be both a framework host and a sync source
  (`class FeedAgent extends Syncable<Env, Claims>()(Agent<Env, State>)`), with no
  framework added to tddc's dependency graph — the app supplies `Base`.
- `SyncDurableObject` is unchanged for its users; only the internal home of the
  code moved.
- The entire cohosting guarantee rests on partyserver's `__pk` filtering, an
  internal behavior of a pre-1.0 package that accepts no external PRs. Every bump
  of `agents`/`partyserver` must be gated by a real-`agents` cohosting smoke (out
  of CI, to keep the ~13 MB dep out of the package) that opens both socket types
  on one DO, forces a hibernation wake, and asserts no cross-talk. The in-CI
  host-matrix suite pins the same contract against a fake host.
- **The wake-time restore itself (discriminator 1's `getWebSockets(SYNC_TAG)`
  call, `mixin.ts`'s constructor) is not exercised by an actual hibernation
  eviction in the in-CI suite** — `@cloudflare/vitest-pool-workers@0.12.21`
  (this repo's pinned version) predates the `evictDurableObject`/
  `evictAllDurableObjects` helpers that ship a real evict-and-reconstruct
  cycle (added in `0.16.20`, which requires `vitest@^4`, a major bump this
  repo doesn't carry yet). Today the restore is verified by code-reading plus
  the same-instance socket-tag assertions in `tests/host-matrix.test.ts`
  (e.g. "sync and host sockets coexist"), not by a test that actually tears
  the instance down and reconstructs it. Upgrading `vitest`/`vitest-pool-workers`
  to unlock a real wake test is tracked as follow-up work, not bundled here.
  *[Superseded: the vitest 4 migration (PR #34) lifted the toolchain blocker,
  and ADR-0019's `tests/hibernation.test.ts` now exercises the restore under
  real `evictDurableObject` cycles — which promptly exposed that the restore
  was incomplete (sockets survived, subscriptions didn't). See ADR-0019.]*

## Amendment — 2026-09-29: subclass tick initialization

The coalescer reads `tickMs` when arming an on-demand flush, not while the mixin
constructor runs: subclass field initializers have not run during `super()`.
This preserves the protected-field tuning contract and creates no idle timer.
`tests/tick-override.test.ts` pins the real DO path; the slow-tick cursor-barrier
and replay tests now run with their intended 30-second coalescer window.
