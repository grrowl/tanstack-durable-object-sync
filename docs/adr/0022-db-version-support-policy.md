# 0022 — `@tanstack/db` version support: an uncapped floor, tested at both ends

**Status:** Accepted. **Supersedes in part** [ADR-0011](./0011-ssr-dehydrate-hydrate.md)
D5's peer-range statement: the floor moves from 0.8.5 to **0.8.6**, and the
rationale for the range now lives here. The rest of D5 (the
`withCollectionConfigFactory` marker, no `Symbol.for` self-branding, released
packages only) stands.

## Context

ADR-0011 D5 declared `@tanstack/db >= 0.8.5`. CI then tested exactly one
version — the lockfile's, which happened to be the floor. Nothing tested the
open upper end, and nothing tested the package as npm delivers it: the suite
imports `src/`, never the built tarball.

Preparing 0.7.0 found the gap was real. `@tanstack/db` 0.8.5 added
`query/runtime-reference-identity.ts`, which calls `crypto.getRandomValues()`
at **module scope**. workerd forbids random values in global scope, so a Worker
that renders SSR (README: `DbClient` + `collectionOptions` + `SsrSnapshotTransport`
on the worker) **fails to start** on 0.8.5 — reproduced in workerd from our
packed tarball. Upstream fixed it in 0.8.6 (TanStack/db#1782). Two things hid it:

- the vitest pool evaluates test modules inside a request, where the
  global-scope restriction does not apply, so the full suite passed on 0.8.5;
- `@tanstack/db` is `sideEffects: false`, so a server-only Worker (no
  `DbClient`/`createCollection`) tree-shakes the module away and boots fine.

Upstream facts that shape the policy: `@tanstack/db` is pre-1.0 and ships
often (0.8.5 → 0.9.2 in weeks). `@tanstack/react-db` pins one **exact**
`@tanstack/db` (0.3.5 → 0.8.5, 0.3.8 → 0.9.0, 0.4.1 → 0.9.2), so a React app's
db version is whatever its react-db pins; it cannot pick one independently.

## Decision

### 1. The floor is the lowest version whose contracts we use: 0.8.6

| Version | Contract we depend on |
|---|---|
| 0.8.0 | `DbClient` dehydrate/hydrate and the syncMeta hooks (ADR-0011); `withCollectionConfigFactory` |
| 0.8.2 | `markError(cause)` — a failed first connect fails `preload()` with its cause |
| 0.8.4 | per-subset load failures reject that subset's promise |
| 0.8.5 | `commit()` receipts (`SyncAppliedReceipt`); descriptor reuse by id |
| 0.8.6 | module evaluation is Worker-safe (the crash above) |

Below 0.8.0 the imported APIs do not exist, and 0.8.5 crashes a Worker at
module evaluation. Between those, the adapter's version-sensitive paths degrade
quietly instead of failing. Without `markError`, `markError?.(e)` is a no-op, so
a failed first connect hangs `preload()` instead of failing it. A `commit()`
without a receipt counts as settled, so a subset load can resolve before its
rows are visible. Supporting older versions would mean making those degraded
paths deliberate, detected and tested, and every such branch would be a code
path no CI leg runs. Rejected: the floor is a single line, not a compatibility
layer.

CI proves the floor **works**, not that it is **minimal**. The suite and
typecheck also pass on 0.8.4 — no test pins the 0.8.5 receipt contract by
failing below it — and the vitest pool cannot see the 0.8.6 fix at all (the
pack smoke test below can). Minimality rests on the table above.

### 2. No upper cap

The peer range is `>=0.8.6`, open-ended — an owner decision: `peerDependencies`
tell the story, and no README compatibility section repeats it. A cap (`<0.10`,
`<1`) would block every app from upgrading react-db — which drags db with it,
exactly — until we cut a release, including the common case where nothing
broke. The cost is accepted and stated: we find an upstream break **after**
upstream ships it, via the scheduled job (§3), not before; an already-published
version of ours cannot be fixed retroactively.

When the scheduled job goes red: triage whether it is our bug or an upstream
break. Fix ours in a patch release. If the fix needs a newer upstream API, that
raises the floor (§4). If it is an upstream regression, report it upstream and
record it in the CHANGELOG. When it is green on a newer release than the lockfile,
bump the root devDependency so `locked` tracks it.

### 3. How both ends are tested

- **PR CI, matrix `locked` × `floor`** (`ci.yml`): `locked` is the lockfile
  version (devDependency `^0.9.2`); `floor` installs exactly the version
  `scripts/db-floor.mjs` derives from `peerDependencies` — so the tested floor
  cannot drift from the declared one, and any range not shaped `>=X.Y.Z` fails
  loud. Both legs assert the installed version, then typecheck and run the full
  suite. **The db version never floats in PR CI**: an upstream db release
  must not turn unrelated PRs red.
- **Pack smoke test** (`npm run smoke:pack`, floor + locked, on every PR):
  build, `npm pack`, then install the tarball into a throwaway consumer. db
  is installed at an exact version; the consumer's other ranges resolve fresh,
  as a real install does (see Alternatives), so a breaking release of one of
  those can surface on an unrelated PR — as a true consumer-facing break. It
  checks:
  - every `main`/`types`/`exports` target and every source-map `sources` entry
    must be in the tarball (hence `src` ships — see Consequences);
  - the consumer type-checks with `skipLibCheck: false`: a Worker program
    (workers types; `bundler` and `nodenext` resolution) using all three
    entries, and a DOM-only browser program importing `./client` alone.
    Known diagnostics inside `@tanstack/db`'s own declarations (its
    local-storage types assume DOM) are allowlisted by file, code and name,
    and printed; any other diagnostic fails, upstream's included — the root
    typecheck skips lib checks, so this is where an upstream declaration break
    shows;
  - `./client` imports under Node's strict ESM resolution;
  - wrangler bundles the consumer Worker and it **boots in real workerd**,
    seeds a row through `runSyncedWrite`, and renders an SSR snapshot through
    `readSyncSnapshot` → `SsrSnapshotTransport` → `DbClient.dehydrate()`.
    This is the only check that evaluates the packed entries at module scope in
    workerd — the class of bug in the Context.

  Its scope is packaging and module evaluation. Behavioural contracts (receipts,
  hydrate/merge, reconnect) are the suite's job, at both ends.
- **Scheduled `latest`** (`upstream-latest.yml`, weekly + `workflow_dispatch`):
  `@tanstack/db@latest` — full suite, build, and the pack smoke test. The
  examples are not covered: they pin react-db, which pins db, so "latest db"
  for them means "latest react-db", a different axis.

### 4. Moving the floor

Raising the floor is a **breaking change** for anyone on an older db: a
conventional commit with `!`, a CHANGELOG entry under *Changed* naming the
contract that forces it, and an update to §1 here (or a superseding ADR).
Because CI derives the floor from `package.json`, the matrix follows the edit
with no workflow change. Lowering it needs the full suite and the pack smoke
test green at the new floor, and a recorded reason.

## Alternatives considered

- **Cap the range** (`<1`, suggested in adversarial review). Rejected by the
  owner; see §2.
- **Float `latest` in PR CI.** Rejected: upstream releases would fail unrelated
  PRs. The scheduled job carries that signal alone.
- **Rely on the vitest suite for runtime coverage.** Insufficient — it passed
  on 0.8.5. The pool's request-scoped evaluation is the blind spot.
- **Lock the smoke consumer's install.** Rejected: db is installed at an exact
  version; what floats is its own ranges (`@tanstack/pacer-lite ^0.2.1`,
  `@standard-schema/spec ^1.1.0`) and ours (`@msgpack/msgpack ^3`) — exactly
  what a fresh consumer gets. Locking would test something nobody installs.

## Consequences

- The published peer range becomes `>=0.8.6` (0.6.0 shipped `>=0.6.0`; the
  0.8.5 floor was never released).
- `src` ships in the tarball so the shipped `.js.map`/`.d.ts.map` resolve
  (go-to-definition lands on source, not a missing file): about +54 kB packed.
- Known gap, not fixed here: the server `.d.ts` files import
  `@cloudflare/workers-types`, which the package does not declare. The smoke
  consumer installs it — the documented setup (README, examples) — so the test
  models that setup rather than proving the import is safe without it.
- GitHub disables scheduled workflows after 60 days without repository
  activity, and sends their failure notifications to whoever last edited the
  cron line. A quiet repo must re-enable the job.
