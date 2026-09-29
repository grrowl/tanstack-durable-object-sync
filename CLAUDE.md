# CLAUDE.md — `tanstack-durable-object-sync`

Bidirectional sync of a TanStack DB collection ↔ one Cloudflare Durable Object,
over a single ordered stream. *What/how* lives in `README.md`; *why* lives in
`docs/adr/`. This file is how we build.

## Ethos

Decision-first, built in the open. The code is downstream of the reasoning —
every non-trivial choice is an ADR, and the next reader should be able to
reconstruct *why* without archaeology. Hold that bar: think before you type,
justify departures, leave the design legible. Caution over speed on anything
load-bearing; judgment on the trivial.

## Rules of the repo

- **Think first.** State assumptions; surface ambiguity; stop when confused
  rather than guess.
- **Simplest thing that works.** Nothing speculative. If a senior eng would call
  it overcomplicated, it is.
- **Surgical.** Touch only what the task needs. Don't "improve" adjacent code or
  reformat on the way past. Match the surrounding style.
- **Read before you write.** Exports, callers, and the relevant ADR — before you
  change anything load-bearing.
- **Surface conflicts, don't blend them.** Two patterns disagree → pick one (more
  recent / more tested), act on it, flag the other. Blended designs erode the
  inversions this thing depends on.
- **Tests encode *why*.** A test that can't fail when the business logic changes
  is wrong.
- **Fail loud.** "Done" is a lie if anything was skipped, capped, sampled, or
  swallowed silently. Reject; don't degrade.

## Orientation

- **`docs/adr/`** is the source of truth for *why*. Read the index, then the ADR
  for whatever you touch. It is **append-mostly**: a changed decision gets a new
  ADR that supersedes the old and says so — never rewrite history.
- **`CHANGELOG.md`** — Keep a Changelog; `[Unreleased]` accrues until a release.

## Architecture (one line each)

- `server/sync-do.ts` — the DO base class: frame dispatch, `mut`/`call`/`fetch`,
  `registerSync`, `runSyncedWrite`, `drainAndBroadcast`.
- `server/registry.ts` — `Registry`: collections (`{table, pk}`), mutations,
  commands, handler context.
- `server/changes.ts` — CDC: the change log, AFTER triggers, PRAGMA validation,
  drain cursor, compaction.
- `server/broadcast.ts` — egress coalescer; flush timer armed on demand (none
  when idle).
- `server/{dedup,subscriptions,sql-compiler}.ts` — txId dedup; sub registry;
  IR→SQLite.
- `client/transport.ts` — `WebSocketTransport`; the single `appliedSeq` cursor.
- `client/do-collection.ts` — the TanStack collection-options creator; eager +
  on-demand.
- `wire/` — frames + MessagePack / tagged-value codecs.

## Invariants you can break by accident

Each is load-bearing and guarded by an ADR — read it before you touch the area.

- **One client cursor**, advanced only at commit boundaries. No second ack
  channel. (0002)
- **Deltas flush before `committed`** on the originating socket. (0002 C1)
- **The DO never joins/aggregates/runs IVM** — reads are client-side. (0001)
- **Sole TEXT-affinity, client-supplied pk** — optimistic id == confirmed id.
  (0001 D9 / 0007)
- **No idle timers.** Anything that polls while the DO is quiet kills
  hibernation. (Why `runSyncedWrite`, not a periodic drain — 0006)
- **Capture ≠ broadcast.** A synced write outside a `mut`/`call` handler must go
  through `runSyncedWrite`, or clients don't see it until the next mutation.
  (0006)

## Build discipline

- **TDD.** Tests run inside workerd; isolate by **unique DO name**, not storage
  rollback. A test pins intent, not mechanics.
- **Test the deferred effect, not just the function.** Work behind `ctx.waitUntil`
  / alarms / timers escapes coverage easily — `maybeCompact`'s housekeeping ran
  untested for milestones because tests called `compactChanges`/`pruneChanges`
  directly (ADR-0009). Drive the real path and **poll for the effect** (the
  `waitFor` pattern); the cadence gate is synchronously observable below
  threshold, so "not yet" is a race-free assertion.
- **One decision → one ADR.** Conventional commits (`!` = breaking); end with the
  Co-Authored-By trailer. Commit/push only when asked; `main` is the release
  branch.
- **Run plans and risky changes past codex (`gpt-5.6-sol`) as an adversary**
  before committing — it has caught real bugs here (the cursor-fetch race, the
  scroll-back `DuplicateKeySyncError`). Use the strongest available model; a
  weaker adversary is a weaker review. Pipe input in with the prompt as the
  only argument (`git diff --no-ext-diff | codex exec "…"`; codex appends
  stdin to the prompt, and rejects a trailing `-` after a prompt), or close
  stdin (`codex exec "…" < /dev/null`) — an open stdin hangs codex forever.
