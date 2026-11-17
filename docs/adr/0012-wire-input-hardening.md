# ADR-0012: Wire-input hardening — frame-shape guards, inbound limits, sanitized execute errors

**Status**: Accepted
**Date**: 2026-06-11
**Plan**: [005-wire-input-hardening.md](../../plans/005-wire-input-hardening.md)

## Context

The DO trusted every *decoded* frame's shape. MessagePack decoding was already
guarded (`webSocketMessage` ignored undecodable bytes), but a frame that decoded
to the wrong *shape* was dispatched as-is: a `mut` whose `txId` was an object
could reach `lookupTx` and `sql.exec` bindings with an arbitrary value.

Additionally, nothing capped `ops` length, per-socket subscriptions, or inbound
frame size, so one authenticated client could force unbounded allocation. And a
thrown `execute` error's raw message (SQLite constraint text, column names, etc.)
was forwarded verbatim to the client — leaking internal schema detail.

None of these are unauthenticated holes — the attacker is already an authed socket
on the same DO — but the library's ethos is reject-don't-degrade, and these are
the cheap mechanical layers of that.

## Decisions

### D1: Shape guard — drop and log, no reply

A `wellFormed(v: unknown): v is ClientFrame` check runs after decode, before any
SQL binding. A frame that fails the check is dropped with a server-side
`console.error` (fail loud in logs) and **no client reply** — mirroring the
existing "ignore undecodable frames" stance and extending its comment.

The guard validates per variant of `ClientFrame` (enumerated from
`src/wire/frames.ts`): required fields are checked for correct type and
non-emptiness; optional fields treat `null` as absent (the client transport
serialises absent fields as `null` in MessagePack rather than omitting them).
`where`/`orderBy` fields are left `unknown` — the sql-compiler is their
validator (it already `fail-loud`s via `UnsupportedPredicateError`).

### D2: Three overrideable limits as `protected readonly` tunables

Following the `tickMs`/`compactionEvery` field pattern (doc comment, `protected
readonly`, override-able by subclasses at construction time):

- **`maxOpsPerMutation = 128`**: checked at the top of `handleMut`. **Reject,
  don't truncate** — a partial apply would silently drop client writes. Sends
  `rejected` with `code: "LIMIT_EXCEEDED"`.
  *(2026-09-25: now checked after the dedup lookup, so a resent `txId` keeps
  its stored outcome; see [ADR-0025](./0025-empty-op-key-rejection.md).)*
- **`maxSubsPerSocket = 256`**: checked in `handleSub` before `subs.add`. A
  re-sub on an existing `subId` replaces the old entry
  (`SubscriptionRegistry.add` semantics) and does NOT count against the cap —
  only genuinely new sub IDs are counted. Over-limit → `reset` for the refused
  `subId` + `console.error`. (Reset is the existing "sub refused" signal.)
- **`maxFrameBytes = 1_048_576`**: checked in `webSocketMessage` before decode
  (`typeof message === "string" ? message.length : message.byteLength`).
  Oversize → drop + `console.error`. Cloudflare caps WS messages at ~1 MiB
  anyway; this makes the bound explicit, testable, and overrideable.

`SubscriptionRegistry.countFor(ws)` was added to expose the per-socket count
without exposing the internal Map.

### D3: Execute-error sanitization; authorize errors stay user-facing

In `handleMut`, there are two catch sites:

- **Authorize catch**: unchanged. Authorize errors are user-facing API
  (`README: "throw to deny"`). The error message passes through verbatim.
- **Execute catch (transaction)**: the full error is logged server-side
  (`console.error`) and a **generic** `"mutation failed"` message with code
  `"EXECUTE_FAILED"` is sent to the client. SQLite constraint strings, column
  names, and programming-error text are internal detail — not client API surface.

In `handleCall`, authorize and execute share one try/catch. Command authorize
errors are not currently user-facing API in the same way mutation authorize is, so
the entire catch is sanitized: log full detail server-side, send `"command failed"`
with `"EXECUTE_FAILED"` to the client.

**Compatibility note**: the client-visible error text for execute failures changed.
Callers who matched on specific SQLite error strings or programming-error messages
must update to the generic messages/codes. Authorize-path messages are unchanged.

### D4: Dedup identity binding deferred

`_sync_seen_tx` is keyed by `txId` alone; any authed socket presenting a
guessed/leaked txId receives the stored receipt. Risk is low (txIds are
client-random UUIDs) but the fix needs an identity-keying decision (`TUser` is
author-defined and unserializable in general — likely a `protected
dedupScope(user: TUser): string` hook). This is **explicitly deferred** pending
a maintainer design decision.

## Consequences

- **Security**: arbitrary decoded values no longer reach SQL bindings; inbound
  resource exhaustion is bounded; internal schema detail does not leak to clients.
- **Behavior change** (observable by consumers): execute-path `rejected` frames
  now carry `"mutation failed"`/`"command failed"` + `"EXECUTE_FAILED"` instead of
  the raw error message. Authorize-path messages are unchanged.
- **Hibernation**: no idle timers were introduced. All new checks are synchronous
  and run on the existing `webSocketMessage` path.
- **Extensibility**: all three limits are `protected readonly` — subclasses can
  override at construction time. `LimitsTestDO` in the test worker exercises this.
- **Test coverage**: `tests/wire-hardening.test.ts` (5 tests) pins all four
  invariants; `tests/error-paths.test.ts` was updated to assert the new generic
  error text for execute failures.

## Amendment — 2026-09-29: D3 command authorization

[ADR-0014 D2](./0014-object-sync-schema.md) revises D3 above: command
`authorize` errors now surface to clients like mutation `authorize` errors;
only command `execute` errors are sanitized as `EXECUTE_FAILED`. The shared
sanitized catch described in D3 is historical, not the current contract.

## Amendment 2026-11-17: D4 is an accepted limitation, and it has two effects

D4 names one effect. There are two, and we accept both for now.

1. **Read.** A socket that presents another client's `txId` receives that
   client's stored receipt, including a command's result. `#oncePerTx` finds the
   row with `lookupTx` and `#replayReceipt` sends it (`src/server/mixin.ts`,
   lines 876 to 879 and 912 to 925; `src/server/dedup.ts`, lines 24 to 34).
2. **Pre-claim.** A socket that uses a `txId` first makes the owner's later write
   with the same `txId` answer `committed` without running, and the owner's rows
   are never written. The same lookup answers the owner with the first socket's
   receipt. If the first attempt is still running, the owner waits on the
   in-flight map and then gets that receipt (`mixin.ts`, lines 881 to 884). A
   rejected attempt is recorded too (`#rejectTx`, lines 907 to 909), so the owner
   can be answered with another socket's rejection. That last case was read from
   the code and not run.

`tests/dedup-identity-scope.test.ts` reproduces both as `it.fails`, next to a
control that shows a reconnect of the same identity must still get its replay.

**Why we accept it.** Each point below is checked in the code on this branch.

- The built-in `txId`s are random. A command's `txId` is
  `crypto.randomUUID()` (`src/client/transport.ts`, line 719). A mutation's
  `txId` is the TanStack transaction id (`src/client/do-collection.ts`, line
  940), which is `config.id ?? safeRandomUUID()` (`transactions.js` in 0.8.6,
  0.9.2 and 1.0.0-rc.1.5). `safeRandomUUID` (`utils/uuid.js`) uses
  `crypto.randomUUID` or `crypto.getRandomValues` and throws if neither exists.
- The table is per Durable Object. `#sql` is `this.ctx.storage.sql`
  (`mixin.ts`, lines 270 to 272), `_sync_seen_tx` is created there
  (`src/server/changes.ts`, line 40), and the in-flight map is an instance field
  (`mixin.ts`, line 200). An attacker must be authenticated on the same object.
- No server frame carries a `txId` to another socket. Only `committed` and
  `rejected` carry one (`src/wire/frames.ts`, lines 79 and 80), and every send of
  them passes the socket that sent the frame (`mixin.ts`, lines 799, 866, 909,
  919 and 925). Broadcasts are only `d` and `uptodate`.

So an attacker needs a `txId` that the built-in clients never expose, plus a
connection to the same Durable Object. A default that scopes dedup to the socket
would also break the replay after a reconnect that ADR-0021 relies on, so the fix
would be an option most apps never set, a new key on a table that already exists
in deployed objects, and a migration.

**What an app must not do.**

- Supply a predictable or reused id, whether as `createTransaction({ id })` or as
  the `txId` of a `transport.sendMut` frame. Treat a `txId` like a password for
  the dedup window.
- Show a `txId` to other users. Some client error messages contain it
  (`transport.ts`, lines 612, 623 and 795), so do not forward them, or telemetry
  that includes them, to other users.
- Put users who do not trust each other in one Durable Object when commands
  return secrets.

**Revisit if** any feature sends a `txId` to another socket (for example a delta
that names the transaction that caused it), or an app needs untrusted peers in
one Durable Object. A documented pattern that asks apps to supply stable
transaction ids, such as an offline outbox, would also raise the risk. The shape
is still the `dedupScope(user)` hook from D4, with today's behavior as the
default.

## Amendment — 2026-11-17: a refusal is always answered, and long lists no longer throw

A `sub` with an `in` list over 100 values threw from SQLite with no reply, and the
sub stayed registered in memory and in `_sync_subs` ([ADR-0013 amendment of the
same date](./0013-predicate-floor-one-evaluator.md)). This change guarantees:

- **Long lists no longer throw.** `in` binds one parameter per kind of value, not
  per value.
- **A statement over one of SQLite's limits is a deliberate refusal.** Two lines
  of defence, in order:
  1. *The compiler's checks come first.* `compileSubsetQuery` refuses a statement
     over any limit a query can reach, on a one-sided bound (it never accepts what
     SQLite rejects for a limit, and may refuse a little early; ADR-0013
     amendment). Before a sub is added, its predicate's stored form
     (`_sync_subs.where_ir`, the tagged value codec) is checked against the value
     cap too: tagged Dates and bigints can make it far larger than the compiled
     query's own parameters.
  2. *The backstop.* If SQLite still rejects one of a sub's or a fetch's
     statements with one of its limit errors, that is treated as the same
     refusal. The errors are matched by message, listed in one place
     (`SQLITE_LIMIT_ERRORS` in `sql-compiler.ts`), and each is provoked for real
     in `tests/sqlite-limits.test.ts`, so a change in workerd's wording fails a
     test instead of bringing the hang back. These errors are deterministic for a
     given statement, so a retry cannot help, and a refusal is the truth. The net
     covers the sub's write-through, its snapshot read and the fetch's reads; a
     row that is over the cap only because of a very long subId is one case it
     catches.
- **A request the server refuses on purpose always gets its refusal reply, and
  nothing of a refused sub stays registered.** The deliberate refusals of a `sub`
  are: an unknown collection, the per-socket cap (D2), a predicate outside either
  floor, and a statement over one of SQLite's limits (either line above). Each
  answers with `reset` and removes the subId in memory and in `_sync_subs`. A
  refused re-sub therefore also drops the entry it would have replaced, since the
  client now holds that subId as reset. A refused `fetch` answers with an empty
  `page`, as before. The orphan-sub sweep (ADR-0019 D4) had the same bug: it bound
  one parameter per live socket and threw in the background with 101 sockets. It
  now passes them as one JSON parameter.

The snapshot rows are read before the sub's first frame, and the below-floor
fallback reads them before it sends its `reset`. So a read that throws has sent
the client nothing for that sub: no `snap`, which the client would keep (a
`reset` before `snap-end` is taken as holding no rows, ADR-0023 D5).

**Unexpected failures are unchanged here.** A throw that is neither a deliberate
refusal nor a SQLite limit error (a storage failure, a bug) still escapes the
handler with no reply, and a sub registered before it stays registered, as
before this change. Two replies were considered and rejected for now: a bare
`reset` loses rows for a resumed eager sub (it truncates, nothing re-snapshots,
and the transport keeps resuming from its cursor), and closing the socket loops
when the cause persists, because the client resets its backoff on every
successful open. Unexpected failures are decided separately, together with a
client backoff fix.

**No new inbound cap.** A list's length no longer reaches SQLite as parameters,
`maxFrameBytes` (D2) already bounds the size of any one predicate on the socket,
and SQLite's limits are refused as above. The evaluator's cost per row is a `Set`
lookup on 1.0 and a scan of the list on 0.8.6 and 0.9.2, bounded by the same
frame cap. Observed and left open: the predicates a socket keeps, in memory and
in `_sync_subs`, are bounded only by `maxSubsPerSocket × maxFrameBytes` (256 MiB),
which is above a Worker isolate's 128 MB. That was already true for any large
literal and does not depend on list length.

Pinned by `tests/long-in-lists.test.ts` ("a predicate over SQLite's limits is
refused on purpose") and `tests/sqlite-limits.test.ts`.
