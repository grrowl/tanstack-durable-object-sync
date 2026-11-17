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
