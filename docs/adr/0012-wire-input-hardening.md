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
