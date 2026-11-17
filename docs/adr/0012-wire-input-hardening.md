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
     over any limit a query can reach, on one-sided bounds that may refuse a
     little early (ADR-0013 amendment). The depth bound covers top-level
     conjunctions; a conjunction nested inside an `or` can still plan deeper,
     and the backstop below covers that. Before a sub is added, its predicate's stored form
     (`_sync_subs.where_ir`, the tagged value codec) is checked against the value
     cap too: tagged Dates and bigints can make it far larger than the compiled
     query's own parameters.
  2. *The backstop.* If SQLite still rejects one of a sub's or a fetch's
     statements with one of its limit errors, that is treated as the same
     refusal. The errors are matched by message, listed in one place
     (`SQLITE_LIMIT_ERRORS` in `sql-compiler.ts`), and each is provoked for real
     in `tests/sqlite-limits.test.ts`, so a change in workerd's wording fails a
     test instead of bringing the hang back. These errors are deterministic for a
     given statement and schema, so a retry cannot help, and a refusal is the
     truth. The net
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
  `page`, as before. The orphan-sub sweep (ADR-0019 D4) had the same bug as the
  long lists: it bound one parameter per live socket and threw in the background
  with 101 sockets. It now passes them as one JSON parameter.
- **A log line clips every string the client chose** (an id, a collection name, a
  malformed frame, a refusal reason that quotes the predicate) to 128 characters,
  and says that it was clipped and how long it was, so a client cannot make the
  server write an unbounded log line. Only log text is clipped; nothing stored,
  sent or compared changes.

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

## Amendment 2026-11-17: `since` is a plain decimal position

D1 checked only that a `sub`'s `since` was a string, and `#handleSub` read it
with `Number()`. So `"1e400"` became `Infinity`, passed every comparison, and was
answered `uptodate` at the current position: the client was told it was up to
date and was sent nothing. `"abc"` became `NaN` and got a snapshot.

### Decision 1: what `since` must look like

A position is a SQLite INTEGER in `_sync_changes.seq`, so its largest value is
2^63 - 1, which has 19 digits. D1 now requires `since`, when present, to match
`^(0|[1-9][0-9]{0,18})$`: digits only, no sign, no leading zero, at most 19
digits (`POSITION` in `src/server/mixin.ts`). Any other `since` makes the frame
malformed, and D1 applies: it is dropped with a log line and no reply, and
nothing of the sub is registered or stored. We drop rather than refuse with a
`reset` because a malformed `since` is a shape error, like a `since` that is
not a string, and the library's client never sends one (decision 3).

The server reads its own positions as numbers (`currentSeq`, `minChangeSeq`), so
it compares `since` as a number too. Up to 2^53 that is exact. A 19 digit value
above that is not exact, but it is still above every position the server can
hold as a number, so it is correctly found to be past the end.

### Decision 2: a `since` past the current position gets `reset` and a snapshot

A well formed `since` above the current position cannot be a position this
object gave out. It happens when the object's storage was recreated and its
positions started again, while a client kept its cursor, or its SSR meta, from
before. Such a `since` used to be answered `uptodate`, so the client kept the
old object's rows for ever. It now falls through to `reset` and a snapshot,
exactly as a `since` below the log's floor already did. This is a change of
behaviour on its own, separate from decision 1, and each kind of subscription in
`src/client/do-collection.ts` already handles that `reset`:

- **An eager sub** (`makeHandler`, `onReset`): it truncates the collection in
  one sync transaction, and the snapshot that follows repopulates it. A hydrated
  eager collection sends its dehydrated cursor as `since`, so this is the path
  that replaces the old object's hydrated rows.
- **An on-demand watch** (`onReset`, the bootstrapped branch): a watch first
  subscribes with no `since`, so it can only meet this on a reconnect, when the
  transport resumes it from its cursor. It truncates every subset
  (`truncateAll`), and upstream's truncate replay reloads each demand with fresh
  subs, as for a reconnect below the floor.
- **The hydrated catch-up** of an on-demand collection (`#hydrate#`, `onReset`):
  it truncates every subset, unsubscribes before the trailing snapshot arrives,
  and marks the collection ready. The live queries' subs then load from the new
  object.

`tests/ssr-cursor-form.test.ts` pins the eager sub and the hydrated catch-up
through the real client, with rows rendered from one object and hydrated
against another whose log is shorter. The watch's path is the one a reconnect
below the floor already takes, and is read from the code.

### Decision 3: the client sends `since` only in the plain form

A cursor in SSR meta is checked by `parseSyncMeta`, which accepted any string
`BigInt` parses, such as `" 5"`, `"05"` or `"0x5"`, and the eager and on-demand
paths sent it on as `since` unchanged. The server always writes the plain form,
so only meta that was changed on the way can hold another spelling. But after
decision 1 such meta would leave the collection with a sub the server never
answers, after `markReady`, so it would silently stop receiving updates.
`parseSyncMeta` now returns the plain form (`String(BigInt(cursor))`), and
refuses a cursor above the largest the server accepts (`MAX_CURSOR`, 19 nines,
in `src/client/transport.ts`) as it refuses a negative or unparseable one: the
hydrate call throws, and sync start takes the safe route of a snapshot that
reconciles the rows. `seedCursor` refuses a cursor above that bound too. Everything else the
client sends as `since` is `appliedCursor`, which is `String` of a bigint taken
from the server's own positions.

**Breaking for raw clients only.** A raw client that sends any other spelling of
a number (`"01"`, `"1.0"`, `"1e3"`, a space) now gets no reply for that `sub`.

Pinned by `tests/since-shape.test.ts`, `tests/ssr-cursor-form.test.ts`, and the
sync meta tests in `tests/ssr-adapter.test.ts` and `tests/ssr-cursor.test.ts`.

## Amendment 2026-11-17: ids and names have a length limit

The only bound on an id a client chooses was the frame size. A `sub` id is
echoed in every frame for its sub (each `snap`, delta, `snap-end`, catch-up
`uptodate` and `reset`) and stored in `_sync_subs`, so a 600 000 character sub
id made every delta for it at least that long. Each such frame also logged an
oversize warning, so the client chose how many log lines the server wrote. A
`txId` is stored in `_sync_seen_tx` for `dedupRetentionMs`. A `fetchId` is
echoed once. A collection or command name that is not registered is refused,
but a `mut` or `call` refusal quoted it in its message, which is stored with the
receipt.

### Decision 1: the limits, and where they come from

- **An id** (`subId`, `fetchId`, `txId`) may be at most **256 bytes**
  (`MAX_ID_BYTES` in `src/server/mixin.ts`).
- **A collection or command name** may be at most **128 bytes**
  (`MAX_NAME_BYTES` in `src/server/registry.ts`).

The library's client builds its ids from a table name: a sub id is
`<table>#<n>`, a fetch id `<table>#fetch#<n>`, and the hydration catch-up's sub
id `<table>#hydrate#<n>` (`src/client/do-collection.ts`). `n` is a counter that
prints as at most 21 digits. A call's `txId` is `crypto.randomUUID()`, 36
characters, and a mutation's is TanStack's transaction id, which is a UUID unless
the app supplies one with `createTransaction({ id })`. Nothing bounded a table
name before: `IDENT` checks its characters only, and SQLite has no identifier
limit of its own. So `compileSchema` now refuses a table name (in
`assertValidCollection`) or a command name over 128 bytes, with a message that
names the limit. Those are the only places a name enters the registry:
`registerSync` compiles the schema with `compileSchema`, and mutations are keyed
by their table name. With that bound, the longest id the library builds is
128 + 9 + 21 = 158 bytes, under 256. Only an app supplied transaction id can
reach the limit, and decision 2 makes that fail at once.

Both are fixed numbers, not options beside D2: they describe the protocol's
shape, not how a deployment is tuned, and a client must be able to rely on them.

**The unit is the UTF-8 byte.** SQLite stores TEXT as UTF-8 and MessagePack sends
a string as UTF-8, so bytes bound what is stored and sent. A limit in characters
would let a client send four times as many bytes in four byte characters. A
UTF-16 unit is at least one UTF-8 byte, so a string longer than the limit in
`length` is over it without being encoded (`exceedsBytes`).

### Decision 2: a frame over a limit gets its own refusal, echoing the id once

Each refusal is the reply that frame type already has, so a client handles it
as it handles any other refusal of that frame:

- **`sub`, id over the limit:** `reset` for that sub id. It is refused before
  anything else is checked or read. No sub can hold such an id, so nothing is
  removed or stored.
- **`fetch`, id over the limit:** an empty `page` with `seq: "0"`, as for an
  unknown collection, before anything is read.
- **`mut` or `call`, `txId` over the limit:** `rejected` with code
  `LIMIT_EXCEEDED` and the message `txId is over 256 bytes`. It is refused
  before the dedup lookup and is **not** recorded. ADR-0025 runs every per
  transaction rejection after the dedup lookup, so that a resent `txId` keeps its
  stored outcome. A `txId` over the limit can have no stored outcome, because
  none is ever recorded under one, so there is nothing for the lookup to find.
  The one exception is a receipt recorded under such an id before this change,
  within one `dedupRetentionMs` of deploying it: a resend of that `txId` is now
  refused instead of replayed.
- **`unsub`, id over the limit:** ignored. No sub can hold the id.
- **A name over the limit:** a `mut` gets `rejected` with `LIMIT_EXCEEDED` and
  `collection name is over 128 bytes`, and a `call` gets `command name is over
  128 bytes`. Neither message quotes the name. The `txId` is within its limit
  here, so this is an ordinary per transaction rejection: it runs after the
  dedup lookup and first in `#applyMut` and `#applyCall`, and is recorded as
  every rejection is. A `sub` or `fetch` needs no check of its own: no
  collection can have such a name, so it is refused as an unknown collection
  already is, with nothing stored.

A frame over more than one limit gets one answer: the id is checked first, then
the name, then everything else.

The reply echoes the id once. That costs about what the client spent to send
it, and the problem was the echo in every delta, the storage and the log lines,
none of which remain. Log lines clip the id as before. Two other answers were
considered:

- **Drop the frame as D1 does, with no reply.** A raw client would wait for ever
  for a `sub`, and until its timeout for anything else. A library app with an
  app supplied transaction id over the limit would wait `timeoutMs` and then see
  a vague `confirmation timeout`. ADR-0025 chose a reply over a drop for the empty
  op key for the same reason, and the amendment above says a deliberate refusal
  always gets its reply.
- **A reply with a clipped id.** The client matches a reply to its request by
  the id, so a clipped id matches nothing, and the client waits as it would for
  a drop.

**Breaking.** A raw client whose ids are over 256 bytes, or whose collection or
command names are over 128, is now refused. An app that supplies a transaction
id over 256 bytes now has the transaction rejected with `LIMIT_EXCEEDED`. A
schema with a table or command name over 128 bytes now fails to register.

Pinned by `tests/id-limits.test.ts` (raw frames at and one byte over each limit,
in one, two and four byte characters), `tests/id-limits-client.test.ts` (the real
client with a table and a command name at 128 bytes, and a watch refused for its
id), and `tests/registry.test.ts`.
