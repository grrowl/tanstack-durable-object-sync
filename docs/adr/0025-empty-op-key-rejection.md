# 0025 — An empty `mut` op key is rejected with a reply, after the dedup lookup

**Status:** Accepted. Amends ADR-0012 D1 for the `mut` op `key` only. Makes
ADR-0014 D3's delete-bullet claim true.
**Date:** 2026-09-25

## Context

ADR-0012 D1 says the shape guard checks required fields "for correct type and
non-emptiness", and drops a failing frame with a log line and **no reply**.
ADR-0014 D3 relies on this: a `delete` needs no schema because "the wire layer
already checks the key is a non-empty string". The guard never did that for op
keys. It checked only `typeof key === "string"`, so `key: ""` reached the
author's handlers.

An op key is the row's pk, and the pk is client-supplied TEXT (ADR-0001 D9,
ADR-0007). `""` is never a real row identity, so it is always a client bug.
But it is an easy bug to hit through the normal API: TanStack DB accepts `""`
as a key, and `doCollectionOptions` forwards it unchanged. So
`coll.insert({ id: "" })` sends a well-formed `mut` whose only fault is the
empty key.

If the guard simply dropped that frame, as D1 prescribes, the client would get
no answer. It would wait out `timeoutMs` (5 s by default), fail with a generic
`confirmation timeout` that looks like a network fault, and only then roll
back. D1's drop fits frames our client never sends: a broken shape means a
hostile or broken peer, and often there is no usable `txId` to reply to. It
does not fit a value that ordinary API use can produce on an otherwise valid
frame.

## Decision

1. **The shape guard keeps the type check** (`key` must be a string). A
   non-string key is still dropped with no reply under D1.
2. **`#handleMut` rejects any op with `key === ""`** by sending `rejected`
   with code `VALIDATION` and the message "mutation op key must be a non-empty
   string". The check covers every op type and refuses the whole batch. It runs
   before authorize and before any row or change-log write, so nothing is
   applied, logged to `_sync_changes`, or broadcast. Only the rejection receipt
   is recorded in the dedup table. This follows the `LIMIT_EXCEEDED` pattern:
   a frame with a valid shape that the server refuses gets a reply. It reuses
   the existing `VALIDATION` code, so the client API gains nothing new.
3. **Order: dedup lookup first, then this check.** A resent `txId` must get its
   stored outcome (exactly-once, ADR-0002 C5). If the check ran first, a
   committed `txId` resent with an empty key would get a fresh rejection that
   contradicts its stored `committed` receipt. The rejection is recorded like
   any other, so a retry of the rejected `txId` replays it.

Out of scope: an insert's `cols[pk]` is not checked for emptiness or for
equality with `key`. That is a broader question about the pk model, and it is
tracked separately.

## Consequences

- A client that uses `""` as a pk now gets a prompt `MutationRejectedError`
  (`code: "VALIDATION"`) and an immediate rollback. Before, the write was
  accepted. This is a behaviour change for anyone who relied on it.
- ADR-0014 D3's delete bullet is now true, with the check in `#handleMut`
  instead of the shape guard.
- The existing `maxOpsPerMutation` check still runs before the dedup lookup,
  so it does not follow rule 3. A resent `txId` gets a fresh `LIMIT_EXCEEDED`
  instead of its stored receipt in two cases: the limit changed between the
  original send and the resend (for example, across a deploy), or the resend
  carries a different, over-limit payload under the same `txId`. Our client
  replays identical frames, so only the first case can reach it. It is left as
  a follow-up.
- Tests: `tests/empty-op-key.test.ts`.
