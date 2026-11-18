# 0029 — First loads follow the reconnect policy

**Status** Accepted for this implementation, 2026-11-18.

## Decision

A transient outage keeps the original eager load or subset request alive. A decision
to stop automatic retries fails reads that still need their first result.
An intentional `transport.close()` pauses registered reads so a remount can
finish them. The owner releases a subscription with collection cleanup or raw
`unsubscribe()`.

| Event | Eager first load | On-demand subset | Data already ready |
| --- | --- | --- | --- |
| Dial fails, retry scheduled | 1.0 keeps waiting. Before 1.0 the first failed dial rejects the preload, and the eventual snapshot heals the collection. | The original query waits on every version. | Rows stay usable. |
| Policy returns `null` or throws | Reject and release the run's subscription. | Reject the affected load and evict the failed acquisition so a new request can try again. | Keep rows and subscriptions. On 1.0 a retained subscription reports the stop through collection health. |
| App closes the transport | Abort the handshake and keep the registration. | Pause registered watches and their page reads. | Keep rows and subscriptions. |
| App reconnects | Resume a paused load. A load failed by policy needs a new run. | Resume paused watches before replaying page reads. A failed query can be recreated. | Resubscribe from the existing cursor. A successful subscription boundary clears the reported interruption. |

Once a full snapshot has arrived and core has accepted its commit, local
application of that commit determines when the load finishes. If the policy
stops in that window, an eager run can report the stop through health once
its rows become visible.

A fresh on-demand source with no resume point needs no socket to become ready.
Each live query owns its load. A hydrated on-demand run with a resume point
waits until its catch-up frame has been sent. The catch-up registers before the
watches, including when core requests a subset during startup. A hydrated
eager collection can show its existing rows immediately.

## Why stop and close have different outcomes

The original auth case includes a server that accepts the upgrade and then
denies organization access. Another application can use an application close
code for an expired token it knows how to refresh. Frequency is unnecessary
to this choice. The application that recognizes refreshable credentials can
return a delay. A route awaiting an ordinary denial needs a rejection. Core
1.0 rejects a call to `reportError` before first ready, so collection health
cannot provide that outcome. Conversely, calling `fail` on the first
retryable dial ends the 1.0 sync run while the transport is still trying.

Explicit close has a different owner. In the mount, close, remount sequence
the app already intends to reuse the transport. Failing the first load there
left eager and on-demand queries disconnected from their server subscriptions.
For permanent disposal the app releases its collections and then closes the
socket. [Original auth case](https://github.com/grrowl/tanstack-durable-object-sync/issues/25)
and [transport reuse](https://github.com/grrowl/tanstack-durable-object-sync/issues/37).

## Transport and adapter contracts

- Each installed socket sends the registered subscriptions once, in registration
  order. Initial requests use their original predicate and resume point.
  Bootstrapped subscriptions use the existing shared cursor. An interrupted
  first snapshot restarts from scratch. A partial install that throws abandons
  the socket and goes through the policy.
- A raw `subscribe()` promise resolves at its first successful send or when
  the owner removes or replaces the registration first. It rejects for an
  invalid local request or a policy stop before the send. The registration
  survives a policy stop until its owner unsubscribes. The adapter releases
  failed first loads and retains ready registrations. `onStopped(error)`
  reaches a raw handler even if the send promise already resolved.
- `TransportStoppedError` carries the peer's `closeCode` and `closeReason`,
  or the failed dial/send as `cause`. When the policy throws, its exception
  becomes the cause. Outstanding reads settle before `onClosed(code, reason,
  error)` runs. The next demand can dial again. A direct failed dial while
  another retry timer is already armed leaves that timer and its budget intact.
- A covered fetch is queued until a socket carries it. Its response timeout
  starts on send. On an ordinary drop or intentional pause with a registered
  subscription, it replays after subscriptions and parked writes. A late
  reply cannot answer a replay on another socket. A policy stop rejects the
  fetch with the same error. Removing the last registration while disconnected
  rejects held fetches unless a dial is already in flight. A standalone fetch
  has no automatic retry. Cursor-regress reconnects retain their existing
  abandoned-page rule.
- The default policy can retry an unreachable server indefinitely. Apps that
  need a deadline supply a bounded policy. A failed eager load on 1.0 needs
  `reload()`, or cleanup followed by preload on older core. For on-demand
  requests, dispose and recreate the failed query. A later write can revive
  the connection but cannot reverse an already rejected promise.

## Compatibility and evidence

This amends the first-load rule in ADR-0028 D2 and the pre-1.0 contract in
ADR-0022. It also amends the hydration gate in ADR-0011 D3, terminal notification
in ADR-0016, read settlement at explicit close in ADR-0020, and acquisition
failure handling in ADR-0023. ADR-0021 still governs mutation receipts. The
server's per-subscription refusal and ADR-0026's proposed cursor hold are
separate decisions. A future cursor hold must include bootstrapped registrations
on a socket installed after an intentional pause.

In a browser, a refused HTTP upgrade exposes no application close code. It
follows the failed-dial policy, which by default continues retrying. An app
that needs a specific denial uses an accepted WebSocket with a close code.

The workerd auth integration drives a real accept-then-close denial before
the first snapshot. With the default terminal outcome, both an eager preload
and a mounted query reject. With the application retrying and the opener
awaiting credentials, the same loads complete with the seeded row. The
first-connect tests mount during an outage and pause an in-flight first dial.
The transport tests exercise a thrown policy, partial install, page replay and
stale replies. These cases run with the supported floor and locked core.
[Auth tests](../../tests/auth-path.test.ts) ·
[First-load tests](../../tests/first-connect-failure.test.ts) ·
[Transport tests](../../tests/connect-contract.test.ts).
