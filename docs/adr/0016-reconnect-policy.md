# 0016 — Transport reconnect policy: injectable delay function, terminal 4xxx, jittered backoff

**Status:** Accepted. Fixes issues #25 and #26. §3 amended 2026-11-17 (see the end).

## Context

`WebSocketTransport` auto-reconnects from two sites — the socket close handler
and the connect-failure catch — and both were blind to *why* the connection
went away: a fixed `reconnectDelayMs` (default 250 ms) rescheduled forever,
never inspecting `CloseEvent.code`. Two real failures fall out:

- **Terminal rejections retry forever (#25).** The DO's only client-readable
  auth-rejection pattern is accept-then-close with an application code (e.g.
  4403) — a failed HTTP handshake status is invisible to a browser WebSocket.
  The transport turned exactly that deliberate rejection into an infinite
  250 ms retry loop against the DO, with no way for the app to even observe
  the code.
- **Fixed-interval thundering herd (#26).** During a DO outage every client
  retries on the same fixed interval indefinitely; when the DO returns, they
  all arrive in lockstep. The option's own doc comment admitted it:
  "production should layer exponential backoff + jitter."

## Decision

### 1. The policy is a function, not a bag of flags

One option subsumes the whole space:

```ts
reconnectDelay?: number | ((attempt: number, closeCode?: number, closeReason?: string) => number | null)
```

The function form is called once per attempt with a 1-based counter; it
returns the delay in ms before the next attempt, or `null` to stop
reconnecting. The alternative — config knobs (`maxRetries`, `backoffFactor`,
`capMs`, `terminalCodes`, …) — grows a flag per policy nuance and still can't
express app-specific shapes ("retry 4408 rate-limit closes after the reason's
hinted delay"). A function is the minimal complete surface; the knobs become a
*default implementation* of it. The number form is the one sanctioned
shorthand: the default policy's base delay. It **replaces** `reconnectDelayMs`
(removed — pre-1.0, one option per concern; a fixed delay is spelled
`() => ms`).

`closeCode` stays `number` and `closeReason` stays `string` deliberately: the
4xxx range is *private-use* vocabulary that each application defines (this
library emits no close codes of its own), the reason is app-authored prose,
and a closed union at a network boundary is a promise the wire doesn't keep.
The closed set in this signature is the *return* — `number | null`, the
policy's actual decision.

Both reconnect sites route through one `scheduleReconnect(code?, reason?)`.
The connect-failure path has no close frame, so the policy (and `onClosed`)
see `undefined` there — honest, and distinguishable from a server-sent close.

### 2. Default: application closes (4000-4999) are terminal

The 4xxx range is reserved for application meaning: the server closed *on
purpose* and said why. Retrying a deliberate rejection cannot succeed and is
indistinguishable from an attack pattern from the DO's perspective. The
default policy returns `null` for any 4000-4999 code; everything else —
1000/1001 (hibernation, redeploy), 1006 (network drop), or a failed open —
stays transient and retries. Apps that use a 4xxx code as "please reconnect
later" opt out with a custom policy.

A terminal stop must be observable, or an auth rejection looks like a silent
hang. New hook:

```ts
onClosed?: (code: number | undefined, reason: string | undefined) => void
```

Called exactly when the policy returns `null` — never for an intentional
`close()`, never while retries continue. This replaces the previous workaround
(attaching a close listener inside a custom `open()` and calling
`transport.close()` before the transport's own handler ran — a
listener-ordering hack).

### 3. Default delay: capped exponential backoff, full jitter, reset on open

`defaultReconnectDelay(baseMs, capMs = 30_000)` returns a uniform delay in
`[0, min(cap, base·2^(attempt−1))]` — AWS-style *full* jitter, which
desynchronizes a fleet of clients that all lost the same DO at the same
moment. The cap never drops below `baseMs`, so a caller who set
`reconnectDelay: 60_000` is not silently truncated. The attempt counter
resets on a successful *open* (the demand-driven reconnect window makes
"first server frame" needlessly stricter — an open that immediately drops
re-enters backoff on the very next close anyway, one attempt later).

### Invariants preserved

- **`reconnecting` is set at SCHEDULING time**, not in the timer — the
  demand-driven connect window (a mutation racing the timer) must run the
  resubscribe path (pre-existing bug, ADR-0011 grill). `scheduleReconnect`
  sets it before consulting the policy; a *terminal* close also sets it, so an
  app-driven `connect()` after re-auth still resubscribes from the cursor.
- **No idle timers.** At most one reconnect timer exists, and only while a
  retry is pending: the handle is tracked and cancelled on a successful open,
  on a terminal stop, on `close()`, and when a newer drop supersedes it.
  Without this (adversarial-review catch), a stale timer from a transient
  drop could fire after a later terminal 4xxx close and resurrect the
  connection — retrying past "stop" and double-firing `onClosed`.

## Alternatives considered

- **Flag-based config** (`maxRetries`/`backoffFactor`/`terminalCodes`). Rejected
  above — a function is smaller and complete.
- **Rejecting `connect()` for a post-handshake 4xxx close.** The close arrives
  *after* `open()` resolved, so connect() has already succeeded by the time
  the code is known; faking a rejection would need a "wait for first frame"
  handshake phase. `onClosed` reports the same fact without restructuring the
  connect path.
- **Terminal = give up on any close code the app didn't allowlist.** Inverts
  the safe default: transient network drops (1006) are the overwhelmingly
  common case and must retry out of the box.

## Consequences

- **Behavior change:** a 4xxx server close no longer auto-reconnects. Code
  that relied on retry-after-4xxx must pass a custom `reconnectDelay`.
- **Breaking (pre-1.0):** `reconnectDelayMs: n` becomes `reconnectDelay: n`
  (same meaning: the default policy's base).
- Retry delays are now randomized and growing; tests that need a fast
  deterministic retry inject `reconnectDelay: () => smallMs` (the migrated
  `reconnectDelay: 20` fixtures stay valid — jitter only shortens them; a
  fixture that needs the timer to NEVER fire uses a fixed policy, since a
  jittered 60 s base can land near 0).
- The `open()`-listener workaround for observing close codes is obsolete;
  `onClosed` is the supported surface.
- **Future unification (deliberate deferral).** The three race guards that
  adversarial review accreted — the tracked reconnect timer, the
  stale-socket close-event guard (`this.ws !== ws`), and the close-epoch
  check after `await open()` — could collapse into one generation counter:
  *a continuation is only valid in the generation it was created in* (bump on
  close / install / schedule; check at each continuation head). That demotes
  a forgotten cleanup from "resurrected socket" to "one late no-op". Not done
  here: the current shape is pinned by the race tests and has passed two
  adversarial reviews; unify when this file is next touched, with those same
  tests as the harness.

## Amendment 2026-11-17: only a connection that stayed open resets the backoff

### What was wrong

§3 reset the attempt counter on every successful open. Its reason was that "an
open that immediately drops re-enters backoff on the very next close anyway,
one attempt later". That is false. The open had already set the counter to 0,
so the next close is attempt 1 again, never attempt 2. A server that accepts a
socket and then closes it holds every client at attempt 1: with the default
policy, a delay from 0 to 250 ms, then a new socket that resends every
subscription, for as long as the cause lasts. In a test, a real Durable
Object that answers each subscription and then closes with 1011 drew 93 dials
in 500 ms from one client whose policy starts at 2 ms and doubles.

The loop is reachable today. A Durable Object can throw on the first frame
after a deploy. ADR-0019 closes a socket whose restored predicate cannot be
compiled. A handler can fail on every resubscribe. In each case the failure
repeats while the cause persists, and every open succeeds.

### Decision

A connection proves healthy by **staying open for at least 30 seconds**. The
transport records the time when it installs a socket. When that socket closes
unexpectedly, the transport compares the time: if the socket lived at least
30 s, the counter goes back to 0 before the next attempt is counted, so a
later blip retries at the first-attempt delay as before. Otherwise the close
counts as one more failed attempt, the same as a failed open. A successful
open no longer resets anything. The check runs once, at close, so it needs no
timer.

The counter also goes back to 0 when a run of retries ends: on a terminal stop
(the policy returned `null`), on `close()`, and when the last unsubscribe
cancels a pending reconnect. The next run, whether an app-driven `connect()`
after re-auth, a revived transport (ADR-0020), or a new `subscribe`, starts at
attempt 1 as it did before. Without this a policy that gives up after N
attempts would give up at once on the next run.

### The candidates

A rule for "proven healthy" was compared against these loops: a server that
closes as soon as it accepts; one that answers every subscription and then
closes; one that answers some subscriptions and closes on the next; and one
that answers everything and closes later, for example on a replayed write or
on a call the app makes whenever it connects.

- **The first server frame arrived.** §3 named this and set it aside. It does
  not stop a server that answers one subscription and closes on the next, or
  any server that answers and then closes.
- **Every subscription in the resubscribe batch received its terminal.** This
  stops the partial answer. It does not stop a server that answers everything
  and then closes, which is the loop of a call the app makes on every connect
  that crashes the object. It also needs a set of pending ids per socket that
  must track subscriptions added or removed during the batch, and it says
  nothing about the first connection, which has no resubscribe batch.
- **The connection stayed open for a minimum time.** One timestamp per socket
  and one comparison at close. Every loop whose connections die before the
  threshold grows to the policy's cap. A loop whose connections outlive the
  threshold cannot dial faster than once per threshold, whatever the server
  does. This is the simplest rule and the only one of the three that stops
  every loop above. It is the decision.

### Where 30 seconds comes from

It is the default policy's cap. With the threshold equal to the cap, no
reconnect cycle runs faster than about one dial per 30 s once the counter has
grown: a cycle whose sockets die young reaches the cap, and a cycle whose
sockets live 30 s or longer is already that slow. A shorter threshold would
leave a server that closes just after it free to loop faster than the cap.

The cost falls on a connection that drops more than once within 30 s of
opening, for example on a flapping mobile link. Each such drop now doubles the
delay's ceiling instead of returning it to 250 ms. That is the backoff doing its job:
a link that cannot hold a socket for 30 s is failing. The threshold is fixed,
not an option, because no app has needed another value. A custom policy still
sees every attempt number and can shape its own delays.

### What does not change

- The policy keeps its signature. A custom policy sees larger attempt numbers
  in the looping case and nothing else.
- Terminal stops, `onClosed`, and the tracked reconnect timer are unchanged.
- A demand-driven connect (a mutation, fetch, or subscribe in the reconnect
  window) still dials at once and still supersedes the pending timer. Its
  socket is judged at close like any other.
- A cursor-regress reconnect (`forceReconnect`) still bypasses the policy and
  consumes no attempt. It no longer resets the counter either.
- The parked write deadline of ADR-0021 is a separate timer that starts at
  the first drop. It is not touched. In a looping case the longer delays make
  it more likely that a parked write settles with `ConnectionLostError` before
  a replay answers, which is the outcome ADR-0021 defines for that case.

### A later design

A proposed design (ADR-0026, not accepted) holds the cursor through a
resubscribe round and wants an abandoned round not to reset the backoff. This
rule gives that without knowing about rounds. Resets now happen only at a
close after 30 s and when a run of retries ends, so a socket abandoned within
30 s never resets the counter, whatever path abandons it. A round abandoned
after 30 s does reset it, but such a cycle already dials no faster than the
cap.
