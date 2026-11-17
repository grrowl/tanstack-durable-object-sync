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

A connection proves healthy by **staying open for at least 5 seconds**. The
transport records the time when it installs a socket. When that socket goes
away without `close()`, the transport compares the time: if the socket lived
at least 5 s, the counter goes back to 0, so the next attempt is attempt 1.
Otherwise the counter is left as it is, and the next attempt counts on from
it, the same as after a failed open. A successful open resets nothing. The
check runs once, when the socket goes away, so it needs no timer.

Every way the current socket goes away is judged the same:

- an unexpected close while subscriptions are active, which then schedules a
  reconnect;
- an unexpected close with no subscriptions left, which does not reconnect.
  Its lifetime still counts, so a connection that worked for hours does not
  leave the count of an older outage for the next session;
- a cursor-regress reconnect (`forceReconnect`), which abandons the socket
  without a close event and still bypasses the policy.

The counter also goes back to 0 when the transport stops retrying: on a
terminal stop (the policy returned `null`) and on `close()`. The next run, an
app-driven `connect()` after re-auth or a revived transport (ADR-0020), starts
at attempt 1. Without this a policy that gives up after N attempts would give
up at once on the next run. This is a small change from before. Before, the
next run started at 1 only if its first dial opened. A dial that failed to
open was attempt N + 1. Now it is attempt 1 either way.

A moment with zero subscriptions is not a new run, and does not reset the
counter. An on-demand app releases one query and loads the next on each route
change, so during an outage the count passes through zero on every
navigation. Resetting there would restart the fast retries on each click, and
a policy that gives up after N attempts would never give up while the user
navigates.

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
  and one comparison when it goes away. This is the simplest rule and the only
  one of the three that stops every loop above. It is the decision.

A rule with no constant does not work. "Reset when the socket outlived the
delay before it" fails because full jitter can draw a delay near 0 at any
attempt, so a server that closes at once still resets often and loops hot.
Stopping a close on accept needs an absolute floor.

### Where 5 seconds comes from

The faults this guards against (a throw on the first frame, a failed
resubscribe, a failed replayed write) close the socket within the first round
trips. 5 s is the default confirmation timeout, the time the transport gives
a parked write to be answered after a reconnect. A socket that outlived it has
got past the work it was sent at open.

It is a separate constant, not `timeoutMs`. Apps set `timeoutMs` for other
reasons, from tens of milliseconds to a minute. Tying the threshold to it would
let an app that shortens its confirmation timeout reopen the hot loop without
knowing, and let one that lengthens it pay the flapping cost below. Nothing in
the transport bounds the resubscribe batch by `timeoutMs` either.

The bound, with both of its limits. Under the default policy, a server whose
sockets die within 5 s drives the counter to the cap, where each delay is
uniform from 0 to 30 s with a mean of 15 s, about four dials a minute per
client. A server that closes each socket just after 5 s gets one dial per
5 s plus a delay of at most 250 ms, about twelve a minute per client. That is
the worst any server can do to a quiet client. It is not a bound on dials an
app causes: a mutation, fetch or subscribe while disconnected dials at once,
outside the policy, as it did before this change.

### The cost

From a simulation of one hour, default policy, every dial succeeding, a link
that drops each socket at the same age:

| socket lifetime | connected share | drops whose delay exceeds 5 s |
|---|---|---|
| 1 s | 6% | 81% |
| 3 s | 17% | 81% |
| 6 s | 98.0% | 0% |
| 10 s | 98.8% | 0% |
| 15 s | 99.2% | 0% |
| 20 s | 99.4% | 0% |
| 29 s | 99.6% | 0% |

Before this change every row was about 99%. A link that cannot hold a socket
for 5 s now backs off to the cap, the same as a broken server, because the two
look the same from the client. A link that holds sockets longer than 5 s is
untouched.

A single blip soon after a long outage also pays. After two minutes of failed
dials the counter is about 15. If the first socket after the outage drops
within 5 s, the next delay is uniform from 0 to 30 s, mean 15 s. If it drops
after 5 s, the delay is at most 250 ms, as before.

For a write in flight at a drop, ADR-0021 parks it for `timeoutMs` (5 s by
default) from the first drop. When the delay exceeds that, the write settles
with `ConnectionLostError` instead of being answered by the replay. That
happens in a server loop and on a link that drops sockets younger than 5 s,
for about four drops in five once the counter is high. It does not happen on a
link whose sockets live longer than 5 s.

A custom policy keeps its signature but sees different numbers. It now counts
every socket that went away within 5 s of opening, not only failed dials, so a
policy of the shape "give up after N attempts" can give up on a working link
that drops sockets younger than 5 s: `onClosed` fires, parked writes fail with
`ConnectionLostError`, and the transport stays down until the app calls
`connect()`. The policy is not given the socket's lifetime, so it cannot tell
a young close from a healthy one or choose its own threshold.

### The clock

The rule reads `Date.now()`, the wall clock, on purpose. When a device sleeps
or a background tab is frozen, the close is seen late and the socket looks
old, so the counter resets and the wake retries at the first-attempt delay,
which is what a user wants. `performance.now()` can stop during suspension and
would make every wake look young. A step of the wall clock can make one close
look healthy or young once, which changes the outcome of that one
comparison. A server cannot cause any of this.

### What does not change

- Terminal stops, `onClosed`, and the tracked reconnect timer.
- A demand-driven connect (a mutation, fetch, or subscribe in the reconnect
  window) still dials at once and still supersedes the pending timer. It does
  not reset the counter; its socket is judged when it goes away, like any
  other.
- A cursor-regress reconnect still bypasses the policy and consumes no
  attempt.
- The parked write deadline of ADR-0021 is a separate timer that starts at
  the first drop. It is not touched; the cost above is about how often a
  reconnect now comes later than it.

### A later design

A proposed design (ADR-0026, not accepted) holds the cursor through a
resubscribe round and wants an abandoned round not to reset the backoff. This
rule gives that without knowing about rounds. The counter resets only when a
socket that lived 5 s goes away, and when the transport stops retrying, so a
socket abandoned within 5 s never resets it, whatever path abandons it. A
round abandoned after 5 s does reset it, but such a cycle already dials no
faster than once per 5 s per client. One condition belongs to that design: it
must abandon a round through the reconnect policy. A path that bypasses the
policy, as `forceReconnect` does, would skip the backoff entirely.
