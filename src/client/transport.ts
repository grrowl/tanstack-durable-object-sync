// WebSocketTransport — browser client, one per Durable Object.
//
// The single-cursor half of the inversion (ADR-0002 C1). It tracks exactly one
// position, `appliedSeq`, advanced only at commit boundaries (snap-end /
// uptodate / committed) — never on individual snap/d frames — so the cursor is
// always a contiguous applied prefix. There is no second ("acked") cursor.
//
// Confirmation rides the same stream: `sendMut`/`sendCall` resolve when the
// server's `committed` for that txId arrives. Because the server flushes a
// mutation's matched deltas before its `committed` (server-side C1 ordering),
// by the time `committed` is processed the deltas are already applied and
// `appliedSeq >= commitSeq` holds.
//
// The socket opener is injectable: the browser default is `new WebSocket(url)`;
// other runtimes (and tests) provide an already-connected socket.

import { createFrameCodec, type FrameCodec, type WireOut } from "../wire/frame-codec.ts"
import type { ClientFrame, RowOp, ServerFrame } from "../wire/frames.ts"

/** Minimal structural socket — satisfied by both browser WebSocket and a
 *  workerd accepted client socket, avoiding the DOM-vs-workers type clash. */
export interface WebSocketLike {
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, listener: (ev: { data?: unknown }) => void): void
  removeEventListener(type: string, listener: (ev: { data?: unknown }) => void): void
}

/** A collection adapter's view of one subscription's inbound frames. */
export interface SubHandler {
  onSnap(key: unknown, row: unknown): void
  onSnapEnd(): void
  onDelta(op: RowOp, key: unknown, cols: Record<string, unknown> | undefined): void
  /** `ownTerminal` is true only for a sub-scoped boundary addressed to THIS
   *  subscription (a catch-up's terminal, ADR-0011 D3) — a transient
   *  subscription may tear itself down on it, but never on a broadcast
   *  boundary, which can precede its own catch-up frames. */
  onUptodate(ownTerminal?: boolean): void
  onReset(): void
  /** The transport re-sent this subscription from scratch after a drop: it had
   *  not reached its first terminal, so it resubscribes without `since` and the
   *  snapshot that follows replaces whatever part of the first one arrived. */
  onRestart?(): void
}

/** The transport surface `doCollectionOptions` consumes — structural, so the
 *  WebSocket transport and the SSR snapshot transport are interchangeable
 *  (ADR-0011 D2). Generic + branded on `Api` so row/table typing survives the
 *  seam: `WebSocketTransport<Api>` and `SsrSnapshotTransport<Api>` both satisfy
 *  it, and `doCollectionOptions` still infers the collection set from `Api`. */
export interface Transport<Api = unknown> {
  /** phantom — carries `Api` through the structural interface so inference at
   *  `doCollectionOptions` recovers it (never `unknown`). */
  readonly __api?: Api
  connect(): Promise<void>
  subscribe(
    subId: string,
    collection: string,
    handler: SubHandler,
    where?: unknown,
    orderBy?: unknown,
    limit?: number,
    since?: string,
  ): Promise<void>
  unsubscribe(subId: string): void
  sendMut(frame: Extract<ClientFrame, { t: "mut" }>): Promise<{ result?: unknown }>
  fetch(frame: Extract<ClientFrame, { t: "fetch" }>): Promise<Array<unknown>>
  close(): void
  readonly appliedCursor: string
  /** True once this transport has established a stream position of its own —
   *  even position 0 (an SSR read against a DO with no history is a REAL
   *  claim: "no resume point", the honest-truncate route). False means no
   *  claim at all (a fresh browser transport) — exportSyncMeta then exports
   *  nothing rather than a spurious "0" that would win a MIN-merge against
   *  every real dehydrated cursor (ADR-0011 D3, merged-upstream semantics). */
  readonly hasPosition: boolean
  seedCursor(cursor: string): void
}

/** Cloudflare's inbound WebSocket edge cap, ~1 MiB (ADR-0018). An
 *  infrastructure FACT, not an application preference: both wire endpoints
 *  ship in this package and the only supported infra fixes the number, so it
 *  is a constant, not a knob — a knob could only lower it pointlessly or
 *  raise it into the edge cap's lie. If Cloudflare changes the cap, this
 *  constant changes with an ADR note. Mirrors the server's ADR-0012 default. */
const MAX_FRAME_BYTES = 1_048_576

export class MutationRejectedError extends Error {
  constructor(
    message: string,
    readonly code?: string,
  ) {
    super(message)
    this.name = "MutationRejectedError"
  }
}

/** Thrown by `connect()` — and so by any `await connect()` in `subscribe` /
 *  `sendMut` / `fetch` — when the transport was closed while a dial was in
 *  flight and was NOT revived by a later `connect()`. `connect()` never
 *  resolves having adopted no socket (issue #37): it re-dials when revived, or
 *  rejects with this typed, catchable error so a collection fails its ready
 *  gate loud (do-collection routes it to `markError`) instead of resolving a
 *  silently-empty collection or floating an unhandled rejection. The default
 *  `open()` also rejects with it when `close()` aborts an in-flight handshake
 *  (issue #38 / ADR-0020). */
export class TransportClosedError extends Error {
  constructor(message = "transport closed during connect") {
    super(message)
    this.name = "TransportClosedError"
  }
}

/** Settles an in-flight `mut`/`call` whose socket dropped UNEXPECTEDLY before the
 *  server's `committed`/`rejected` receipt arrived — the outcome is genuinely
 *  unknown to the client (issue #39 / ADR-0021). Distinct from every other
 *  settlement so an app can hold its optimistic overlay instead of flashing a
 *  rollback of a write the server may already have committed:
 *
 *  - not a `MutationRejectedError` — the server did NOT reject it;
 *  - not the generic `Error("confirmation timeout: …")` — the socket died, it
 *    did not stay open and go quiet;
 *  - not a `TransportClosedError` — that is a `connect()`-time failure, not a
 *    settled-in-flight mutation.
 *
 *  It is a FALLBACK: when the transport reconnects (subscriptions active,
 *  non-terminal close) it first replays each in-flight txId through the server's
 *  dedup table, so the TRUE outcome (`committed`/`rejected`) wins whenever it
 *  arrives within the parked window. This error surfaces only when no reconnect
 *  can resolve it (terminal 4xxx close, no active subscriptions) or the replay
 *  does not answer before the parked timeout. The type is the contract: an app
 *  catches it to keep the overlay pending until resubscribe converges the row. */
export class ConnectionLostError extends Error {
  constructor(
    message = "connection lost before the server's receipt arrived; outcome unknown",
    readonly txId?: string,
  ) {
    super(message)
    this.name = "ConnectionLostError"
  }
}

/** Reconnect delay policy (ADR-0016). Called once per reconnect attempt with
 *  the 1-based attempt number (reset to 1 after each successful open) and,
 *  when the drop came from a socket close, that close's code/reason (a failed
 *  `open()` has no close frame, so both are undefined). Return the delay in ms
 *  before the next attempt, or `null` to stop reconnecting (terminal — the
 *  transport surfaces it via `onClosed`). */
export type ReconnectDelayFn = (attempt: number, closeCode?: number, closeReason?: string) => number | null

/** The default reconnect policy: capped exponential backoff with full jitter —
 *  a uniform delay in [0, min(cap, base·2^(attempt−1))], cap 30 s (never below
 *  the base) — and application close codes (4000-4999) are terminal: the
 *  server closed deliberately (e.g. an accept-then-close 4403 auth rejection),
 *  so retrying cannot succeed. */
export function defaultReconnectDelay(baseMs: number, capMs = 30_000): ReconnectDelayFn {
  const cap = Math.max(capMs, baseMs)
  return (attempt, closeCode) => {
    if (closeCode !== undefined && closeCode >= 4000 && closeCode <= 4999) return null
    return Math.random() * Math.min(cap, baseMs * 2 ** (attempt - 1))
  }
}

export interface TransportOptions {
  url: string
  /** Returns a CONNECTED socket. Default opens `new WebSocket(url)` and resolves
   *  on its `open` event. Tests/other runtimes inject a ready socket.
   *
   *  The optional `AbortSignal` is aborted by `close()` when the handshake is
   *  still in flight (issue #38 / ADR-0020): an `open` that honours it must
   *  close its in-flight socket and reject, so a still-CONNECTING socket is
   *  never leaked — including a handshake that never resolves on its own.
   *  Ignoring the signal stays backward-compatible (the transport still
   *  discards and closes the socket if `open()` ever resolves), but a
   *  never-resolving handshake then leaks; custom openers SHOULD honour it. */
  open?: (signal?: AbortSignal) => WebSocketLike | Promise<WebSocketLike>
  codec?: FrameCodec
  /** Confirmation/await timeout in ms. */
  timeoutMs?: number
  /** Reconnect pacing. A number is the base delay (ms) for the default
   *  jittered-backoff policy (`defaultReconnectDelay`) — the attempt-1 jitter
   *  ceiling. A function is the full policy; return `null` to stop
   *  reconnecting. Default: `defaultReconnectDelay(250)`. A truly fixed delay
   *  is a policy too: `() => 500`. */
  reconnectDelay?: number | ReconnectDelayFn
  /** Called when an unexpected drop is TERMINAL — the policy returned `null`
   *  (default: any 4000-4999 application close, e.g. an auth rejection) — so
   *  the app can tell "re-auth needed" from a transient blip the transport is
   *  still retrying. Never called for an intentional `close()`. */
  onClosed?: (code: number | undefined, reason: string | undefined) => void
}

interface SeqWaiter {
  target: bigint
  resolve: () => void
  reject: (e: Error) => void
  timer: ReturnType<typeof setTimeout>
}

interface TxWaiter {
  resolve: (r: { result?: unknown }) => void
  reject: (e: Error) => void
  timer: ReturnType<typeof setTimeout>
  /** The encoded frame, retained so an unexpected close can replay it through the
   *  server's dedup table on reconnect (issue #39 / ADR-0021). */
  frame: WireOut
  /** True once an unexpected close has swapped this waiter's generic-timeout timer
   *  for the bounded ConnectionLostError timer and marked it for replay-on-reconnect. */
  parked: boolean
}

// --- Typed-command projection over the schema Api (`typeof schema`) ---------
// Structural-only: the client imports the Api *as a type*, so these recover the
// command map and each command's Args/Result from the phantom carriers the
// server's `CommandEntry` attaches — no server import, nothing at runtime.
type CommandsOf<Api> = Api extends { commands: infer C } ? C : Record<never, never>
/** Callable command names. `""` is excluded: `wellFormed` drops a call frame
 *  with an empty name, so a `""` command would hang on the wire; making it
 *  uncallable here turns that footgun into harmless dead code at no runtime cost. */
type CommandName<Api> = Exclude<keyof CommandsOf<Api> & string, "">
type ArgsOf<Entry> = Entry extends { __args?: infer A } ? A : never
type ResultOf<Entry> = Entry extends { __result?: infer R } ? R : never
/** The `transport.call.*` proxy: one method per command, args + result typed.
 *  `""` is remapped away for the same reason as `CommandName`. */
type CallProxy<Api> = {
  [K in keyof CommandsOf<Api> as K extends "" ? never : K]: (
    args: ArgsOf<CommandsOf<Api>[K]>,
  ) => Promise<ResultOf<CommandsOf<Api>[K]>>
}

export class WebSocketTransport<Api = unknown> {
  /** Phantom brand tying the transport to its schema `Api`, so a transport for
   *  one DO is not assignable where another DO's schema is expected. `declare`
   *  keeps it type-only — no runtime field. */
  declare readonly __api?: Api
  private ws: WebSocketLike | null = null
  private connectPromise: Promise<void> | null = null
  private readonly codec: FrameCodec
  private readonly timeoutMs: number
  private readonly open: (signal?: AbortSignal) => WebSocketLike | Promise<WebSocketLike>
  /** The in-flight handshake's abort controller, so close() can tear down a
   *  socket whose open() has not yet resolved (issue #38 / ADR-0020). Null
   *  whenever no dial is parked on `await open()`. */
  private openAbort: AbortController | null = null

  /** `bootstrapped`: the sub reached its first terminal (`snap-end`, or its own
   *  catch-up `uptodate`). Only a bootstrapped sub may resume from the shared
   *  cursor: the cursor says nothing about rows a sub never received — other
   *  subs may have advanced it. Until then a sub given an explicit `since`
   *  (SSR hydration) resumes from THAT, and any other restarts from a snapshot. */
  private readonly handlers = new Map<
    string,
    {
      handler: SubHandler
      collection: string
      where?: unknown
      orderBy?: unknown
      limit?: number
      since?: string
      bootstrapped: boolean
    }
  >()
  private appliedSeq = 0n
  private readonly seqWaiters: Array<SeqWaiter> = []
  private readonly pendingTx = new Map<string, TxWaiter>()
  private readonly pendingFetches = new Map<
    string,
    { resolve: (rows: Array<unknown>) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }
  >()
  /** Suppresses auto-reconnect after an intentional close(). */
  private intentionallyClosed = false
  /** True while reconnecting, so connect() resubscribes on success. */
  private reconnecting = false
  private readonly reconnectDelay: ReconnectDelayFn
  private readonly onClosed?: (code: number | undefined, reason: string | undefined) => void
  /** Consecutive reconnect attempts since the last successful open; the
   *  1-based value passed to the policy. */
  private reconnectAttempt = 0
  /** The one pending reconnect timer, so a successful open, a terminal stop,
   *  or close() can cancel it — a stale timer from an earlier transient drop
   *  must never fire a further attempt after any of those. */
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  /** Bumped by close(). A connect() body captures it before awaiting open();
   *  a mismatch after the await means close() ran mid-flight — the resolved
   *  socket must be discarded, not installed. (close() can cancel the pending
   *  timer, but not a connect() body already parked on a slow open() — without
   *  this, that body resurrects a live, resubscribed socket after teardown.
   *  An epoch rather than checking intentionallyClosed, because an explicit
   *  connect() AFTER close() is allowed and must still install its socket.) */
  private closeEpoch = 0

  constructor(opts: TransportOptions) {
    this.codec = opts.codec ?? createFrameCodec()
    this.timeoutMs = opts.timeoutMs ?? 5000
    this.reconnectDelay =
      typeof opts.reconnectDelay === "function" ? opts.reconnectDelay : defaultReconnectDelay(opts.reconnectDelay ?? 250)
    this.onClosed = opts.onClosed
    this.open =
      opts.open ??
      ((signal?: AbortSignal) =>
        new Promise<WebSocketLike>((resolve, reject) => {
          const ws = new (globalThis as unknown as { WebSocket: new (u: string) => WebSocketLike }).WebSocket(opts.url)
          const onAbort = (): void => {
            // close() landed mid-handshake: abort the still-CONNECTING socket so
            // it is never leaked in CONNECTING forever (issue #38), and reject
            // typed so the parked connect() unwinds instead of hanging.
            try {
              ws.close()
            } catch {
              /* already dead */
            }
            reject(new TransportClosedError())
          }
          if (signal) {
            if (signal.aborted) return onAbort()
            signal.addEventListener("abort", onAbort, { once: true })
          }
          ws.addEventListener("open", () => resolve(ws))
          ws.addEventListener("error", () => reject(new Error("websocket error")))
        }))
  }

  /** Highest committed position the client has applied (stringified bigint). */
  get appliedCursor(): string {
    return String(this.appliedSeq)
  }

  /** A live transport's position exists once anything has advanced (or seeded)
   *  the cursor — it can never claim position 0 (seedCursor("0") is a no-op),
   *  so 0 here honestly means "no claim yet". */
  get hasPosition(): boolean {
    return this.appliedSeq !== 0n
  }

  /**
   * Claim a cursor position on behalf of externally-applied state — SSR
   * hydration (ADR-0011 D3). The hydrated rows ARE the stream's prefix up to
   * the dehydrated cursor, so claiming it keeps a bootstrap-window reconnect
   * from re-snapshotting over them (a fresh snapshot carries no tombstones, so
   * a row deleted server-side meanwhile would never be removed).
   *
   * The claim only ever SHRINKS relative to live progress: claiming a shorter
   * applied prefix is always safe; claiming a longer one without data never
   * is. A seed below the current position (a late streamed chunk — upstream
   * has already applied its possibly-stale rows; there is no veto) regresses
   * the cursor and resubscribes, so the catch-up replay re-freshens exactly
   * the clobbered window. Replay is idempotent: latest-op-per-key, applied as
   * upserts/deletes.
   */
  seedCursor(cursor: string): void {
    const c = BigInt(cursor) // malformed cursor throws — fail loud, never guess
    if (c <= 0n) return // "0" honestly means: no resume point to claim
    if (c >= this.appliedSeq && this.appliedSeq !== 0n) return // never grow the claim
    const wasLive = this.appliedSeq !== 0n && this.ws !== null
    this.appliedSeq = c
    if (wasLive && this.handlers.size > 0) {
      // A live regress cannot replay on the SAME socket: boundary frames the
      // server already sent (full duplex) would dispatch after the regress
      // and re-advance the cursor past the repair window — then a drop
      // resumes beyond it and the late chunk's clobbered rows stay stale
      // forever. Force a reconnect instead: the old socket stops speaking for
      // the stream (message dispatch is identity-guarded), and the FRESH
      // socket resubscribes from the seed — clean ordering, replay guaranteed.
      this.forceReconnect()
    }
  }

  /** Abandon the current socket and reconnect NOW. A cursor-regress reconnect
   *  is voluntary — not a network failure — so it bypasses the backoff policy
   *  (no attempt consumed, no delay, and a custom policy cannot declare it
   *  terminal) but still runs the resubscribe path. A failed open falls back
   *  into the normal policy-driven retry via connect()'s failure path. */
  private forceReconnect(): void {
    const old = this.ws
    this.ws = null // the identity guards now ignore the old socket entirely
    this.connectPromise = null
    try {
      old?.close()
    } catch {
      /* already dead; the reconnect proceeds regardless */
    }
    this.reconnecting = true
    this.clearReconnectTimer()
    void this.connect().catch(() => {
      /* retries route through connect()'s failure path (policy-driven) */
    })
  }

  async connect(): Promise<void> {
    if (this.ws) return
    if (this.connectPromise) return this.connectPromise
    // Dialing is the clearest statement of intent to be connected: clear the
    // intentional-close latch so a transport revived after close() (connection
    // pools do this) auto-reconnects on a later drop AND delivers onClosed
    // again (issue #37). This is orthogonal to the close-epoch guard below,
    // which still discards any socket opened before THIS dial's close.
    this.intentionallyClosed = false
    const p = (async (): Promise<void> => {
      const epoch = this.closeEpoch
      const controller = new AbortController()
      this.openAbort = controller
      let ws: WebSocketLike
      try {
        ws = await this.open(controller.signal)
      } finally {
        // Release our handle the instant open() settles — before the epoch
        // check / install — so a close() during install-processing never aborts
        // the socket we are about to adopt (it bumps closeEpoch instead).
        if (this.openAbort === controller) this.openAbort = null
      }
      if (epoch !== this.closeEpoch) {
        // close() ran while open() was in flight: the transport was torn down.
        // Discard the orphan instead of installing it — it must never speak for
        // the stream.
        try {
          ws.close()
        } catch {
          /* ignore */
        }
        // connect() must never RESOLVE disconnected (issue #37). If the
        // transport was closed and stays closed, reject typed. If a later
        // connect() revived it (the latch is clear), defer to whichever dial
        // now owns the transport so our awaiters ride the socket it installs.
        if (this.intentionallyClosed) throw new TransportClosedError()
        return this.connect()
      }
      // Browsers default WebSocket.binaryType to "blob"; force "arraybuffer" so
      // binary frames arrive as ArrayBuffer (workerd already does). Without this
      // the codec can't decode and every server frame is silently dropped.
      try {
        ;(ws as { binaryType?: string }).binaryType = "arraybuffer"
      } catch {
        /* some socket impls don't expose binaryType; codec handles AB/Uint8Array */
      }
      ws.addEventListener("message", (ev) => {
        // Only the CURRENT socket speaks for the STREAM. An abandoned socket
        // (forceReconnect regress, a superseded reconnect) can still deliver
        // queued frames — applying its stream frames, or worse advancing the
        // cursor on them, would claim positions the fresh socket's replay is
        // about to own (ADR-0011 D3). Dropped stream frames are re-covered by
        // the resubscribe catch-up from the applied cursor, idempotently.
        // Mutation receipts (`committed`/`rejected`) are NOT re-covered by any
        // replay, so a stale socket may still settle those waiters — it just
        // never advances the cursor (codex review: a committed mutation must not
        // be reported as timed out because a late hydration chunk forced a
        // reconnect first). A `page` is a snapshot, not an outcome: the fresh
        // socket's replay may already have deleted a row it carries, so a stale
        // page FAILS its fetch rather than resurrect that row (ADR-0023).
        this.onMessage(ev.data, this.ws !== ws)
      })
      ws.addEventListener("close", (ev) => {
        // Only the CURRENT socket's close may detach/reconnect. A stale
        // socket's late close (delivered after close()+connect() installed a
        // fresh socket) must not null the live connection (codex review).
        if (this.ws !== ws) return
        this.ws = null
        this.connectPromise = null
        if (this.intentionallyClosed) return // close() owns pendingTx teardown
        // Auto-reconnect on an unexpected drop while subscriptions are active.
        if (this.handlers.size > 0) {
          // Hold in-flight mut/call for dedup replay on reconnect (issue #39)
          // BEFORE scheduling — a terminal policy then fails them typed below.
          this.parkPendingForReplay()
          const { code, reason } = ev as { code?: number; reason?: string }
          this.scheduleReconnect(code, reason)
        } else {
          // No subscriptions → ADR-0016 does not reconnect, so no replay can ever
          // learn the outcome: settle the in-flight mut/call now, typed (issue #39).
          this.failPendingConnectionLost()
        }
      })
      this.ws = ws
      // A successful open resets the backoff: a later blip starts from the
      // policy's first-attempt delay again, not the accumulated one. It also
      // supersedes any pending timer (a demand-driven connect beat it).
      this.reconnectAttempt = 0
      this.clearReconnectTimer()
      // On a reconnect, re-establish every subscription from our single applied
      // cursor so the server serves a windowed catch-up rather than a snapshot.
      if (this.reconnecting) {
        this.reconnecting = false
        this.resubscribeAll()
        // Replay any in-flight mut/call parked by the drop, so the dedup table
        // answers each with its true recorded outcome (issue #39 / ADR-0021).
        this.resendParkedTx()
      }
    })()
    this.connectPromise = p
    // A socket that never OPENED fires no close event, so the close-handler
    // recovery path (above) can't run. Clear the cached rejection so the next
    // connect() starts fresh, and re-arm the timer while subscriptions are
    // live — otherwise one unreachable attempt wedges the transport forever.
    // Only the dial that still OWNS connectPromise drives recovery: a superseded
    // dial (a revived transport's earlier attempt that re-dialed via the epoch
    // branch above) must not null a newer attempt's promise or double-schedule
    // its reconnect — that newer attempt owns its own recovery.
    p.catch(() => {
      if (this.connectPromise !== p) return
      this.connectPromise = null
      if (!this.intentionallyClosed && this.handlers.size > 0) {
        // A socket that never opened has no close frame — the policy sees an
        // undefined code (backoff, under the default).
        this.scheduleReconnect()
      }
    })
    return p
  }

  /** Consult the policy and either arm the next reconnect attempt or stop. */
  private scheduleReconnect(closeCode?: number, closeReason?: string): void {
    // The flag is set at SCHEDULING time, not in the timer: a demand-driven
    // connect() (a mutation inside the reconnect window) may establish the
    // fresh socket first, and it must run the resubscribe path too — or every
    // subscription is silently dead on the new socket and the late timer
    // wedges the flag (pre-existing bug, found in the ADR-0011 grill). It is
    // also set on a TERMINAL close, so an app-driven connect() after e.g.
    // re-auth still resubscribes from the cursor.
    this.reconnecting = true
    // At most one pending attempt: a newer drop supersedes an older timer.
    this.clearReconnectTimer()
    const delay = this.reconnectDelay(++this.reconnectAttempt, closeCode, closeReason)
    if (delay === null) {
      // Terminal (default: application close 4000-4999, e.g. an
      // accept-then-close auth rejection): retrying cannot help — surface the
      // close to the app instead of looping against the DO.
      this.onClosed?.(closeCode, closeReason)
      // No reconnect will ever replay these — settle in-flight mut/call typed now
      // rather than let the parked timer wait out timeoutMs (issue #39).
      this.failPendingConnectionLost()
      return
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      void this.connect().catch(() => {
        /* next attempt retries via the connect-failure path above */
      })
    }, delay)
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  /** Re-send a `sub` for every registered subscription. A bootstrapped sub
   *  carries `since` (catch-up from the cursor). One that never reached its
   *  first terminal must not: the shared cursor may have been advanced by other
   *  subs, and a catch-up from it would skip changes it never received — its
   *  load would never settle. An explicit catch-up resumes from its own `since`
   *  (replaying a catch-up is idempotent); any other restarts from a snapshot. */
  private resubscribeAll(): void {
    const cursor = this.appliedCursor
    for (const [subId, entry] of this.handlers) {
      if (!entry.bootstrapped && entry.since === undefined) entry.handler.onRestart?.()
      // A late hydration chunk may have REGRESSED the cursor below a pending
      // catch-up's own `since` (seedCursor): then the lower one is the resume
      // point, so the replay re-covers the clobbered window.
      const since = entry.bootstrapped
        ? cursor
        : entry.since !== undefined && this.appliedSeq > 0n && this.appliedSeq < BigInt(entry.since)
          ? cursor
          : entry.since
      this.sendFrame({
        t: "sub",
        subId,
        collection: entry.collection,
        where: entry.where,
        orderBy: entry.orderBy,
        limit: entry.limit,
        since,
      })
    }
  }

  /** Re-send every parked mut/call frame after a reconnect so the server's dedup
   *  table answers each replayed txId with its recorded outcome — the
   *  reconciliation half of issue #39 (ADR-0021). A txId the server never saw
   *  (dropped before it was received) simply executes now; either way the parked
   *  waiter settles with the TRUE outcome instead of a spurious rollback. Replay
   *  stays inside the dedup window: the parked timer bounds the hold to timeoutMs
   *  (≪ dedupRetentionMs), so a resend only fires while the recorded outcome is
   *  still present. Runs on the FRESH socket (this.ws already installed). */
  private resendParkedTx(): void {
    if (!this.ws) return
    for (const [, w] of this.pendingTx) {
      if (!w.parked) continue
      try {
        this.sendRaw(w.frame)
      } catch {
        /* socket died again mid-resend; the parked timer still bounds the wait */
      }
    }
  }

  /** An unexpected close with a reconnect pending: HOLD each in-flight mut/call
   *  for dedup replay rather than let its confirmation timeout fire a spurious
   *  rollback of a write the server may already have committed (issue #39). Swap
   *  the generic-timeout timer for a bounded ConnectionLostError timer so the wait
   *  can't outlast timeoutMs — if the reconnect+replay answers first the true
   *  outcome wins; otherwise the app gets the typed unknown-outcome error, never a
   *  plain timeout. Already-parked waiters keep the first drop's bounded timer, so
   *  a flapping connection can't extend the hold past timeoutMs from the first drop. */
  private parkPendingForReplay(): void {
    for (const [txId, w] of this.pendingTx) {
      if (w.parked) continue
      clearTimeout(w.timer)
      w.parked = true
      w.timer = setTimeout(() => {
        this.pendingTx.delete(txId)
        w.reject(new ConnectionLostError(`connection lost before receipt: txId=${txId}`, txId))
      }, this.timeoutMs)
    }
  }

  /** Settle every in-flight mut/call with the typed unknown-outcome error — used
   *  when no reconnect will resolve them: a terminal 4xxx close, or an unexpected
   *  drop with no active subscriptions (ADR-0016 reconnects only for live subs). */
  private failPendingConnectionLost(): void {
    for (const [txId, w] of this.pendingTx) {
      clearTimeout(w.timer)
      w.reject(new ConnectionLostError(`connection lost before receipt: txId=${txId}`, txId))
    }
    this.pendingTx.clear()
  }

  close(): void {
    this.intentionallyClosed = true
    this.closeEpoch++
    // Abort an in-flight handshake so a still-CONNECTING socket is closed now,
    // not leaked until (or unless) its open() eventually resolves (issue #38).
    // A signal-honouring open() closes its socket and rejects; the epoch guard
    // in connect() is the backstop for one that ignores the signal.
    this.openAbort?.abort()
    this.openAbort = null
    this.clearReconnectTimer()
    for (const w of this.seqWaiters.splice(0)) {
      clearTimeout(w.timer)
      w.reject(new Error("transport closed"))
    }
    for (const [, w] of this.pendingTx) {
      clearTimeout(w.timer)
      w.reject(new Error("transport closed"))
    }
    this.pendingTx.clear()
    for (const [, w] of this.pendingFetches) {
      clearTimeout(w.timer)
      w.reject(new Error("transport closed"))
    }
    this.pendingFetches.clear()
    try {
      this.ws?.close()
    } catch {
      /* ignore */
    }
    this.ws = null
    this.connectPromise = null
  }

  async subscribe(
    subId: string,
    collection: string,
    handler: SubHandler,
    where?: unknown,
    orderBy?: unknown,
    limit?: number,
    /** Resume point for the FIRST sub — SSR hydration's dehydrated cursor
     *  (ADR-0011 D3). One-shot: reconnects resume from `appliedCursor`. */
    since?: string,
  ): Promise<void> {
    this.handlers.set(subId, { handler, collection, where, orderBy, limit, since, bootstrapped: false })
    await this.connect()
    // Unsubscribed while the connect was in flight (its `unsub` had no socket
    // to ride): sending now would register a ghost subscription the server
    // persists (ADR-0019) with no local handler — dead weight against the
    // sub cap until the socket drops (codex review).
    if (!this.handlers.has(subId)) return
    this.sendFrame({ t: "sub", subId, collection, where, orderBy, limit, since })
  }

  unsubscribe(subId: string): void {
    this.handlers.delete(subId)
    if (this.ws) {
      this.sendFrame({ t: "unsub", subId })
      return
    }
    // Disconnected/reconnecting and the LAST subscription just went away: ADR-0016
    // does not reconnect for zero subs, so a pending reconnect has nothing to
    // resubscribe — and it must never silently reconnect just to REPLAY a parked
    // mut/call (that would contradict ADR-0021's no-subs fallback). Cancel a
    // still-pending reconnect timer, clear the reconnecting flag so any handshake
    // already in flight installs WITHOUT resubscribing or replaying, and settle the
    // parked in-flight mut/call typed now (issue #39 / ADR-0021). A dial already
    // parked on open() is not force-aborted here — that would entangle ADR-0020's
    // epoch/revival machinery; at worst it installs an idle, demand-less socket
    // (as an initial connect also can), which the app disposes via close().
    if (this.handlers.size === 0) {
      this.clearReconnectTimer()
      this.reconnecting = false
      this.failPendingConnectionLost()
    }
  }

  sendMut(frame: Extract<ClientFrame, { t: "mut" }>): Promise<{ result?: unknown }> {
    return this.sendAwaitingReceipt(frame, frame.txId)
  }

  /**
   * Invoke a server command by name. Builds the `call` frame internally and
   * generates the txId (`crypto.randomUUID()`) — the app no longer supplies one.
   * Name + args are checked and the result is inferred against the schema `Api`.
   * Resolves with the command's result when its `committed` receipt arrives.
   */
  async sendCall<K extends CommandName<Api>>(
    name: K,
    args: ArgsOf<CommandsOf<Api>[K]>,
  ): Promise<ResultOf<CommandsOf<Api>[K]>> {
    const txId = crypto.randomUUID()
    const { result } = await this.sendAwaitingReceipt({ t: "call", txId, name, args }, txId)
    return result as ResultOf<CommandsOf<Api>[K]>
  }

  /** Sugar over `sendCall`: `transport.call.clearRoom({ … })`. One method per
   *  command on the schema `Api`; a Proxy forwarding to `sendCall`. */
  readonly call: CallProxy<Api> = new Proxy(
    {},
    {
      get:
        (_t, name: string) =>
        (args: unknown): Promise<unknown> =>
          this.sendCall(name as CommandName<Api>, args as ArgsOf<CommandsOf<Api>[CommandName<Api>]>),
    },
  ) as CallProxy<Api>

  /** One-shot page fetch; resolves with the page's rows. No live subscription. */
  async fetch(frame: Extract<ClientFrame, { t: "fetch" }>): Promise<Array<unknown>> {
    await this.connect()
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingFetches.delete(frame.fetchId)
        reject(new Error(`fetch timeout: ${frame.fetchId}`))
      }, this.timeoutMs)
      this.pendingFetches.set(frame.fetchId, { resolve, reject, timer })
      this.sendFrame(frame)
    })
  }

  /** Resolves once `appliedSeq >= target`. */
  awaitSeq(target: string): Promise<void> {
    const t = BigInt(target)
    if (this.appliedSeq >= t) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this.seqWaiters.findIndex((w) => w.resolve === resolve)
        if (i >= 0) this.seqWaiters.splice(i, 1)
        reject(new Error(`awaitSeq timeout: target=${target}`))
      }, this.timeoutMs)
      this.seqWaiters.push({ target: t, resolve, reject, timer })
    })
  }

  private async sendAwaitingReceipt(frame: ClientFrame, txId: string): Promise<{ result?: unknown }> {
    // Pre-send size guard (ADR-0018): the reliable half of oversize handling.
    // Cloudflare's edge caps inbound WS messages at ~1 MiB, so an oversize
    // frame may never reach the DO — waiting for a server rejection would just
    // be the confirmation timeout. Reject here, typed and immediate, so the
    // optimistic overlay rolls back promptly. Encode once; the bytes are
    // reused for the actual send.
    const encoded = this.codec.encode(frame)
    // A string frame (the JSON debug codec) goes over the wire as UTF-8 —
    // measure bytes, not UTF-16 code units, or non-ASCII payloads undercount
    // and slip past the guard only to die at the edge cap (codex review).
    const bytes = typeof encoded === "string" ? new TextEncoder().encode(encoded).byteLength : encoded.byteLength
    if (bytes > MAX_FRAME_BYTES) {
      throw new MutationRejectedError(
        `frame too large (${bytes} bytes > the ${MAX_FRAME_BYTES}-byte inbound WebSocket edge cap)`,
        "FRAME_TOO_LARGE",
      )
    }
    await this.connect()
    // A txId identifies exactly ONE in-flight mut/call. A CONCURRENT reuse would
    // overwrite the first waiter, then either waiter's timeout timer (deleting by
    // key) could evict the other's entry — dropping a receipt and corrupting the
    // parked-replay identity across a reconnect (issue #39 / ADR-0021). Reject the
    // duplicate loud rather than corrupt state. A SEQUENTIAL retry after the first
    // settled is fine: its entry is already gone, so this guard does not fire —
    // exactly the retry-through-dedup the server is built to answer.
    if (this.pendingTx.has(txId)) {
      throw new MutationRejectedError(`txId already in flight: ${txId}`, "DUPLICATE_TXID")
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingTx.delete(txId)
        reject(new Error(`confirmation timeout: txId=${txId}`))
      }, this.timeoutMs)
      this.pendingTx.set(txId, { resolve, reject, timer, frame: encoded, parked: false })
      // A socket may refuse a send synchronously (workerd does for frames over
      // its own cap). Clean up the waiter/timer before rejecting, or the stale
      // entry lingers with an armed timeout for timeoutMs (codex review).
      try {
        this.sendRaw(encoded)
      } catch (e) {
        clearTimeout(timer)
        this.pendingTx.delete(txId)
        reject(e instanceof Error ? e : new Error(String(e)))
      }
    })
  }

  private sendFrame(frame: ClientFrame): void {
    this.sendRaw(this.codec.encode(frame))
  }

  private sendRaw(data: WireOut): void {
    if (!this.ws) throw new Error("transport not connected")
    this.ws.send(data)
  }

  private onMessage(data: unknown, stale = false): void {
    let frame: ServerFrame
    try {
      frame = this.codec.decode(data as ArrayBuffer | string) as ServerFrame
    } catch {
      return
    }
    switch (frame.t) {
      case "snap":
        if (stale) return
        this.handlers.get(frame.sub)?.handler.onSnap(frame.key, frame.row)
        return
      case "snap-end": {
        if (stale) return
        const entry = this.handlers.get(frame.sub)
        if (entry) entry.bootstrapped = true
        entry?.handler.onSnapEnd()
        this.advance(frame.seq)
        return
      }
      case "d":
        if (stale) return
        this.handlers.get(frame.sub)?.handler.onDelta(frame.op, frame.key, frame.cols)
        return
      case "uptodate":
        if (stale) return
        // A sub-scoped terminal (a catch-up's) goes to its handler alone; a
        // broadcast boundary (coalescer tick / barrier flush) goes to all.
        if (frame.sub) {
          const entry = this.handlers.get(frame.sub)
          if (entry) entry.bootstrapped = true
          entry?.handler.onUptodate(true)
        } else for (const { handler } of this.handlers.values()) handler.onUptodate(false)
        this.advance(frame.seq)
        return
      case "committed": {
        const w = this.pendingTx.get(frame.txId)
        if (w) {
          clearTimeout(w.timer)
          this.pendingTx.delete(frame.txId)
          w.resolve({ result: frame.result })
        }
        if (!stale) this.advance(frame.seq)
        return
      }
      case "rejected": {
        const w = this.pendingTx.get(frame.txId)
        if (w) {
          clearTimeout(w.timer)
          this.pendingTx.delete(frame.txId)
          w.reject(new MutationRejectedError(frame.error.message, frame.error.code))
        }
        return
      }
      case "page": {
        const w = this.pendingFetches.get(frame.fetchId)
        if (w) {
          clearTimeout(w.timer)
          this.pendingFetches.delete(frame.fetchId)
          if (stale) w.reject(new Error(`fetch page from an abandoned socket: ${frame.fetchId}`))
          else w.resolve(frame.rows)
        }
        return
      }
      case "reset":
        if (stale) return
        if (frame.sub) this.handlers.get(frame.sub)?.handler.onReset()
        else for (const { handler } of this.handlers.values()) handler.onReset()
        return
    }
  }

  private advance(seq: string): void {
    const s = BigInt(seq)
    if (s > this.appliedSeq) this.appliedSeq = s
    if (this.seqWaiters.length === 0) return
    const remaining: Array<SeqWaiter> = []
    for (const w of this.seqWaiters) {
      if (this.appliedSeq >= w.target) {
        clearTimeout(w.timer)
        w.resolve()
      } else {
        remaining.push(w)
      }
    }
    this.seqWaiters.length = 0
    this.seqWaiters.push(...remaining)
  }
}
