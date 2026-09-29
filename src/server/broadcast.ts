// Egress coalescer (ADR-0001 D2). Per-(ws, sub, key) last-write-wins buffer,
// flushed on a tick (default 50ms) as one `d` per surviving key plus a single
// `uptodate` boundary. Collapses high-rate writes to the same row (e.g. a
// streaming token field) to one delta per tick.
//
// Hibernation-native: the flush timer is armed on demand by enqueue() and
// cleared by flushAll(). A quiet broadcaster holds no timer, so the DO is free
// to hibernate (held timers would prevent it).
//
// The C1 ordering invariant (ADR-0002): the originating socket of a mutation is
// flushed via flushOne() BEFORE its `committed` receipt, so its deltas precede
// the confirmation on the wire. Other subscribers flush on the tick.

import type { RowOp, ServerFrame } from "../wire/frames.ts"

export interface PendingDelta {
  subId: string
  key: unknown
  op: RowOp
  cols?: Record<string, unknown>
}

export type RawSend = (ws: WebSocket, frame: ServerFrame) => void

export class Broadcaster {
  private readonly pending = new WeakMap<WebSocket, Map<string, PendingDelta>>()
  private latestCursor = "0"
  private flushTimer: ReturnType<typeof setTimeout> | null = null
  private getAllWs: (() => Iterable<WebSocket>) | null = null

  constructor(
    private readonly rawSend: RawSend,
    private readonly tickMs: number | (() => number) = 50,
  ) {}

  /** True while a tick flush is pending — exposed for hibernation assertions. */
  get isFlushScheduled(): boolean {
    return this.flushTimer !== null
  }

  /** Engage on-demand tick flushing. Does not create a timer until there is
   *  work to flush, so an idle broadcaster never blocks hibernation. */
  start(getAllWs: () => Iterable<WebSocket>): void {
    this.getAllWs = getAllWs
  }

  stop(): void {
    this.getAllWs = null
    this.clearTimer()
  }

  enqueue(ws: WebSocket, delta: PendingDelta, cursor: string): void {
    this.latestCursor = cursor
    let m = this.pending.get(ws)
    if (!m) {
      m = new Map()
      this.pending.set(ws, m)
    }
    m.set(`${delta.subId}::${String(delta.key ?? "")}`, delta)
    this.armFlush()
  }

  /** Flush one socket's buffered deltas + a boundary, immediately. */
  flushOne(ws: WebSocket): void {
    const m = this.pending.get(ws)
    if (!m || m.size === 0) return
    for (const d of m.values()) {
      this.rawSend(
        ws,
        d.op === "delete"
          ? { t: "d", sub: d.subId, key: d.key, op: "delete", seq: this.latestCursor }
          : { t: "d", sub: d.subId, key: d.key, op: d.op, cols: d.cols, seq: this.latestCursor },
      )
    }
    m.clear()
    this.rawSend(ws, { t: "uptodate", seq: this.latestCursor })
  }

  flushAll(wss: Iterable<WebSocket>): void {
    for (const ws of wss) this.flushOne(ws)
    this.clearTimer()
  }

  private armFlush(): void {
    if (!this.getAllWs || this.flushTimer) return
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null
      const get = this.getAllWs
      if (get) this.flushAll(get())
    }, typeof this.tickMs === "function" ? this.tickMs() : this.tickMs)
  }

  private clearTimer(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = null
    }
  }
}
