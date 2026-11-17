// SsrSnapshotTransport — the server-rendering half of ADR-0011 D2.
//
// Implements the same structural `Transport` the WebSocket transport does, so
// `doCollectionOptions` runs unchanged inside a per-request DbClient on the
// worker: each subscribe is ONE snapshot read (rows + durable cursor), synthesized
// as onSnap*/onSnapEnd. No socket, no timers, no live deltas — render, dehydrate,
// throw away.
//
// The reader is injected as a plain function so this file carries no Cloudflare
// types; the author passes `(req) => stub.readSyncSnapshot(req, request)` (the
// SyncDurableObject RPC), closing over the same claims-bearing Request the WS
// upgrade gets — parseAttachment is the ONE auth gate for both paths.
//
// SSR is read-only: mutations during render are a design error, not a queue —
// they throw. Create one transport (and one options object) PER REQUEST; a
// module-scope instance would leak cursor state across requests (the upstream
// hooks offer no per-instance identity — see ADR-0011 "Context").

import { decode, encode } from "../wire/codec.ts"
import type { ClientFrame } from "../wire/frames.ts"
import type { SubHandler, Transport } from "./transport.ts"

/** TanStack's expression IR arrives as class instances (Func/Ref/Value), which
 *  structured clone — and therefore DO RPC — rejects. The wire tagged-value
 *  codec already flattens them to plain data preserving bigint/Date/±Inf, so a
 *  round-trip gives the reader a clone-safe request (same shape the WS frames
 *  carry). */
function plain(v: unknown): unknown {
  return v === undefined ? undefined : decode(encode(v))
}

export class SsrReadOnlyError extends Error {
  constructor(operation: string) {
    super(
      `${operation} during SSR: the snapshot transport is read-only. ` +
        `Mutations belong on the live client after hydration.`,
    )
    this.name = "SsrReadOnlyError"
  }
}

/** One snapshot read — rows plus the durable high-water cursor at one position. */
export type SnapshotRead = (req: {
  collection: string
  where?: unknown
  orderBy?: unknown
  limit?: number
}) => Promise<{ rows: Array<Record<string, unknown>>; cursor: string }>

export class SsrSnapshotTransport<Api = unknown> implements Transport<Api> {
  /** phantom — carries `Api` so `doCollectionOptions` infers the collection set
   *  from the SSR transport exactly as it does from `WebSocketTransport<Api>`. */
  declare readonly __api?: Api
  /** MIN across reads — multiple subsets read at different positions can only
   *  safely resume from the EARLIEST one (replay is idempotent; skipping is
   *  not). Null until the first read. */
  private cursor: bigint | null = null

  constructor(private readonly opts: { read: SnapshotRead }) {}

  /** Lowest position across this render's reads (stringified bigint). */
  get appliedCursor(): string {
    return String(this.cursor ?? 0n)
  }

  /** True after ANY read — a read that returned cursor 0 (a DO with no
   *  history) is a REAL "no resume point" claim and must dehydrate as such;
   *  only a transport that never read holds no claim. */
  get hasPosition(): boolean {
    return this.cursor !== null
  }

  async connect(): Promise<void> {
    // Nothing to open — but resolving lets on-demand mode markReady as usual.
  }

  async subscribe(
    _subId: string,
    collection: string,
    handler: SubHandler,
    where?: unknown,
    orderBy?: unknown,
    limit?: number,
    _since?: string,
  ): Promise<void> {
    const rows = await this.read(collection, where, orderBy, limit)
    // Key is derived by the adapter via getKey(row); snap keys are advisory.
    for (const row of rows) handler.onSnap(undefined, row)
    handler.onSnapEnd()
  }

  /** One snapshot read; its position joins the render's MIN cursor. */
  private async read(collection: string, where: unknown, orderBy: unknown, limit?: number) {
    const { rows, cursor } = await this.opts.read({ collection, where: plain(where), orderBy: plain(orderBy), limit })
    const c = BigInt(cursor)
    this.cursor = this.cursor === null || c < this.cursor ? c : this.cursor
    return rows
  }

  unsubscribe(): void {
    // One-shot reads hold nothing to release.
  }

  sendMut(_frame: Extract<ClientFrame, { t: "mut" }>): Promise<{ result?: unknown }> {
    return Promise.reject(new SsrReadOnlyError("mutation"))
  }

  /** A render refuses writes, never reads (ADR-0011 amendment 2026-11-17).
   *  A fetch without a cursor (a covered on-demand request, ADR-0023 D2) is the
   *  same where/orderBy/limit read a subscribe makes. A fetch with a cursor is
   *  the server's two reads (ADR-0003): the boundary ties, unbounded, then the
   *  next page, bounded. Here they are two reads at two positions; a render has
   *  no delta stream to slot them into, and both join the MIN cursor, so the
   *  hydration catch-up replays whatever changed between them. A row in both
   *  is written once (installPage). */
  async fetch(frame: Extract<ClientFrame, { t: "fetch" }>): Promise<Array<unknown>> {
    const { collection, where, cursor, orderBy, limit } = frame
    if (cursor == null) return this.read(collection, where, orderBy, limit)
    const and = (a: unknown, b: unknown): unknown => (a == null ? b : { type: "func", name: "and", args: [plain(a), plain(b)] })
    const ties = await this.read(collection, and(where, cursor.whereCurrent), orderBy)
    const page = await this.read(collection, and(where, cursor.whereFrom), orderBy, limit)
    return [...ties, ...page]
  }

  seedCursor(): void {
    // Hydrating INTO a render is meaningless — reads define this cursor.
  }

  close(): void {
    // Nothing held.
  }
}
