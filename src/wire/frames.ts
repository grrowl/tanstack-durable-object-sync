// Wire protocol frames (ADR-0001 §single-ordered-stream, ADR-0002 §2).
// Shared by the Durable Object server and the browser client.
//
// One ordered stream per DO. The client tracks a single cursor (appliedSeq);
// `seq` is opaque (a stringified bigint). Confirmation rides on the same stream
// as data — `committed`/`rejected` correlate to a client `txId`; there is no
// second ack channel (ADR-0002 C1).
//
// Predicate/order fields are typed `unknown` here and tightened to the
// @tanstack/db expression IR in M5.

export type Cursor = string

/** A position in its plain decimal form, the only form the server accepts as a
 *  `sub`'s `since`: no sign, no leading zero, at most 19 digits, the length of
 *  SQLite's largest INTEGER (2^63 - 1) (ADR-0012 amendment 2026-11-17). */
export const POSITION = /^(0|[1-9][0-9]{0,18})$/
export type TxId = string
export type RowOp = "insert" | "update" | "delete"

/** Placeholder for the @tanstack/db BasicExpression IR (tightened in M5). */
export type WireExpression = unknown
export type WireOrderBy = unknown

/** One row operation inside a `mut` frame. `cols` is a getChanges() diff; for
 *  an insert it is the full row (ADR-0001 D19). */
export interface MutOp {
  type: RowOp
  key: unknown
  cols?: Record<string, unknown>
}

export type ClientFrame =
  | {
      t: "sub"
      subId: string
      collection: string
      where?: WireExpression
      orderBy?: WireOrderBy
      limit?: number
      offset?: number
      cursor?: WireExpression
      since?: Cursor
    }
  | { t: "unsub"; subId: string }
  | { t: "mut"; txId: TxId; collection: string; ops: Array<MutOp> }
  | { t: "call"; txId: TxId; name: string; args: unknown }
  // One-shot paginated page fetch — a subset snapshot with NO live
  // subscription. Used for cursor load-more (scroll-back); deltas for the
  // window already flow via the live `sub` on the query's `where`.
  //
  // The frame is a serialized subset of @tanstack/db's `LoadSubsetOptions`
  // (ADR-0005): `where` is the base filter and `cursor` is its `CursorExpressions`,
  // carried RAW (whereFrom/whereCurrent exclude the base `where`, exactly as
  // TanStack defines them). The server composes `base AND whereCurrent` (ties, unbounded)
  // and `base AND whereFrom` (next page, bounded by `limit`). The cursor
  // double-read is ONE frame so the server reads both halves at a single `seq`
  // (atomic) and the client applies the page in stream order before any later
  // delta — see ADR-0003. Ties must be unbounded while next is limited, an
  // asymmetry a single (where, limit) can't express, hence two predicates.
  | {
      t: "fetch"
      fetchId: string
      collection: string
      where?: WireExpression
      cursor?: { whereFrom: WireExpression; whereCurrent: WireExpression }
      orderBy?: WireOrderBy
      limit?: number
    }

export type ServerFrame =
  // Snapshot rows (full row) then a boundary; client truncates + applies.
  | { t: "snap"; sub: string; key: unknown; row: unknown; seq: Cursor }
  | { t: "snap-end"; sub: string; seq: Cursor }
  // Live delta; `cols` is a partial (top-level) patch, absent for delete.
  | { t: "d"; sub: string; key: unknown; op: RowOp; cols?: Record<string, unknown>; seq: Cursor }
  // Batch boundary — client commits the buffered sync transaction here.
  // `sub` scopes a CATCH-UP's terminal to its subscription (ADR-0011 D3): a
  // transient hydration catch-up must distinguish its own terminal from a
  // coalescer/barrier boundary, or it unsubscribes early and drops its own
  // deltas. Absent on broadcast boundaries (additive, backwards-compatible).
  | { t: "uptodate"; seq: Cursor; sub?: string }
  // Mutation receipt (the no-subscription-match path lives here; ADR-0002 C1/C2).
  | { t: "committed"; txId: TxId; seq: Cursor; result?: unknown }
  | { t: "rejected"; txId: TxId; error: { code?: string; message: string } }
  // Compaction/rotation reset — client truncates and resnapshots (ADR-0002 C5).
  | { t: "reset"; sub?: string }
  // Response to a `fetch`: the page's rows, in one frame (no live sub).
  | { t: "page"; fetchId: string; rows: Array<unknown>; seq: Cursor }

export type Frame = ClientFrame | ServerFrame
export type FrameTag = Frame["t"]
