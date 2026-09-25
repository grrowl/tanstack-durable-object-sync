// doCollectionOptions — a TanStack DB collection-options creator backed by a
// Durable Object over a WebSocketTransport.
//
// Maps the transport's frame callbacks onto TanStack DB's sync API
// (begin/write/commit/markReady/truncate) and wraps mutations as `mut` frames
// confirmed on the single ordered stream.
//
// Two sync modes:
//   - 'eager' (default): subscribe to the whole collection (optionally filtered
//     by a static `where`) up front. A `where` also preflights writes.
//   - 'on-demand': sync nothing up front; the collection calls loadSubset(where)
//     as live queries mount, and unloadSubset when they unmount. Each distinct
//     `where` is one refcounted server subscription; ordering/limit are applied
//     client-side by IVM over the loaded rows. Writes that land outside every
//     loaded subset are confirmed and their optimistic overlay retired by a
//     post-mutation empty sync commit (ADR-0002 C2, verified).

import {
  compileSingleRowExpression,
  toBooleanPredicate,
  withCollectionConfigFactory,
  type CollectionConfig,
} from "@tanstack/db"
import { encode as codecEncode } from "../wire/codec.ts"
import type { MutOp, RowOp } from "../wire/frames.ts"
import type { SubHandler, Transport } from "./transport.ts"

let subSeq = 0

export class WriteOutsideSubError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WriteOutsideSubError"
  }
}

// --- Row inference from a schema Api (`typeof schema`) ----------------------
// Mirrors the transport's command projection: structural-only, recovering a
// collection's Row from the phantom `__row` the server's `CollectionEntry`
// carries. The client needs NO runtime schema value — just the Api type.
type CollectionsOf<Api> = Api extends { collections: infer C } ? C : never
/** Table names declared on the schema Api. */
export type CollectionName<Api> = keyof CollectionsOf<Api> & string
/** The Row type of collection `K` on the schema Api. */
export type RowOf<Api, K extends PropertyKey> = K extends keyof CollectionsOf<Api>
  ? CollectionsOf<Api>[K] extends { __row?: infer R }
    ? R
    : never
  : never

interface PendingMutationLike {
  type: RowOp
  key: string
  modified: unknown
  changes: unknown
}

/** `true` = applied synchronously; a Promise settles when the rows are visible
 *  (@tanstack/db 0.8.5's SyncAppliedReceipt — loadSubset must await it). */
type CommitReceipt = true | Promise<void>

interface SyncParams {
  collection: { get: (key: string) => unknown }
  begin: (options?: { immediate?: boolean }) => void
  write: (message: { type: RowOp; value?: unknown; key?: string }) => void
  commit: (signal?: AbortSignal) => CommitReceipt
  markReady: () => void
  /** Fails the collection's readiness promises with the cause (@tanstack/db
   *  0.8.2); recovery is a retried preload() once sync later succeeds.
   *  Optional only for bare test harnesses — real @tanstack/db provides it. */
  markError?: (error?: unknown) => void
  truncate: () => void
}

/** The opaque payload that rides TanStack's dehydrated state (ADR-0011 D3).
 *  Shape is ours; `v` gates forward evolution loudly. `where` fingerprints the
 *  eager filter the rows were dehydrated under: a cursor is only a sound
 *  resume point FOR THAT FILTER (catch-up emits changed keys only — an
 *  unchanged out-of-filter hydrated row would never be reconciled away). */
export interface DoSyncMeta {
  v: 1
  cursor: string
  where?: string
}

function parseSyncMeta(meta: unknown): DoSyncMeta {
  const m = meta as Partial<DoSyncMeta> | null
  if (m == null || m.v !== 1 || typeof m.cursor !== "string" || (m.where !== undefined && typeof m.where !== "string")) {
    throw new Error(`unrecognized sync meta (expected {v:1, cursor, where?}): ${JSON.stringify(meta)}`)
  }
  // Malformed throws; NEGATIVE is rejected too (codex review): seedCursor
  // ignores it but `since:"-1"` would reach the server, which answers a
  // full snapshot the on-demand catch-up handler discards — no terminal, the
  // transient sub never tears down, and stale hydrated rows survive forever.
  if (BigInt(m.cursor) < 0n) throw new Error(`negative sync-meta cursor: ${m.cursor}`)
  return { v: 1, cursor: m.cursor, ...(m.where === undefined ? {} : { where: m.where }) }
}

/** Subset of @tanstack/db's LoadSubsetOptions we consume. */
interface LoadSubsetOptions {
  where?: unknown
  orderBy?: unknown
  limit?: number
  offset?: number
  cursor?: { whereFrom: unknown; whereCurrent: unknown; lastKey?: unknown }
}

function compilePredicate(where: unknown): (row: Record<string, unknown>) => boolean {
  if (where === undefined || where === null) return () => true
  const evaluate = compileSingleRowExpression(where as never) as (
    row: Record<string, unknown>,
  ) => boolean | null
  return (row) => toBooleanPredicate(evaluate(row))
}

/** Api-typed options: Row is inferred from the schema `Api` + `table`, so the
 *  client needs no runtime schema value. `getKey` and the row type follow. */
export interface DoApiCollectionOptions<Api, K extends CollectionName<Api>> {
  /** One transport per DO, parameterized by the same schema `Api`. In the
   *  browser a `WebSocketTransport<Api>`; during SSR an
   *  `SsrSnapshotTransport<Api>` — created PER REQUEST (ADR-0011 D2). Both
   *  satisfy the structural `Transport<Api>`. */
  transport: Transport<Api>
  /** Collection (table) name on the DO — a key of the schema's collections. */
  table: K
  /** Stable client-supplied key extractor (must match the server pk). */
  getKey: (row: RowOf<Api, K>) => string
  /** Collection id; defaults to the table name. */
  id?: string
  syncMode?: "eager" | "on-demand"
  where?: unknown
}

// The schema `Api` is the single source of truth: `Api` is inferred from the
// (branded) transport and the table key from the `table` literal, so the row
// type follows and a table that isn't a collection of `Api` is a type error.
//
//   const messages = createCollection(
//     doCollectionOptions({ transport, table: "messages", getKey: (m) => m.id }),
//   )
//
// Explicit type args are optional (`doCollectionOptions<Api, "messages">(...)`).
//
// The returned config carries @tanstack/db's collection-config factory
// (`withCollectionConfigFactory`), so `collectionOptions("id", () =>
// doCollectionOptions({...}))` descriptors materialize with FRESH adapter
// state per DbClient. The transport is the caller's: per-request SSR clients
// must construct a per-request transport inside their own descriptor factory.
export function doCollectionOptions<Api, K extends CollectionName<Api>>(
  opts: DoApiCollectionOptions<Api, K>,
): CollectionConfig<RowOf<Api, K> & object, string>
export function doCollectionOptions(opts: {
  transport: Transport<any>
  table: string
  getKey: (row: any) => string
  id?: string
  syncMode?: "eager" | "on-demand"
  where?: unknown
}): CollectionConfig<any, string> {
  const { transport, table, getKey, where } = opts
  const syncMode = opts.syncMode ?? "eager"
  const eagerSubId = `${table}#${++subSeq}`
  const matches = compilePredicate(where)

  // Set by sync(); used by mutationFn to retire no-subset-match optimistic rows.
  let emptyCommit: (() => void) | null = null

  // SSR hydration's resume point (ADR-0011 D3). Set by importSyncMeta — which
  // upstream calls AFTER applying the dehydrated rows as synced upserts, and
  // possibly BEFORE sync() ever runs (lazy collections). Consumed exactly once
  // at sync start and cleared in cleanup: after a collection GC the rows are
  // wiped, so a retained cursor would resume over an empty store and silently
  // lose everything below it.
  let hydratedCursor: string | null = null

  // Settlement gate for the syncMeta claim (codex review): the transport's
  // cursor advances at commit BOUNDARIES, but a commit's SyncAppliedReceipt
  // may settle later (application queued behind a persisting user
  // transaction). Exporting the boundary cursor in that window would dehydrate
  // pre-boundary rows under meta claiming the boundary — a resume that skips
  // the gap forever. While any receipt is unsettled, exportSyncMeta claims the
  // last fully-settled position instead (under-claiming is always safe: MIN
  // semantics, idempotent replay).
  let pendingReceipts = 0
  let settledCursor = "0"

  const sync = (params: SyncParams): SyncConfigResult => {
    const { collection, begin, write, commit, markReady, markError, truncate } = params
    const consumeHydratedCursor = (): string | null => {
      const hc = hydratedCursor
      hydratedCursor = null
      return hc
    }
    // Presence in SYNCED data — the combined view (collection.get) includes
    // optimistic overlays, which sync writes must never be steered by: a key
    // under an optimistic delete still exists synced (insert would throw), and
    // an optimistic-only insert does not (update would not upsert the synced
    // store the hydration correction targets). `_state.syncedData` is the same
    // seam upstream's DbClient hydration itself writes through.
    const syncedData = (): Map<string, unknown> | null =>
      (collection as { _state?: { syncedData?: Map<string, unknown> } })._state?.syncedData ?? null
    const syncedHas = (key: string): boolean => {
      const sd = syncedData()
      return sd ? sd.has(key) : collection.get(key) !== undefined
    }
    let open = false
    const ensureBegin = (): void => {
      if (!open) {
        begin()
        open = true
      }
    }
    /** Book a commit's receipt against the settlement gate (see
     *  pendingReceipts). Returns the ORIGINAL receipt so callers still await
     *  and propagate rejection; the passive branch also keeps a fire-and-
     *  forget flush from surfacing as an unhandled rejection. */
    const track = (receipt: CommitReceipt): CommitReceipt => {
      const r = receipt as { then?: (a: () => void, b: () => void) => unknown }
      if (r != null && typeof r.then === "function") {
        pendingReceipts++
        const settle = (): void => {
          pendingReceipts--
          // Everything booked has applied: the transport's position is a
          // sound claim again from here.
          if (pendingReceipts === 0) settledCursor = transport.appliedCursor
        }
        void r.then(settle, settle)
      } else if (pendingReceipts === 0) {
        settledCursor = transport.appliedCursor
      }
      return receipt
    }
    /** Commit the open transaction; the receipt settles when rows are visible
     *  (`true` = already are). The 0.8.5 loadSubset contract chains on it. */
    const flush = (): CommitReceipt => {
      if (!open) return true
      open = false
      return track(commit())
    }
    /** Run `onApplied` once `receipt` says the rows are visible; a REJECTED
     *  receipt (the application was aborted, 0.8.5) is a failure, not
     *  success — it must not resolve a subset load or readiness (codex
     *  review). Thenable-sniffed — anything non-promise (incl. `true`, and
     *  bare harness mocks returning void) means "already applied". */
    const afterApplied = (receipt: CommitReceipt, onApplied: () => void, onFail: (e: unknown) => void): void => {
      const r = receipt as { then?: (a: () => void, b: (e: unknown) => void) => unknown }
      if (r != null && typeof r.then === "function") void r.then(onApplied, onFail)
      else onApplied()
    }
    emptyCommit = (): void => {
      flush()
      begin()
      track(commit()) // a standalone empty boundary; runs the direct-upsert clear path
    }

    const makeHandler = (
      onReady: () => void,
      opts?: { reconcileSnapshots?: boolean; onFail?: (e: unknown) => void },
    ): SubHandler => {
      // Where a rejected receipt lands: a subset load rejects ITS promise; the
      // collection-level default fails readiness loud (a later markReady —
      // any successful snapshot — recovers, error → ready).
      const onFail = opts?.onFail ?? ((e: unknown): void => markError?.(e))
      // `reconcileSnapshots` (armed for every EAGER sub, never for on-demand
      // subset subs — a subset snapshot must not delete other subsets' rows):
      // a snapshot is authoritative SET semantics over the synced rows —
      // held keys absent from it were deleted server-side, and snapshots
      // carry no tombstones (ADR-0011 D4). Track each snapshot's keys and
      // delete the rest at ITS boundary; no truncate, so a hydrated first
      // paint never flashes empty. The set is per-snapshot (reset at every
      // snap-end), and an EMPTY snapshot (zero snap frames — the server
      // wiped the table) still reconciles everything away at the boundary.
      let snapKeys: Set<string> | null = null
      return {
        onSnap: (_key, row) => {
          ensureBegin()
          const key = getKey(row as Record<string, unknown>)
          if (opts?.reconcileSnapshots) (snapKeys ??= new Set()).add(key)
          // A held key's snapshot row is an upsert: hydrated rows may have
          // changed since dehydration, and a differing insert would throw
          // DuplicateKeySyncError. With the C1′ barrier a snapshot row is
          // never staler than the held synced row, so the snapshot wins.
          write(syncedHas(key) ? { type: "update", value: row } : { type: "insert", value: row })
        },
        onSnapEnd: () => {
          if (opts?.reconcileSnapshots) {
            const seen = snapKeys // null ⇒ empty snapshot ⇒ empty authoritative set
            snapKeys = null
            const sd = syncedData()
            if (!sd) throw new Error("snapshot reconcile requires collection._state.syncedData (incompatible @tanstack/db)")
            for (const key of sd.keys()) {
              // ensureBegin only when a delete is actually due — the common
              // converged/empty case stays boundary-free.
              if (!seen?.has(key)) {
                ensureBegin()
                write({ type: "delete", key })
              }
            }
          }
          afterApplied(flush(), onReady, onFail)
        },
        onDelta: (op, key, cols) => {
          ensureBegin()
          if (op === "delete") write({ type: "delete", key: key as string })
          // A catch-up emits the LATEST op per changed key, so a key deleted-
          // and-reinserted while we were away arrives as "insert" for a key we
          // still HOLD — TanStack's sync write throws DuplicateKeySyncError on
          // that unless values deep-equal. Apply a held-key insert as the
          // upsert it semantically is (update upserts; move-in, ADR-0002 C4).
          else if (op === "insert" && syncedHas(key as string)) write({ type: "update", value: cols })
          else write({ type: op, value: cols })
        },
        // The transport restarts a sub that never bootstrapped from a fresh
        // snapshot (a drop before its snap-end). Undo what the partial snapshot
        // wrote, in the still-open transaction, so a row deleted meanwhile is not
        // carried over; rows that still exist are re-upserted by the
        // replacement before the commit (no flash).
        onRestart: () => {
          for (const key of snapKeys ?? []) {
            ensureBegin()
            write({ type: "delete", key })
          }
          snapKeys = null
        },
        onUptodate: () => flush(),
        onReset: () => {
          flush()
          begin()
          truncate()
          // A reset is also the only terminal signal for a REJECTED sub (the
          // server sends `reset` with no `snap-end` for an unsupported predicate
          // or unknown collection). Mark ready here too, or this subset's load
          // promise — and the live query's preload() — would hang forever. For a
          // compaction/rotation reset (a valid sub that re-snapshots) this is an
          // idempotent no-op: onSnapEnd's onReady() has already fired.
          afterApplied(track(commit()), onReady, onFail)
        },
      }
    }

    if (syncMode === "on-demand") {
      // Hydration catch-up (ADR-0011 D3): the dehydrated rows are the union of
      // whatever subsets the server render loaded — per-subset resume is
      // unsound (a subset the render didn't cover has no since to resume
      // from, and overlapping predicates leave stale-delete holes). ONE
      // transient unfiltered sub from the dehydrated cursor covers every
      // changed key (always-emit ⇒ synthetic deletes included) in the
      // render→hydrate window, then unsubscribes at ITS terminal — never at a
      // broadcast boundary, which can precede its own frames. Semantic cost
      // (documented): rows outside any loaded subset that changed in the
      // window land in the collection.
      //
      // With NO resume point ("0"), or when the server resets the catch-up
      // (below the retention floor), the hydrated rows are honestly
      // UNRESUMABLE: truncate. In on-demand a full snapshot would strand
      // never-subscribed whole-table rows as permanently-stale state — worse
      // than a one-roundtrip refetch of the live subsets. The reset path
      // unsubscribes IMMEDIATELY so the server's trailing unfiltered
      // resnapshot is dropped on the floor (no handler), and the subset subs
      // repopulate right after.
      //
      // markReady gates on the catch-up sub FRAME being sent (not completed):
      // loadSubset subs only fire after ready, so on the single ordered
      // socket the catch-up's truncate/deltas always precede subset
      // snapshots. Ready never waits for data — stale-while-revalidate.
      const hc = consumeHydratedCursor()
      let readyGate: Promise<void>
      if (hc !== null && hc !== "0") {
        const catchupId = `${table}#hydrate#${++subSeq}`
        const done = (): void => transport.unsubscribe(catchupId)
        readyGate = transport.subscribe(
          catchupId,
          table,
          {
            onSnap: () => {}, // catch-ups never snapshot; reset's resnapshot is dropped (unsubbed)
            onSnapEnd: () => {},
            onDelta: makeHandler(() => {}).onDelta,
            onUptodate: (ownTerminal) => {
              flush()
              if (ownTerminal) {
                done()
                // Also heals an earlier failed gate (error → ready, 0.8.2):
                // the readyGate rejected, the policy-driven reconnect
                // resubscribed this catch-up, and its terminal is the first
                // proof the collection is usable again (codex review —
                // idempotent when the gate already marked ready).
                markReady()
              }
            },
            onReset: () => {
              flush()
              begin()
              truncate()
              track(commit())
              done() // before the trailing resnapshot frames arrive
              markReady() // same healing as the terminal path
            },
          },
          undefined,
          undefined,
          undefined,
          hc,
        )
      } else if (hc === "0") {
        // No resume point: drop the hydrated rows at sync start, honestly.
        readyGate = transport.connect().then(() => {
          begin()
          truncate()
          track(commit())
        })
      } else {
        readyGate = transport.connect()
      }
      // A failed gate fails readiness loud (markError; preload() rejects with
      // the cause) instead of hanging — a later retried preload() recovers
      // once the transport's policy-driven reconnect succeeds (0.8.2).
      void readyGate.then(markReady, (e) => markError?.(e))

      // Distinct `where` -> one refcounted server subscription.
      const loaded = new Map<string, { subId: string; refs: number; ready: Promise<void> }>()
      const keyOf = (o: LoadSubsetOptions): string => JSON.stringify(o.where ?? null)

      // Cursor load-more (scroll-back). The live sub on `where` already streams
      // deltas for the whole subset, so this is a one-shot fetch of the older
      // rows the window now needs — NOT a new live registration.
      //
      // The fetch frame is a serialized `LoadSubsetOptions` (ADR-0005): we forward TanStack's
      // own `where` and `cursor` (whereFrom/whereCurrent) verbatim. The server
      // composes `base AND whereCurrent` (ties, unbounded) and `base AND whereFrom`
      // (next page, bounded by `limit`). It's ONE frame, so the server reads both
      // halves at one seq (atomic) and the client applies the whole page in one
      // macrotask — the write lands before any later delta. That ordering is what
      // prevents a concurrent delete from being undone by a stale tie (ADR-0003).
      //
      // Rows are written insert-if-ABSENT: a boundary tie already in the window
      // must not be re-inserted (a differing value would throw
      // DuplicateKeySyncError and abort the transaction), and a key the live sub
      // already holds keeps its fresher value. The live `where` sub stays the
      // source of truth for anything currently in the collection.
      const loadMore = async (o: LoadSubsetOptions): Promise<void> => {
        const { whereFrom, whereCurrent } = o.cursor!
        const page = await transport.fetch({
          t: "fetch",
          fetchId: `${table}#fetch#${++subSeq}`,
          collection: table,
          where: o.where,
          cursor: { whereFrom, whereCurrent },
          orderBy: o.orderBy,
          limit: o.limit,
        })
        ensureBegin()
        for (const r of page) {
          if (collection.get(getKey(r)) === undefined) write({ type: "insert", value: r })
        }
        // 0.8.5 contract: a subset load settles only once its rows are visible.
        const receipt = flush()
        if (receipt !== true) await receipt
      }

      const loadSubset = (o: LoadSubsetOptions): true | Promise<void> => {
        if (o.cursor) return loadMore(o)
        const key = keyOf(o)
        const existing = loaded.get(key)
        if (existing) {
          existing.refs++
          return existing.ready
        }
        let resolve!: () => void
        let reject!: (e: unknown) => void
        const ready = new Promise<void>((res, rej) => {
          resolve = res
          reject = rej
        })
        const subId = `${table}#${key}`
        loaded.set(key, { subId, refs: 1, ready })
        // Forward orderBy/limit so the INITIAL snapshot is the bounded window
        // (recent N), not the whole where-subset. The live sub's predicate is
        // still `where`, so entering rows (e.g. new messages) are delivered.
        // A send failure or rejected receipt rejects THIS load (0.8.4 surfaces
        // it per-subscription as loadSubset:error), not the whole collection.
        // A completed subset also (re)marks ready — the recovery path out of a
        // failed ready-gate's error state (idempotent otherwise).
        const handler = makeHandler(
          () => {
            resolve()
            markReady()
          },
          { onFail: reject },
        )
        void transport.subscribe(subId, table, handler, o.where, o.orderBy, o.limit).catch(reject)
        return ready
      }

      const unloadSubset = (o: LoadSubsetOptions): void => {
        // Symmetric with loadSubset: a cursor load was a one-shot fetch that
        // never took a refcount, so its unload must not release one either —
        // otherwise a second live query on the same `where` is under-counted
        // and its still-live sub is torn down early.
        if (o.cursor) return
        const key = keyOf(o)
        const entry = loaded.get(key)
        if (entry && --entry.refs <= 0) {
          transport.unsubscribe(entry.subId)
          loaded.delete(key)
        }
      }

      return {
        loadSubset,
        unloadSubset,
        cleanup: () => {
          hydratedCursor = null // GC wiped the rows; a retained cursor would lie
          transport.close()
        },
      }
    }

    // eager — reconcile is ALWAYS armed: an eager snapshot is authoritative
    // set semantics over synced rows, period (ADR-0011 D4). For the normal
    // empty-at-first-snapshot flow it is a no-op; for ANY path where synced
    // rows precede a snapshot — hydration with no resume point, hydration
    // whose meta failed validation (rows land before importSyncMeta; no
    // veto), futures we haven't imagined — it is what prevents a
    // server-deleted held row from being stale forever. C1′ makes it sound
    // mid-session too: a held synced key absent from a snapshot is deleted.
    {
      const hc = consumeHydratedCursor()
      const handler = makeHandler(markReady, { reconcileSnapshots: true })
      if (hc !== null) {
        // Hydrated (ADR-0011 D3): the rows were applied upstream as synced
        // upserts before we ran. Resume from the dehydrated cursor (server
        // catch-up; below the floor an honest reset + resnapshot) — or, with
        // no resume point ("0"), take a fresh snapshot and reconcile it.
        // Ready NOW: stale-while-revalidate is the explicit SSR contract —
        // first paint renders the hydrated rows, the boundary converges them.
        void transport
          .subscribe(eagerSubId, table, handler, where, undefined, undefined, hc === "0" ? undefined : hc)
          .catch((e) => markError?.(e))
        markReady()
      } else {
        // A first-connect failure fails readiness loud (preload() rejects);
        // the policy-driven reconnect keeps retrying and the eventual
        // snapshot's markReady recovers the collection (error → ready, 0.8.2).
        void transport.subscribe(eagerSubId, table, handler, where).catch((e) => markError?.(e))
      }
    }
    return () => {
      hydratedCursor = null // GC wiped the rows; a retained cursor would lie
      transport.unsubscribe(eagerSubId)
    }
  }

  const mutationFn = async (params: {
    transaction: { id: string; mutations: ReadonlyArray<PendingMutationLike> }
  }): Promise<void> => {
    const ops: Array<MutOp> = params.transaction.mutations.map((m) => {
      // Eager filtered preflight: a write outside the static `where` would never
      // be confirmed by a delta — reject before any I/O.
      if (where != null && m.type !== "delete" && !matches(m.modified as Record<string, unknown>)) {
        throw new WriteOutsideSubError(
          `write to '${table}' (key '${m.key}') falls outside the collection's where filter`,
        )
      }
      return {
        type: m.type,
        key: m.key,
        cols:
          m.type === "delete"
            ? undefined
            : m.type === "insert"
              ? (m.modified as Record<string, unknown>)
              : (m.changes as Record<string, unknown>),
      }
    })
    await transport.sendMut({ t: "mut", txId: params.transaction.id, collection: table, ops })

    // On-demand: a confirmed write may land outside every loaded subset, so no
    // delta clears its direct optimistic upsert. A post-mutation empty sync
    // commit (after the tx completes) retires it; for an in-view write it is a
    // no-op (the synced row keeps it). See ADR-0002 C2.
    if (syncMode === "on-demand" && emptyCommit) {
      const run = emptyCommit
      setTimeout(() => run(), 0)
    }
  }

  // SSR syncMeta hooks (ADR-0011 D3) — called by TanStack's DbClient
  // dehydrate/hydrate (@tanstack/db ≥0.8.0, PR #1564 as merged). The eager
  // `where` fingerprint is the codec envelope — stable for the same
  // constructor code; a cross-deploy false mismatch merely downgrades to the
  // (always-sound) snapshot-reconcile path.
  const whereFingerprint = where == null ? undefined : codecEncode(where)
  // Merged-upstream contract (client.ts applyRows): on every hydrated chunk,
  // upstream asks exportSyncMeta() for the CURRENT meta and — when it exists —
  // routes the incoming meta through mergeSyncMeta. A fresh browser-side
  // adapter must therefore export UNDEFINED, not {cursor:"0"}: it holds no
  // claim, and a "0" claim would win the MIN-merge against every real
  // dehydrated cursor, silently downgrading all hydration to the
  // snapshot-reconcile path. "0" stays a REAL claim ("no resume point" — the
  // honest-truncate route); no-claim is the absence of meta. On the server the
  // SSR transport's reads establish the position this exports.
  const exportSyncMeta = (): DoSyncMeta | undefined => {
    // While a commit's receipt is unsettled the boundary cursor is not yet a
    // sound claim — export the last fully-settled position instead (see
    // pendingReceipts). Under-claiming is always safe.
    const live = pendingReceipts === 0 ? transport.appliedCursor : settledCursor
    const cursor = transport.hasPosition ? live : hydratedCursor
    if (cursor === null) return undefined
    return {
      v: 1,
      cursor,
      ...(whereFingerprint === undefined ? {} : { where: whereFingerprint }),
    }
  }
  const importSyncMeta = (meta: unknown): void => {
    // Upstream applies the dehydrated rows BEFORE this runs — there is no
    // veto. So a validation failure must fail loud AND fail safe: the rows
    // are in syncedData regardless, and silently skipping our bookkeeping
    // would start sync down the no-resume path with no reconcile intent —
    // a server-deleted hydrated row would then be stale forever. Set the
    // safe state ("0" → snapshot + reconcile) FIRST, then throw so the
    // version/corruption skew still surfaces to the app.
    let m: DoSyncMeta
    try {
      m = parseSyncMeta(meta)
    } catch (e) {
      hydratedCursor = "0"
      throw e
    }
    if (m.where === whereFingerprint) {
      hydratedCursor = m.cursor
      transport.seedCursor(m.cursor)
    } else {
      // The rows were dehydrated under a DIFFERENT eager filter: the cursor
      // is not a sound resume point for ours (see DoSyncMeta). "0" routes the
      // sync start to snapshot + reconcile; the transport cursor stays
      // unseeded so a bootstrap-window reconnect resnapshots too.
      hydratedCursor = "0"
    }
  }
  const mergeSyncMeta = (current: unknown, incoming: unknown): DoSyncMeta => {
    // Same fail-loud-but-SAFE contract as importSyncMeta: upstream calls
    // merge (then import) AFTER applying the chunk's rows, so a parse throw
    // here also can't veto anything — and upstream never reaches
    // importSyncMeta when merge throws, which would skip the safety net.
    let a: DoSyncMeta
    let b: DoSyncMeta
    try {
      a = parseSyncMeta(current)
      b = parseSyncMeta(incoming)
    } catch (e) {
      hydratedCursor = "0"
      throw e
    }
    // Fingerprint skew between the two sides means SOME chunk's rows were
    // dehydrated under a filter that is not ours — and they were APPLIED (no
    // veto). MIN would let the matching side's cursor survive the merge and
    // sail through import's fingerprint check, leaving the foreign rows with
    // no catch-up that covers them (codex review). No sound joint resume
    // point exists: return the honest "0" (snapshot-reconcile / on-demand
    // truncate route) under OUR fingerprint so import routes it there.
    if (a.where !== b.where) {
      return { v: 1, cursor: "0", ...(whereFingerprint === undefined ? {} : { where: whereFingerprint }) }
    }
    // MIN is self-healing: a late chunk's rows were already applied over
    // newer state (no veto); resuming from the EARLIER position replays the
    // window idempotently and re-freshens whatever the chunk clobbered.
    return BigInt(a.cursor) <= BigInt(b.cursor) ? a : b
  }

  const options = {
    id: opts.id ?? table,
    getKey,
    syncMode,
    sync: { sync, rowUpdateMode: "partial", exportSyncMeta, importSyncMeta, mergeSyncMeta },
    onInsert: mutationFn,
    onUpdate: mutationFn,
    onDelete: mutationFn,
  }
  // Descriptor opt-in (@tanstack/db ≥0.8.0): a `collectionOptions("id", …)`
  // descriptor over this config materializes per DbClient through this
  // factory, giving each client FRESH adapter state (hydratedCursor, subIds).
  // The transport is deliberately not recreated — it is the caller's; SSR
  // callers construct a per-request transport in their own factory closure.
  return withCollectionConfigFactory(
    options as never,
    () => doCollectionOptions(opts as Parameters<typeof doCollectionOptions>[0]) as never,
  ) as unknown as CollectionConfig<any, string>
}

/** What our sync() returns: a cleanup fn (eager) or the on-demand handlers. */
type SyncConfigResult =
  | (() => void)
  | {
      loadSubset: (o: LoadSubsetOptions) => true | Promise<void>
      unloadSubset: (o: LoadSubsetOptions) => void
      cleanup: () => void
    }
