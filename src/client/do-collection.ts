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
//   - 'on-demand': sync nothing up front; the collection calls loadSubset as
//     live queries mount, and unloadSubset when they unmount (ADR-0023). Each
//     request loads exactly the rows it asks for: identical requests share one
//     load; a request an existing server subscription already keeps live is a
//     one-shot fetch; any other opens its own subscription. A released
//     subscription takes the rows only it held with it. Ordering/limit are
//     applied client-side by IVM over the loaded rows. Writes that land outside
//     every loaded subset are confirmed and their optimistic overlay retired by
//     a post-mutation empty sync commit (ADR-0002 C2, verified).

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
  signal?: AbortSignal
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
    const writeDelta = (op: RowOp, key: string, cols: Record<string, unknown> | undefined): void => {
      ensureBegin()
      if (op === "delete") write({ type: "delete", key })
      // A catch-up emits the LATEST op per changed key, so a key deleted-
      // and-reinserted while we were away arrives as "insert" for a key we
      // still HOLD — TanStack's sync write throws DuplicateKeySyncError on
      // that unless values deep-equal. Apply a held-key insert as the
      // upsert it semantically is (update upserts; move-in, ADR-0002 C4).
      else if (op === "insert" && syncedHas(key)) write({ type: "update", value: cols })
      else write({ type: op, value: cols })
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
        onDelta: (op, key, cols) => writeDelta(op, key as string, cols),
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
      // --- Watches, acquisitions and holds (ADR-0023) --------------------------
      // A WATCH is one live server sub: it keeps a predicate (`where`) live, i.e.
      // every delta for a row it matches arrives on it. An ACQUISITION is one
      // `loadSubset` request, identified by its where/orderBy/limit: identical
      // requests share one; a request that an accepted watch already covers is
      // answered by a one-shot fetch and PINS that watch; any other opens its own
      // watch, whose snapshot is shaped by the request. HOLDS record which
      // watches keep each row live. Every sub on the socket gets a frame for
      // every changed key (ADR-0002 C4): the row if it matches, else a synthetic
      // delete — also when ANOTHER watch still matches it. So a `delete` from one
      // watch drops only that watch's hold, and the row goes when no watch holds
      // it; a real delete reaches every holder, so the last one removes it. A
      // released watch takes the rows only it held with it (the release/load gap:
      // nothing would keep them live, so a later delete would never reach us).
      // Per loaded row: one `holders` entry, a small Set (usually one watch), and
      // one entry in each holding watch's `rows`. Every frame is O(1) bookkeeping.
      interface Watch {
        subId: string
        where: unknown
        /** Encoded top-level conjuncts of `where`: the watch covers any request
         *  whose conjuncts include all of them. */
        conjunctKeys: Array<string>
        pins: number
        /** Reached its first snap-end: the DO accepted it. */
        bootstrapped: boolean
        /** Settles true at its first snap-end, false if refused or retired;
         *  rejects if its subscribe itself fails. */
        accepted: Promise<boolean>
        accept: (ok: boolean) => void
        gen: number
        rows: Set<string>
      }
      interface Acquisition {
        key: string
        refs: number
        ready: Promise<void>
        watch: Watch | null
        released: boolean
        gen: number
        /** Resolve the load without rows: a truncate or a release overtook it. */
        settle: () => void
        fail: (e: unknown) => void
      }
      // Bumped by every truncate: acquisitions and watches from before it no
      // longer share or cover, so upstream's truncate replay (load-then-release
      // on 0.8, release-then-reload on 0.9) really reloads every demand.
      let gen = 0
      let alive = true // false after cleanup: late async work installs nothing
      const watches = new Set<Watch>()
      const acquisitions = new Map<string, Acquisition>() // current generation, by request key
      const acquisitionOf = new WeakMap<object, Acquisition>() // by the options object core loaded
      const outstanding = new WeakMap<object, number>() // loads not yet released, per options object
      const holders = new Map<string, Set<Watch>>()
      const requestKey = (o: LoadSubsetOptions): string => codecEncode([o.where ?? null, o.orderBy ?? null, o.limit ?? null])

      /** Release must never throw (0.9 UnloadSubsetFn). A send that throws means
       *  the socket is closing; the DO drops every sub of a closed socket
       *  (webSocketClose), so there is nothing to retry. */
      const unsubscribeQuietly = (subId: string): void => {
        try {
          transport.unsubscribe(subId)
        } catch {
          /* closing socket */
        }
      }
      const hold = (key: string, w: Watch): void => {
        let hs = holders.get(key)
        if (!hs) holders.set(key, (hs = new Set()))
        hs.add(w)
        w.rows.add(key)
      }
      /** Drop `w`'s hold on `key`; true when no watch holds it any more. */
      const unhold = (key: string, w: Watch): boolean => {
        w.rows.delete(key)
        const hs = holders.get(key)
        if (!hs) return true
        hs.delete(w)
        if (hs.size > 0) return false
        holders.delete(key)
        return true
      }
      /** Drop every hold `w` has; delete the rows no other watch holds —
       *  unconditionally: a row may still be only an insert in the open sync
       *  transaction, or in a commit core has queued, and so not yet synced
       *  (a delete of an absent key is a no-op, as for any synthetic delete). */
      const dropHolds = (w: Watch): void => {
        for (const key of w.rows) {
          const hs = holders.get(key)
          hs?.delete(w)
          if (hs && hs.size > 0) continue
          holders.delete(key)
          ensureBegin()
          write({ type: "delete", key })
        }
        w.rows.clear()
      }
      /** Truncate the whole collection and start a new generation. Every
       *  current watch is retired at once — unsubscribed, so its trailing frames
       *  find no handler even if core applies the truncate (and replays every
       *  demand with fresh subs) only later, behind a persisting transaction;
       *  and older acquisitions no longer share, cover or install pages. */
      const truncateAll = (): CommitReceipt => {
        for (const w of watches) {
          w.accept(false)
          unsubscribeQuietly(w.subId)
        }
        watches.clear()
        holders.clear()
        const orphans = [...acquisitions.values()]
        acquisitions.clear()
        gen++
        flush()
        begin()
        truncate()
        const receipt = track(commit())
        // A retired watch's snapshot will never arrive: settle every load still
        // waiting on one, or it would stay pending forever (0.8 keeps it in
        // isLoadingSubset). Resolve, not reject — core would report a rejected
        // load as the query's error, and its truncate replay reloads them all —
        // but only once the truncate is applied (core's replay barrier exists
        // from then on); a truncate that fails fails them.
        afterApplied(
          receipt,
          () => orphans.forEach((a) => a.settle()),
          (e) => orphans.forEach((a) => a.fail(e)),
        )
        return receipt
      }
      const unpin = (w: Watch): void => {
        if (--w.pins > 0 || !watches.delete(w)) return
        unsubscribeQuietly(w.subId)
        dropHolds(w)
        flush()
      }
      const conjuncts = (e: unknown): Array<unknown> => {
        const f = e as { type?: unknown; name?: unknown; args?: unknown } | null
        return f?.type === "func" && f.name === "and" && Array.isArray(f.args) ? f.args.flatMap(conjuncts) : [e]
      }
      const conjunctKeys = (where: unknown): Array<string> => conjuncts(where).map((c) => codecEncode(c ?? null))
      /** A watch of this generation that keeps every row of `where` live:
       *  unfiltered, or every conjunct of its predicate is a conjunct of `where`
       *  (so `where` implies it). Upstream's boundary-tie requests are
       *  `and(subscriptionWhere, tie)`; a subscription `where` may itself be an
       *  `and`. `pending` also admits a watch the DO has not accepted yet (the
       *  caller waits for it). */
      const findCover = (where: unknown, pending = false): Watch | null => {
        let keys: Set<string> | null = null
        for (const w of watches) {
          if (w.gen !== gen || !(w.bootstrapped || pending)) continue
          if (w.where == null) return w
          keys ??= new Set(conjunctKeys(where))
          if (w.conjunctKeys.every((k) => keys!.has(k))) return w
        }
        return null
      }
      /** Write a fetched page insert-if-absent (ADR-0003: a key already synced
       *  keeps its value; the watch streams its changes) and attribute every row
       *  to the watch that keeps it live. Settles once the rows are visible. */
      const installPage = async (rows: Array<unknown>, w: Watch | null): Promise<void> => {
        ensureBegin()
        for (const r of rows) {
          const key = getKey(r as Record<string, unknown>)
          if (!syncedHas(key)) write({ type: "insert", value: r })
          if (w && w.gen === gen && watches.has(w)) hold(key, w)
        }
        const receipt = flush()
        if (receipt !== true) await receipt
      }

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
      let catchupId: string | null = null
      let readyGate: Promise<void>
      if (hc !== null && hc !== "0") {
        const id = `${table}#hydrate#${++subSeq}`
        catchupId = id
        const done = (): void => {
          if (catchupId === id) catchupId = null
          unsubscribeQuietly(id)
        }
        readyGate = transport.subscribe(
          id,
          table,
          {
            onSnap: () => {}, // catch-ups never snapshot; reset's resnapshot is dropped (unsubbed)
            onSnapEnd: () => {},
            onDelta: (op, key, cols) => {
              // Unfiltered: a delete here is a real delete — no hold survives it.
              if (op === "delete") {
                for (const w of holders.get(key as string) ?? []) w.rows.delete(key as string)
                holders.delete(key as string)
              }
              writeDelta(op, key as string, cols)
            },
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
              truncateAll()
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
          truncateAll()
        })
      } else {
        readyGate = transport.connect()
      }
      // A failed gate fails readiness loud (markError; preload() rejects with
      // the cause) instead of hanging — a later retried preload() recovers
      // once the transport's policy-driven reconnect succeeds (0.8.2).
      void readyGate.then(markReady, (e) => markError?.(e))

      const openWatch = (o: LoadSubsetOptions, onReady: () => void, onFail: (e: unknown) => void, onRefused: () => void): Watch => {
        let accept!: (ok: boolean) => void
        let failAccept!: (e: unknown) => void
        const accepted = new Promise<boolean>((res, rej) => {
          accept = res
          failAccept = rej
        })
        accepted.catch(() => {}) // observed by covered requests, if any
        const w: Watch = {
          subId: `${table}#${++subSeq}`, // fresh per watch: a released sub's late frames find no handler
          where: o.where,
          conjunctKeys: conjunctKeys(o.where),
          pins: 1,
          bootstrapped: false,
          accepted,
          accept,
          gen,
          rows: new Set(),
        }
        watches.add(w)
        const handler: SubHandler = {
          onSnap: (_key, row) => {
            const key = getKey(row as Record<string, unknown>)
            ensureBegin()
            // A held key's snapshot row is an upsert (the snapshot is never
            // staler than the held synced row, C1′).
            write(syncedHas(key) ? { type: "update", value: row } : { type: "insert", value: row })
            hold(key, w)
          },
          onSnapEnd: () => {
            w.bootstrapped = true
            w.accept(true)
            afterApplied(flush(), onReady, onFail)
          },
          onDelta: (op, key, cols) => {
            if (op !== "delete") {
              writeDelta(op, key as string, cols)
              hold(key as string, w)
            } else if (unhold(key as string, w)) writeDelta(op, key as string, cols)
          },
          onUptodate: () => flush(),
          onReset: () => {
            if (!w.bootstrapped) {
              // Before its first snapshot a reset can only be a refusal
              // (unsupported predicate, sub cap, unknown collection): a sub that
              // never bootstrapped never resubscribes with `since`, so no
              // below-floor reset reaches it. It holds no rows. Settle its load
              // as before and touch nothing else — a truncate here looped on 0.9
              // (truncate → replay → the same refusal) and wiped every other
              // subset on 0.8.
              watches.delete(w)
              w.accept(false)
              unsubscribeQuietly(w.subId)
              onRefused()
              onReady()
              return
            }
            // Below the retention floor on reconnect: whatever this watch held
            // may be stale, and so may rows that covered fetches and cursor pages
            // attributed to it, which its bounded resnapshot never restores. Only
            // upstream's truncate replay knows every demand, so truncate; the new
            // generation makes that replay reload each one with fresh subs, and
            // the old subs' trailing frames find no handler.
            afterApplied(truncateAll(), onReady, onFail)
          },
          // The transport restarts a sub that never bootstrapped from a fresh
          // snapshot (reconnect before its snap-end): drop what the partial one
          // delivered, so a row deleted meanwhile does not survive the restart.
          onRestart: () => dropHolds(w),
        }
        void transport.subscribe(w.subId, table, handler, o.where, o.orderBy, o.limit).catch((e) => {
          onFail(e)
          failAccept(e) // requests waiting to be covered by it fail with it
        })
        return w
      }

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
        const g = gen
        const cover = findCover(o.where) // the watch that keeps these rows live, as of the request
        const page = await transport.fetch({
          t: "fetch",
          fetchId: `${table}#fetch#${++subSeq}`,
          collection: table,
          where: o.where,
          cursor: { whereFrom, whereCurrent },
          orderBy: o.orderBy,
          limit: o.limit,
        })
        // A request cancelled, a truncate (new generation), or a session cleaned
        // up meanwhile: install nothing (0.9 cooperative cancellation; a page
        // read before a truncate would come back as an unheld ghost).
        if (!alive || o.signal?.aborted || g !== gen) return
        // 0.8.5 contract: a subset load settles only once its rows are visible.
        await installPage(page, cover)
      }

      /** A request a watch covers: its exact rows in one fetch, once the DO has
       *  accepted the watch. A refused watch covers nothing: the request then
       *  opens its own. */
      const fetchCovered = async (acq: Acquisition, o: LoadSubsetOptions, w: Watch): Promise<void> => {
        if (!(await w.accepted)) {
          if (!alive || acq.released || acq.gen !== gen) return
          unpin(w)
          await new Promise<void>((resolve, reject) => {
            acq.watch = openWatch(o, resolve, reject, () => {
              if (acquisitions.get(acq.key) === acq) acquisitions.delete(acq.key)
            })
          })
          return
        }
        const page = await transport.fetch({
          t: "fetch",
          fetchId: `${table}#fetch#${++subSeq}`,
          collection: table,
          where: o.where,
          orderBy: o.orderBy,
          limit: o.limit,
        })
        // Released by every owner, or from before a truncate: install nothing. An
        // acquisition can be shared, so one owner's abort does not cancel it —
        // core releases an aborted owner, and the last release sets `released`.
        if (!alive || acq.released || acq.gen !== gen) return
        await installPage(page, w)
      }

      const loadSubset = (o: LoadSubsetOptions): true | Promise<void> => {
        if (o.cursor) return loadMore(o)
        // Neither a watch's snapshot nor a fetch can skip rows without a cursor:
        // fail loud rather than answer with the wrong page. (Upstream pairs a
        // non-zero offset with a cursor.)
        if (o.offset) {
          return Promise.reject(new Error(`on-demand '${table}': a subset request with offset ${o.offset} and no cursor is unsupported`))
        }
        outstanding.set(o, (outstanding.get(o) ?? 0) + 1)
        const key = requestKey(o)
        const existing = acquisitions.get(key)
        if (existing) {
          existing.refs++
          acquisitionOf.set(o, existing)
          return existing.ready
        }
        let resolve!: () => void
        let reject!: (e: unknown) => void
        const ready = new Promise<void>((res, rej) => {
          resolve = res
          reject = rej
        })
        const acq: Acquisition = { key, refs: 1, ready, watch: null, released: false, gen, settle: () => resolve(), fail: reject }
        acquisitions.set(key, acq)
        acquisitionOf.set(o, acq)
        // A completed load also (re)marks ready — the recovery path out of a
        // failed ready-gate's error state (idempotent otherwise). A send failure
        // or rejected receipt rejects THIS load (0.8.4 surfaces it
        // per-subscription as loadSubset:error), not the whole collection.
        const loaded = (): void => {
          resolve()
          markReady()
        }
        // A pending watch covers too: a compatible request mounted in the same
        // tick waits for it rather than opening a second subscription.
        const cover = findCover(o.where, true)
        if (cover) {
          cover.pins++
          acq.watch = cover
          fetchCovered(acq, o, cover).then(loaded, reject)
        } else {
          // Forward orderBy/limit so the snapshot is this request's bounded
          // window (recent N), not the whole where-subset. The watch's
          // predicate is still `where`, so entering rows are delivered.
          acq.watch = openWatch(o, loaded, reject, () => {
            // Refused: a later identical request tries again.
            if (acquisitions.get(key) === acq) acquisitions.delete(key)
          })
        }
        return ready
      }

      const unloadSubset = (o: LoadSubsetOptions): void => {
        // Symmetric with loadSubset: a cursor load was a one-shot fetch that
        // never took a refcount, so its unload must not release one either.
        if (o.cursor) return
        // Idempotent (0.9 UnloadSubsetFn): core releases each acquisition once,
        // with the options object it loaded (0.8.x and 0.9.x alike). A repeat
        // release, or an object that was never loaded, is a no-op — releasing by
        // an equal object's request key could release another owner's load.
        const acq = acquisitionOf.get(o)
        const n = outstanding.get(o) ?? 0
        if (!acq || n <= 0) return
        outstanding.set(o, n - 1)
        if (acq.released || --acq.refs > 0) return
        acq.released = true
        acq.settle() // released before its rows arrived: nothing will settle it now
        if (acquisitions.get(acq.key) === acq) acquisitions.delete(acq.key)
        if (acq.watch) unpin(acq.watch)
      }

      return {
        loadSubset,
        unloadSubset,
        cleanup: () => {
          hydratedCursor = null // GC wiped the rows; a retained cursor would lie
          alive = false
          // Unsubscribe what this collection owns — never close the transport:
          // it is the caller's and may serve other collections (a shared
          // transport stopped delivering to them when on-demand closed it).
          for (const w of watches) {
            w.accept(false)
            unsubscribeQuietly(w.subId)
          }
          watches.clear()
          holders.clear()
          acquisitions.clear()
          if (catchupId !== null) unsubscribeQuietly(catchupId)
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
