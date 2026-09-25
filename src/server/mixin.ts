// Syncable(Base) — the sync machinery as a mixin factory (ADR-0015).
//
// The body of `SyncDurableObject` extracted into a factory that applies over any
// Durable Object subclass, so one DO class can be both its framework's host (the
// Agents SDK `Agent`, `@cloudflare/think`'s `Think`, or a bare `DurableObject`)
// AND a tddc sync source. `SyncDurableObject` (sync-do.ts) is the trivial
// application of this factory over `DurableObject`, preserving 0.4.0 exactly.
//
// Cohosting safety rests on three independent discriminators (ADR-0015):
//   - a reserved hibernation tag (SYNC_TAG) on every sync socket, so wake-time
//     restore and the broadcaster only ever touch tddc's own sockets;
//   - a plain socket attachment with no partyserver `__pk` key, so a host that
//     filters on `__pk` (partyserver, and therefore Agent/Think) is blind to
//     sync sockets with no cooperation from tddc;
//   - a dedicated fetch path (default "/_sync") so upgrades are partitioned
//     before either protocol sees them, with non-matching traffic delegated to
//     `super.fetch`.
// The `sql` getter is deliberately absent: a property `sql` would shadow the
// `sql` tagged-template method partyserver/agents define (ADR-0015). Internals
// reach SQLite through `this.ctx.storage.sql`.

import { DurableObject } from "cloudflare:workers"
import type { SqlStorage, SqlStorageValue } from "@cloudflare/workers-types"
import { createFrameCodec, type FrameCodec } from "../wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../wire/frames.ts"
import {
  compactChanges,
  currentSeq,
  ensureTriggers,
  getDrainCursor,
  highWaterSeq,
  hydrateRows,
  initSchema,
  minChangeSeq,
  pruneChanges,
  readChangesSince,
  readChangesSinceFor,
  setDrainCursor,
} from "./changes.ts"
import { Broadcaster } from "./broadcast.ts"
import { decodeResult, encodeResult, lookupTx, recordTx, type SeenTx, sweepDedup } from "./dedup.ts"
import { compileSchema, type CompiledSync, type SyncSchema, ValidationError } from "./registry.ts"
import { andPredicates, compileSubsetQuery, UnsupportedPredicateError } from "./sql-compiler.ts"
import {
  deletePersistedSocketSubs,
  deletePersistedSub,
  loadPersistedSubs,
  persistSub,
  SubscriptionRegistry,
  type Sub,
  sweepOrphanSubs,
} from "./subscriptions.ts"

/** Reserved hibernation tag stamped on every sync socket. The wake-time restore
 *  (`getWebSockets(SYNC_TAG)`) and every handler's socket-ownership check key off
 *  this tag, so tddc never touches a host's sockets and vice versa (ADR-0015). */
export const SYNC_TAG = "_tddc"

/** Second tag stamped at accept: a per-socket id that survives hibernation and
 *  keys this socket's durable `_sync_subs` rows, so a wake can re-associate
 *  subscriptions with the surviving socket (ADR-0019). Tags — not the
 *  attachment, which is contractually the author's claims (ADR-0001 D13,
 *  ADR-0015) and capped at 16 KiB. Budget: 2 of Cloudflare's 10 tags/socket. */
const SOCKET_ID_TAG_PREFIX = "_tddc.sid:"

/** Outbound frame-size warning threshold (ADR-0018): observability only, and
 *  deliberately NOT a knob. Inbound is enforced at the edge-cap constant;
 *  outbound is deliberately unenforced — breaking a correct broadcast to save
 *  bandwidth would invert the failure hierarchy — so this warn is the one
 *  production breadcrumb. The real fix for oversize full-row rebroadcasts is
 *  column projection (issue #28). */
const WARN_OUTBOUND_FRAME_BYTES = 1_048_576

/** Runtime options for the mixin. `configure()` in your constructor.
 *  Numeric tuning knobs stay protected overridable fields (see ADR-0015). */
export interface SyncableOptions<TUser = unknown> {
  /** URL pathname the mixin claims in `fetch`. Default "/_sync". Ignored on a
   *  bare `DurableObject` base, which has no host `fetch` to delegate to and so
   *  owns every upgrade (0.4.0 parity). */
  path?: string
  /** `setWebSocketAutoResponse("ping","pong")` — DO-global. Default: true when
   *  Base === DurableObject (preserves SyncDurableObject), false otherwise. */
  autoResponse?: boolean
  /** `PRAGMA case_sensitive_like = ON` — connection-global. Same defaulting rule
   *  as `autoResponse`. */
  caseSensitiveLike?: boolean
  /** Auth hook, same contract as the legacy `parseAttachment`: validate the
   *  upgrade and produce the attachment, or throw a `Response` to reject. */
  parseAttachment?: (req: Request) => TUser | Promise<TUser>
}

/** The single facade the mixin adds. Everything that used to be a loose
 *  protected member lives behind this one name to shrink the collision surface
 *  with an arbitrary host to the four runtime-dispatched methods plus `sync`. */
export interface SyncApi<Env, TUser> {
  /** Unchanged semantics (ADR-0007): call in blockConcurrencyWhile after your
   *  tables exist. */
  registerSync(schema: SyncSchema<TUser, Env>): void
  /** Unchanged semantics (ADR-0006): server write + drain + broadcast. */
  runSyncedWrite<T>(fn: (sql: SqlStorage) => T): T
  /** The resolved auth hook (as configured). */
  parseAttachment(req: Request): TUser | Promise<TUser>
  /** Set options; safe to call from the host constructor. */
  configure(opts: SyncableOptions<TUser>): void
  /** The compiled schema; throws (ADR-0007) if `registerSync` hasn't run yet.
   *  Behind the facade so the name never shadows a host member (ADR-0015). */
  readonly registry: CompiledSync<TUser, Env>
  /** Drain the CDC log and broadcast pending deltas (ADR-0006). The manual
   *  broadcast trigger for a raw server-side write done outside `runSyncedWrite`. */
  drainAndBroadcast(): void
}

/** One consistent snapshot read for SSR (ADR-0011 D1): the request shape and
 *  result of `readSyncSnapshot`, callable over the DO binding as plain RPC. */
export interface SyncSnapshotReq {
  collection: string
  where?: unknown
  orderBy?: unknown
  limit?: number
}
export interface SyncSnapshotRes {
  rows: Array<Record<string, SqlStorageValue>>
  cursor: string
}

/** The surface the mixin adds to `Base`. The four methods are real, runtime-
 *  dispatched overrides so workerd finds them on the prototype;
 *  `readSyncSnapshot` is public (not behind the `sync` facade) because DO RPC
 *  dispatches only on public instance members — one deliberate addition to the
 *  host-collision surface (ADR-0011 D1, amending ADR-0015's four-method rule). */
export interface SyncMixin<Env, TUser> {
  readonly sync: SyncApi<Env, TUser>
  readSyncSnapshot(req: SyncSnapshotReq, request: Request): Promise<SyncSnapshotRes>
  fetch(request: Request): Promise<Response>
  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void>
  webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): void | Promise<void>
  webSocketError(ws: WebSocket, error: unknown): void | Promise<void>
}

/** The structural constructor shape the mixin needs from a host — nothing more
 *  than a `DurableObject` subclass. No framework is imported or depended on. */
export type DOCtor = abstract new (...args: any[]) => DurableObject<any>

/**
 * `Syncable` — curried so `Env` and `TUser` are pinned by the caller while
 * `Base` stays a runtime value:
 *
 *   class FeedAgent extends Syncable<Env, Claims>()(Agent<Env, State>) { … }
 *
 * The outer call pins the generics; the inner call takes the runtime `Base` and
 * returns a class extending it with the `sync` facade re-exposed at `Env`/`TUser`.
 */
export function Syncable<Env = unknown, TUser = unknown>() {
  return function <TBase extends DOCtor>(Base: TBase) {
    abstract class SyncableMixin extends Base {
      // ---- configuration ------------------------------------------------------
      /** Fetch path this instance claims (see SyncableOptions.path). */
      #path = "/_sync"
      #autoResponse = false
      #caseSensitiveLike = false
      #parseAttachmentHook: (req: Request) => TUser | Promise<TUser> = () => undefined as TUser
      /** True when `Base` defines a `fetch` we can delegate non-sync traffic to.
       *  On a bare `DurableObject` this is false and the mixin owns all upgrades. */
      readonly #hasSuperFetch: boolean
      /** True when `Base === DurableObject`. A bare DO shares its sockets with no
       *  host, so tddc owns every socket (incl. legacy untagged 0.4.0 sockets). */
      readonly #isBareDO: boolean

      // ---- tuning knobs (protected, overridable — ADR-0015) ------------------
      /** Egress coalescer tick (ms) — the single user-perceived-latency knob. */
      protected readonly tickMs: number = 50
      /** Compact the change log every this-many drained mutations (not on a timer —
       *  an alarm would wake idle DOs; this rides recent work). */
      protected readonly compactionEvery: number = 200
      /** Age bound for `_sync_changes` (ADR-0009). Changes older than this are
       *  pruned during compaction; a reconnect older than the surviving floor gets a
       *  full re-snapshot instead of a delta. `null` disables retention. Default 2 days. */
      protected readonly changelogRetentionMs: number | null = 172_800_000
      /** Dedup retention window (ms), independent of changelog retention (C5). */
      protected readonly dedupRetentionMs: number = 3_600_000
      /** Maximum ops in a single `mut` frame (ADR-0012). Reject-don't-truncate. */
      protected readonly maxOpsPerMutation: number = 128
      /** Maximum concurrent subscriptions per socket (ADR-0012). */
      protected readonly maxSubsPerSocket: number = 256
      /** Maximum inbound frame size in bytes (ADR-0012). */
      protected readonly maxFrameBytes: number = 1_048_576

      // ---- internal machinery (private — off the collision surface) ----------
      #compiled: CompiledSync<TUser, Env> | undefined
      readonly #codec: FrameCodec = createFrameCodec()
      readonly #subs = new SubscriptionRegistry()
      #writesSinceCompaction = 0
      readonly #broadcaster: Broadcaster
      readonly #liveWs = new Set<WebSocket>()
      readonly #api: SyncApi<Env, TUser>

      constructor(...args: any[]) {
        super(...args)
        // Base-dependent defaults: reproduce 0.4.0 exactly on a bare DO, default
        // the two DO-global side effects OFF over any other host (ADR-0015).
        this.#isBareDO = (Base as unknown) === (DurableObject as unknown)
        this.#hasSuperFetch = typeof (Base.prototype as { fetch?: unknown }).fetch === "function"
        this.#autoResponse = this.#isBareDO
        this.#caseSensitiveLike = this.#isBareDO
        if (this.#autoResponse) this.#applyAutoResponse(true)
        if (this.#caseSensitiveLike) this.#applyCaseSensitiveLike(true)
        // Restore live sockets after a hibernation wake. On a bare DO tddc owns
        // EVERY socket (there is no host to share with), so restore all of them —
        // including legacy untagged sockets accepted by a pre-mixin 0.4.0 build
        // that survive the wake across an upgrade. Over any other base, restore
        // ONLY our tagged sockets so the broadcaster never touches a host socket.
        const restore = this.#isBareDO ? this.ctx.getWebSockets() : this.ctx.getWebSockets(SYNC_TAG)
        for (const ws of restore) this.#liveWs.add(ws)
        this.#broadcaster = new Broadcaster((ws, frame) => this.#send(ws, frame), this.tickMs)
        this.#broadcaster.start(() => this.#liveWs)
        const self = this
        this.#api = {
          registerSync: (schema) => self.#registerSync(schema),
          runSyncedWrite: (fn) => self.#runSyncedWrite(fn),
          parseAttachment: (req) => self.#parseAttachmentHook(req),
          configure: (opts) => self.#configure(opts),
          drainAndBroadcast: () => self.#drainAndBroadcast(),
          get registry() {
            return self.#registry
          },
        }
      }

      get sync(): SyncApi<Env, TUser> {
        return this.#api
      }

      #applyAutoResponse(on: boolean): void {
        // Auto-pong via the runtime: survives hibernation, no per-message billing.
        // `off` clears the pair so `configure({ autoResponse: false })` is a real
        // toggle (undoing the bare-DO default), not a dead option.
        this.ctx.setWebSocketAutoResponse(on ? new WebSocketRequestResponsePair("ping", "pong") : undefined)
      }

      #applyCaseSensitiveLike(on: boolean): void {
        // Make SQLite LIKE case-sensitive so the SQL snapshot path matches
        // @tanstack/db's case-sensitive `like` evaluator on the delta path — the
        // single source of truth for filtered-subscription membership (ADR-0013).
        // Connection-scoped; re-applied on every instantiation (incl. a wake). The
        // `off` branch makes `configure({ caseSensitiveLike: false })` a real toggle.
        this.ctx.storage.sql.exec(`PRAGMA case_sensitive_like = ${on ? "ON" : "OFF"}`)
      }

      #configure(opts: SyncableOptions<TUser>): void {
        if (opts.path !== undefined) this.#path = opts.path
        if (opts.parseAttachment !== undefined) this.#parseAttachmentHook = opts.parseAttachment
        if (opts.autoResponse !== undefined) {
          this.#autoResponse = opts.autoResponse
          this.#applyAutoResponse(opts.autoResponse)
        }
        if (opts.caseSensitiveLike !== undefined) {
          this.#caseSensitiveLike = opts.caseSensitiveLike
          this.#applyCaseSensitiveLike(opts.caseSensitiveLike)
        }
      }

      /** SQLite handle. Private accessor (NOT a public `sql` getter — that would
       *  shadow the host's `sql` tagged-template method, ADR-0015). */
      get #sql(): SqlStorage {
        return this.ctx.storage.sql
      }

      /** The compiled schema. Throws if `registerSync` hasn't run yet (ADR-0007). */
      get #registry(): CompiledSync<TUser, Env> {
        if (!this.#compiled) {
          throw new Error(
            "sync not registered — call this.sync.registerSync(schema) in your constructor's " +
              "blockConcurrencyWhile, after creating your tables",
          )
        }
        return this.#compiled
      }

      /**
       * Wire collections for sync: validate each table is sync-compatible (ADR-0007)
       * and reconcile its CDC triggers — install the registered set, drop triggers
       * for any collection no longer registered (ADR-0008). The author owns table
       * creation; call this AFTER the tables exist. Idempotent.
       */
      #registerSync(schema: SyncSchema<TUser, Env>): void {
        const compiled = compileSchema(schema)
        initSchema(this.#sql)
        ensureTriggers(this.#sql, compiled.collections.values())
        this.#compiled = compiled
        this.#restoreSubs()
        this.#reconcileSubs()
      }

      /** Re-associate durable `_sync_subs` rows with the sockets that survived
       *  a hibernation wake (ADR-0019). Runs inside `registerSync` — the
       *  author's documented constructor obligation — so the registry is whole
       *  before any frame dispatch or drain can consult it (the guarantee is
       *  conditional on that obligation; a frame arriving before registration
       *  fails loud at `#registry`). Idempotent (a second registerSync re-adds
       *  the same entries; `add` replaces).
       *
       *  A row whose predicate no longer compiles (a version change narrowed
       *  the evaluator floor) closes the SOCKET with a non-terminal code
       *  instead of sending `reset`: the client's `onReset` truncates but does
       *  not re-subscribe (a reset would strand the query dead while its
       *  cursor advances — codex review), whereas a close engages the
       *  reconnect machinery, which re-subscribes everything recoverable and
       *  routes the bad predicate through `#handleSub`'s pre-existing,
       *  app-visible rejection. */
      #restoreSubs(): void {
        for (const ws of this.#liveWs) {
          const sid = this.#socketIdFor(ws)
          if (!sid) continue // legacy pre-0.6 socket: nothing was persisted
          for (const row of loadPersistedSubs(this.#sql, sid)) {
            try {
              this.#subs.add(ws, row.subId, row.collection, row.where)
            } catch (e) {
              if (e instanceof UnsupportedPredicateError) {
                console.error(
                  `restored sub '${row.subId}' on '${row.collection}' no longer compiles (${e.message}); ` +
                    `closing socket for clean re-subscribe`,
                )
                this.#dropSocketSubs(ws) // durable rows too — the retry must re-derive from the client
                this.#liveWs.delete(ws)
                ws.close(1011, "sync restore failed; resubscribe")
                break // remaining rows for this socket are moot
              }
              throw e
            }
          }
        }
      }

      /** Drop subscriptions whose collection is not in the compiled schema —
       *  in memory AND durably, with a `reset` so the client truncates
       *  (correct terminal state: the collection is gone, there is nothing to
       *  re-subscribe to). Covers a wake restoring rows from an older schema
       *  AND a mid-life re-registration that removed a collection; without
       *  this, such subs sit in the registry as zombies no drain will ever
       *  service (codex review). */
      #reconcileSubs(): void {
        for (const ws of this.#liveWs) {
          const sid = this.#socketIdFor(ws)
          for (const sub of this.#subs.forWs(ws)) {
            if (this.#registry.collections.has(sub.collection)) continue
            console.error(`sub '${sub.subId}' dropped: collection '${sub.collection}' is not registered`)
            this.#subs.remove(ws, sub.subId)
            if (sid) deletePersistedSub(this.#sql, sid, sub.subId)
            this.#send(ws, { t: "reset", sub: sub.subId })
          }
        }
      }

      // ---- runtime-dispatched overrides --------------------------------------

      /**
       * One consistent snapshot of a collection plus a durable resume cursor,
       * WITHOUT a WebSocket — the SSR read path (ADR-0011 D1). Throws on an
       * unknown collection or an un-lowerable predicate (fail loud; RPC
       * propagates the error to the caller).
       *
       * `request` is REQUIRED and runs through the SAME gate as the WS upgrade:
       * the configured `parseAttachment` — pass the claims-bearing Request the
       * worker already forges (or forwards) for the socket path. One hook guards
       * both paths, so an author's tenant check cannot be silently bypassed by
       * the read path (and the minted claims are the seam where uniform
       * read-scoping would land, on subs and snapshots alike). A rejecting
       * parseAttachment rejects the RPC. The await happens BEFORE the reads:
       * rows and cursor are still taken at one position (synchronous SQLite,
       * no await between them).
       *
       * The cursor is the durable high-water mark, not bare `currentSeq` — see
       * `highWaterSeq`. A cursor of "0" honestly means "no resume point" and the
       * client must reconcile a fresh snapshot instead of catching up.
       *
       * BLOB parity (ADR-0017): the WS path's codec normalizes bare ArrayBuffer
       * to Uint8Array; RPC structured clone would leak ArrayBuffer through, so
       * rows are normalized here — one blob shape on both paths.
       */
      async readSyncSnapshot(req: SyncSnapshotReq, request: Request): Promise<SyncSnapshotRes> {
        await this.#parseAttachmentHook(request) // the one gate (claims unused until read-scoping exists)
        const coll = this.#registry.collections.get(req.collection)
        if (!coll) throw new Error(`readSyncSnapshot: unknown collection '${req.collection}'`)
        const query = compileSubsetQuery(req.collection, {
          where: req.where,
          orderBy: req.orderBy,
          limit: req.limit,
        })
        const rows = Array.from(this.#sql.exec(query.sql, ...query.params)).map((row) => {
          let out: Record<string, SqlStorageValue> | undefined
          for (const [k, v] of Object.entries(row)) {
            if (v instanceof ArrayBuffer) {
              out ??= { ...row }
              out[k] = new Uint8Array(v) as unknown as SqlStorageValue
            }
          }
          return out ?? row
        }) as Array<Record<string, SqlStorageValue>>
        return { rows, cursor: String(highWaterSeq(this.#sql)) }
      }

      override async fetch(req: Request): Promise<Response> {
        if (req.headers.get("Upgrade") === "websocket" && this.#claimsUpgrade(req)) {
          return this.#acceptSyncSocket(req)
        }
        // Non-sync traffic: delegate to the host if it has a fetch (Agent.fetch
        // itself delegates upward; partyserver's fetch handles the rest). On a
        // bare DurableObject there is nothing to delegate to.
        if (this.#hasSuperFetch) {
          return (super.fetch as (r: Request) => Promise<Response>).call(this, req)
        }
        return new Response("expected websocket upgrade", { status: 426 })
      }

      /** True iff this upgrade is ours. On a bare DO (no host fetch) we own every
       *  upgrade regardless of path — exactly 0.4.0. With a host present we claim
       *  only the configured path and let everything else fall to `super.fetch`. */
      #claimsUpgrade(req: Request): boolean {
        if (!this.#hasSuperFetch) return true
        const pathname = new URL(req.url).pathname
        return pathname === this.#path || pathname.endsWith(this.#path)
      }

      async #acceptSyncSocket(req: Request): Promise<Response> {
        let attachment: TUser
        try {
          attachment = await this.#parseAttachmentHook(req)
        } catch (e) {
          if (e instanceof Response) return e
          return new Response("unauthorized", { status: 401 })
        }

        // The socket attachment must not carry partyserver's reserved `__pk` key,
        // or a partyserver-like host would mis-claim this sync socket as its own
        // (the second, host-side discriminator). The SYNC_TAG below always keeps
        // tddc's own side correct; this guard fails loud so a claim object that
        // happens to use `__pk` cannot silently break host isolation.
        if (!this.#isBareDO && attachment != null && typeof attachment === "object" && "__pk" in attachment) {
          console.error(
            "sync attachment carries a reserved `__pk` key — a partyserver-like host will mis-claim " +
              "this socket. Remove `__pk` from your parseAttachment claims (ADR-0015).",
          )
        }

        const pair = new WebSocketPair()
        const client = pair[0]
        const server = pair[1]
        server.serializeAttachment(attachment)
        // Tagged accept (SYNC_TAG) + plain attachment (no `__pk`): the two
        // independent discriminators that keep host and sync sockets apart.
        // The second tag is this socket's durable id (ADR-0019): tags survive
        // hibernation, so a wake can find the socket's `_sync_subs` rows.
        this.ctx.acceptWebSocket(server, [SYNC_TAG, SOCKET_ID_TAG_PREFIX + crypto.randomUUID()])
        this.#liveWs.add(server)

        return new Response(null, { status: 101, webSocket: client })
      }

      /** True iff `ws` is a tddc sync socket. On a bare DO tddc owns every socket
       *  (no host to share with), so all sockets are sync — which also keeps a
       *  legacy untagged 0.4.0 socket working after the mixin upgrade. Over any
       *  other base, only SYNC_TAG sockets are ours; the rest delegate to `super`. */
      #isSyncSocket(ws: WebSocket): boolean {
        return this.#isBareDO || this.ctx.getTags(ws).includes(SYNC_TAG)
      }

      /** The socket's durable id (ADR-0019), read back from its accept-time
       *  tag. Undefined for a legacy socket accepted by a pre-0.6 build that
       *  survived an in-place upgrade wake — such a socket's subscriptions
       *  were never persisted, so it degrades to pre-fix behavior (dead until
       *  reconnect) rather than failing. */
      #socketIdFor(ws: WebSocket): string | undefined {
        for (const tag of this.ctx.getTags(ws)) {
          if (tag.startsWith(SOCKET_ID_TAG_PREFIX)) return tag.slice(SOCKET_ID_TAG_PREFIX.length)
        }
        return undefined
      }

      override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
        if (!this.#isSyncSocket(ws)) {
          const base = super.webSocketMessage as
            | ((ws: WebSocket, m: string | ArrayBuffer) => void | Promise<void>)
            | undefined
          if (base) await base.call(this, ws, message)
          return
        }
        // "ping"/"pong" are handled by the auto-response and never arrive here.

        // Reject oversize frames before decode (ADR-0012): mirrors the
        // undecodable-frame stance — drop + log, no reply, no crash.
        //
        // No typed `rejected` here (ADR-0018): recovering the txId would mean
        // decoding the very payload this guard exists NOT to decode (the bound
        // is on memory/CPU, per ADR-0012). In production Cloudflare's edge caps
        // inbound WS messages at ~1 MiB anyway, so an oversize frame usually
        // never reaches this handler at all — the client transport's pre-send
        // guard (same limit, MutationRejectedError "FRAME_TOO_LARGE") is the
        // reliable rejection surface; this drop is defense in depth.
        const byteLen = typeof message === "string" ? message.length : message.byteLength
        if (byteLen > this.maxFrameBytes) {
          console.error(`oversize frame dropped (${byteLen} bytes > maxFrameBytes ${this.maxFrameBytes})`)
          return
        }

        let decoded: unknown
        try {
          decoded = this.#codec.decode(message)
        } catch {
          return // ignore undecodable frames
        }

        // Shape-guard after decode (ADR-0012): a frame that decodes but has the
        // wrong structure is dropped + logged. The guard runs BEFORE any SQL
        // binding so no arbitrary decoded value reaches lookupTx or sql.exec.
        if (!this.#wellFormed(decoded)) {
          // Safe stringify: decoded may contain bigints (MessagePack useBigInt64);
          // JSON.stringify throws on bigint — use a replacer to avoid crashing the
          // logging itself.
          let summary: string
          try {
            summary = JSON.stringify(decoded, (_k, v) => (typeof v === "bigint" ? String(v) : v))
          } catch {
            summary = String(decoded)
          }
          console.error("malformed frame dropped", summary)
          return
        }

        await this.#dispatch(ws, decoded)
      }

      override webSocketClose(ws: WebSocket, code?: number, reason?: string, wasClean?: boolean): void {
        if (!this.#isSyncSocket(ws)) {
          const base = super.webSocketClose as
            | ((ws: WebSocket, code: number, reason: string, wasClean: boolean) => void)
            | undefined
          if (base) base.call(this, ws, code ?? 1000, reason ?? "", wasClean ?? false)
          return
        }
        this.#dropSocketSubs(ws)
        this.#liveWs.delete(ws)
      }

      override webSocketError(ws: WebSocket, error?: unknown): void {
        if (!this.#isSyncSocket(ws)) {
          const base = super.webSocketError as ((ws: WebSocket, error: unknown) => void) | undefined
          if (base) base.call(this, ws, error)
          return
        }
        this.#dropSocketSubs(ws)
        this.#liveWs.delete(ws)
      }

      /** Drop a closed/errored socket's subscriptions, in memory AND durably
       *  (ADR-0019). The `#compiled` guard covers a DO that never called
       *  `registerSync` (no `initSchema`, no `_sync_subs`): it can have had no
       *  subscriptions, so there is nothing durable to delete. */
      #dropSocketSubs(ws: WebSocket): void {
        this.#subs.removeAll(ws)
        if (this.#compiled) {
          const sid = this.#socketIdFor(ws)
          if (sid) deletePersistedSocketSubs(this.#sql, sid)
        }
      }

      /** Shape-guard: returns true iff `v` is a structurally valid ClientFrame.
       *  (ADR-0012) Runs after decode, before any SQL binding.
       *
       *  Optional fields treat null == absent (the client transport serialises
       *  absent fields as null in MessagePack rather than omitting them). */
      #wellFormed(v: unknown): v is ClientFrame {
        if (v === null || typeof v !== "object") return false
        const f = v as Record<string, unknown>
        const t = f["t"]
        if (typeof t !== "string") return false

        const isNonEmptyString = (x: unknown): x is string => typeof x === "string" && x.length > 0
        /** null is treated as absent for optional fields */
        const absent = (x: unknown): boolean => x === undefined || x === null

        switch (t) {
          case "sub":
            return (
              isNonEmptyString(f["subId"]) &&
              isNonEmptyString(f["collection"]) &&
              (absent(f["since"]) || typeof f["since"] === "string") &&
              (absent(f["limit"]) || typeof f["limit"] === "number") &&
              (absent(f["offset"]) || typeof f["offset"] === "number")
            )
          case "unsub":
            return typeof f["subId"] === "string"
          case "mut": {
            if (!isNonEmptyString(f["txId"]) || !isNonEmptyString(f["collection"])) return false
            if (!Array.isArray(f["ops"]) || f["ops"].length === 0) return false
            const validOpTypes = new Set(["insert", "update", "delete"])
            for (const op of f["ops"] as Array<unknown>) {
              if (op === null || typeof op !== "object") return false
              const o = op as Record<string, unknown>
              if (!validOpTypes.has(o["type"] as string)) return false
              if (typeof o["key"] !== "string") return false
              if (!absent(o["cols"]) && (typeof o["cols"] !== "object" || Array.isArray(o["cols"]))) return false
            }
            return true
          }
          case "call":
            return isNonEmptyString(f["txId"]) && isNonEmptyString(f["name"])
          case "fetch":
            return (
              isNonEmptyString(f["fetchId"]) &&
              isNonEmptyString(f["collection"]) &&
              (absent(f["cursor"]) || typeof f["cursor"] === "object")
            )
          default:
            return false
        }
      }

      async #dispatch(ws: WebSocket, frame: ClientFrame): Promise<void> {
        switch (frame.t) {
          case "sub":
            return this.#handleSub(ws, frame)
          case "unsub": {
            this.#subs.remove(ws, frame.subId)
            if (this.#compiled) {
              const sid = this.#socketIdFor(ws)
              if (sid) deletePersistedSub(this.#sql, sid, frame.subId)
            }
            return
          }
          case "mut":
            return this.#handleMut(ws, frame)
          case "call":
            return this.#handleCall(ws, frame)
          case "fetch":
            return this.#handleFetch(ws, frame)
        }
      }

      /** One-shot paginated page fetch — a subset snapshot, NO live registration.
       *  Used by the client for cursor load-more; the window's live deltas already
       *  flow via the `sub` on the query's `where`.
       *
       *  The frame mirrors @tanstack/db's `LoadSubsetOptions` (ADR-0005): a base
       *  `where` plus a raw `cursor` (whereFrom/whereCurrent, which exclude the base). We compose
       *  `base AND whereCurrent` (ties, unbounded) and `base AND whereFrom` (next
       *  page, bounded by `limit`) as TWO SELECTs in ONE handler turn: synchronous
       *  SQLite, no `await` between them, so both observe the same database at one
       *  `seq`. The page therefore slots into the delta stream at a single position
       *  — a concurrent mutation is either reflected in it or arrives as a delta
       *  AFTER it, never split across the two reads (ADR-0003). */
      #handleFetch(ws: WebSocket, frame: Extract<ClientFrame, { t: "fetch" }>): void {
        const coll = this.#registry.collections.get(frame.collection)
        if (!coll) {
          this.#send(ws, { t: "page", fetchId: frame.fetchId, rows: [], seq: "0" })
          return
        }
        // A cursor must carry BOTH halves (TanStack's CursorExpressions always
        // does). A missing `whereCurrent` would otherwise compose to an empty
        // predicate and run the ties SELECT unbounded — a silent full-table scan,
        // which the operator floor exists to forbid. Reject loudly instead.
        if (frame.cursor != null && (frame.cursor.whereCurrent == null || frame.cursor.whereFrom == null)) {
          console.error(`fetch '${frame.fetchId}' on '${frame.collection}' rejected: malformed cursor`)
          this.#send(ws, { t: "page", fetchId: frame.fetchId, rows: [], seq: String(currentSeq(this.#sql)) })
          return
        }
        const seq = String(currentSeq(this.#sql))
        try {
          const rows: Array<unknown> = []
          // Cursor present: ties first (base AND whereCurrent, unbounded boundary
          // set), then the bounded next page (base AND whereFrom). The cursor
          // expressions arrive raw — excluding the base — so we compose them here.
          // No cursor: a plain bounded `where` read.
          if (frame.cursor != null) {
            const tq = compileSubsetQuery(frame.collection, {
              where: andPredicates(frame.where, frame.cursor.whereCurrent),
              orderBy: frame.orderBy,
            })
            rows.push(...Array.from(this.#sql.exec(tq.sql, ...tq.params)))
          }
          const nextWhere = frame.cursor != null ? andPredicates(frame.where, frame.cursor.whereFrom) : frame.where
          const nq = compileSubsetQuery(frame.collection, {
            where: nextWhere,
            orderBy: frame.orderBy,
            limit: frame.limit,
          })
          rows.push(...Array.from(this.#sql.exec(nq.sql, ...nq.params)))
          this.#send(ws, { t: "page", fetchId: frame.fetchId, rows, seq })
        } catch (e) {
          if (e instanceof UnsupportedPredicateError) {
            console.error(`fetch '${frame.fetchId}' on '${frame.collection}' rejected: ${e.message}`)
            this.#send(ws, { t: "page", fetchId: frame.fetchId, rows: [], seq })
            return
          }
          throw e
        }
      }

      /**
       * Apply a mutation atomically and confirm on the single ordered stream.
       *
       * Order is the load-bearing invariant (ADR-0002 C1): this connection's
       * matched deltas are flushed BEFORE its `committed` frame, so the client's
       * single cursor only ever advances over a contiguous applied prefix and the
       * optimistic overlay is never dropped before the authoritative row lands.
       */
      async #handleMut(ws: WebSocket, f: Extract<ClientFrame, { t: "mut" }>): Promise<void> {
        // Dedup first: a resent txId gets its stored outcome, never a fresh
        // rejection from a check below (ADR-0025).
        const seen = lookupTx(this.#sql, f.txId)
        if (seen) return this.#replayReceipt(ws, f.txId, seen)

        // Inbound limit: reject over-length batches without applying anything
        // (ADR-0012). Reject-don't-truncate: a partial apply silently drops writes.
        if (f.ops.length > this.maxOpsPerMutation) {
          return this.#rejectTx(ws, f.txId, `mutation exceeds maxOpsPerMutation (${this.maxOpsPerMutation})`, "LIMIT_EXCEEDED")
        }

        // An op key is the row's client-supplied TEXT pk; `""` is never a real
        // identity. Reject with a reply, not a shape-guard drop, so the client
        // rolls back at once instead of timing out (ADR-0025). After the dedup
        // lookup, so a resent txId still gets its stored outcome.
        if (f.ops.some((op) => op.key === "")) {
          return this.#rejectTx(ws, f.txId, "mutation op key must be a non-empty string", "VALIDATION")
        }

        const user = this.#userFor(ws)

        // Authorize every op BEFORE the transaction (may be async).
        try {
          for (const op of f.ops) {
            const def = this.#registry.mutations.get(`${f.collection}:${op.type}`)
            if (!def) throw new Error(`no mutation handler for '${f.collection}:${op.type}'`)
            if (def.authorize) await def.authorize({ user, op, sql: this.#sql, env: this.env })
          }
        } catch (e) {
          // authorize and validation errors surface to the client: a schema failure
          // carries a VALIDATION code, an authz "throw to deny" keeps its message. The
          // execute catch below stays sanitized.
          if (e instanceof ValidationError) return this.#rejectTx(ws, f.txId, e.message, "VALIDATION")
          return this.#rejectTx(ws, f.txId, errorMessage(e))
        }

        // Apply all ops in one synchronous transaction (atomic with the trigger
        // rows). A handler that returns a Promise is a programming error.
        let commitSeq: string
        try {
          this.ctx.storage.transactionSync(() => {
            for (const op of f.ops) {
              const def = this.#registry.mutations.get(`${f.collection}:${op.type}`)!
              const result = def.execute({ user, op, sql: this.#sql, env: this.env }) as unknown
              if (result !== undefined && typeof (result as PromiseLike<unknown>).then === "function") {
                // `execute` runs inside transactionSync, which cannot await; an async
                // execute also can't be atomic with its CDC rows. Do async work in
                // `authorize` (pre-tx), `afterCommit` (post-commit), or a command.
                throw new Error(
                  `mutation '${f.collection}:${op.type}' execute must be synchronous — do async work in authorize, afterCommit, or a command`,
                )
              }
            }
          })
          commitSeq = String(currentSeq(this.#sql))
        } catch (e) {
          // Log full detail server-side; send only a generic message to the client
          // (ADR-0012). SQLite constraint strings, column names, and programming-
          // error text are internal detail — not client API surface. The authorize
          // catch above is intentionally kept user-facing (README: "throw to deny").
          console.error(`mutation '${f.collection}' execute failed: ${errorMessage(e)}`)
          return this.#rejectTx(ws, f.txId, "mutation failed", "EXECUTE_FAILED")
        }

        recordTx(this.#sql, f.txId, true, commitSeq, null, null, null)
        // Enqueue deltas for all subscribers, then flush THIS socket before its
        // receipt (C1) so its deltas land first. Other subscribers flush on the
        // coalescer tick.
        this.#drainAndBroadcast()
        this.#broadcaster.flushOne(ws)
        this.#send(ws, { t: "committed", txId: f.txId, seq: commitSeq })

        // Fire-and-forget post-commit hooks AFTER the receipt — never on the
        // client's critical path. Each runs under `waitUntil` (keeps the DO alive
        // until it settles) and is isolated: a throw is logged and dropped, leaving
        // the committed mutation untouched. The hook owns its own idempotency
        // (ADR-0004); the library guarantees only "runs once per commit, off-path".
        for (const op of f.ops) {
          const after = this.#registry.mutations.get(`${f.collection}:${op.type}`)?.afterCommit
          if (!after) continue
          this.ctx.waitUntil(
            (async () => {
              try {
                await after({ user, op, sql: this.#sql, env: this.env })
              } catch (e) {
                console.error(`afterCommit '${f.collection}:${op.type}' failed: ${errorMessage(e)}`)
              }
            })(),
          )
        }
      }

      /** Run a named command (outside any transaction) and confirm with its result. */
      async #handleCall(ws: WebSocket, f: Extract<ClientFrame, { t: "call" }>): Promise<void> {
        const seen = lookupTx(this.#sql, f.txId)
        if (seen) return this.#replayReceipt(ws, f.txId, seen)

        const def = this.#registry.commands.get(f.name)
        if (!def) return this.#rejectTx(ws, f.txId, `unknown command '${f.name}'`, "UNKNOWN_COMMAND")

        const user = this.#userFor(ws)
        // authorize and validation errors surface like a mutation's: a schema failure
        // carries a VALIDATION code, an authz "throw to deny" keeps its message. This
        // matches mutation surfacing, revising ADR-0012 D3 (which sanitized a
        // command's authorize too).
        try {
          if (def.authorize) await def.authorize({ user, args: f.args, sql: this.#sql, env: this.env })
        } catch (e) {
          if (e instanceof ValidationError) return this.#rejectTx(ws, f.txId, e.message, "VALIDATION")
          return this.#rejectTx(ws, f.txId, errorMessage(e))
        }
        // execute runs arbitrary, often async code, so its errors are sanitized like a
        // mutation's execute — internal detail never leaks.
        let result: unknown
        try {
          result = await def.execute({ user, args: f.args, sql: this.#sql, env: this.env })
        } catch (e) {
          console.error(`command '${f.name}' execute failed: ${errorMessage(e)}`)
          return this.#rejectTx(ws, f.txId, "command failed", "EXECUTE_FAILED")
        }

        // Serialize the result for dedup replay BEFORE recording success. A
        // non-serializable result can't be replayed, so record an error rather
        // than risk re-running the command's side effects on retry.
        let stored: string | null
        try {
          stored = encodeResult(result)
        } catch (e) {
          return this.#rejectTx(ws, f.txId, `non-serializable command result: ${errorMessage(e)}`, "NON_SERIALIZABLE")
        }

        const commitSeq = String(currentSeq(this.#sql))
        recordTx(this.#sql, f.txId, true, commitSeq, null, null, stored)
        this.#drainAndBroadcast()
        this.#broadcaster.flushOne(ws)
        this.#send(ws, { t: "committed", txId: f.txId, seq: commitSeq, result })
      }

      #rejectTx(ws: WebSocket, txId: string, message: string, code?: string): void {
        recordTx(this.#sql, txId, false, null, message, code ?? null, null)
        this.#send(ws, { t: "rejected", txId, error: code ? { code, message } : { message } })
      }

      #replayReceipt(ws: WebSocket, txId: string, seen: SeenTx): void {
        if (seen.ok) {
          this.#send(ws, { t: "committed", txId, seq: seen.cursor ?? "0", result: decodeResult(seen.result) })
        } else {
          // Shape the replay exactly like #rejectTx's original frame — with the
          // persisted code when there was one (issue #21) — so a retrying client's
          // code-based handling sees the same outcome either way.
          const message = seen.error ?? "unknown"
          this.#send(ws, { t: "rejected", txId, error: seen.errorCode ? { code: seen.errorCode, message } : { message } })
        }
      }

      /**
       * Apply a SERVER-ORIGINATED write and broadcast it to connected clients
       * (ADR-0006). The home for writes outside the client mutation flow — an agent
       * inserting a row, a webhook, a cron/`alarm` job, an admin edit, a bulk seed.
       *
       * `fn` runs inside `transactionSync` (atomic, and the same synchronous
       * constraint mutations live under) and may return a value; its CDC is then
       * drained and broadcast on the next coalescer tick. A thenable return is
       * rejected (and rolls back): any async work belongs BEFORE the call.
       */
      #runSyncedWrite<T>(fn: (sql: SqlStorage) => T): T {
        let result: T
        this.ctx.storage.transactionSync(() => {
          result = fn(this.#sql)
          if (result != null && typeof (result as unknown as PromiseLike<unknown>).then === "function") {
            throw new Error("runSyncedWrite fn must be synchronous (it returned a thenable)")
          }
        })
        this.#drainAndBroadcast()
        return result!
      }

      /**
       * Drain `_sync_changes` from the last broadcast watermark, fan out one `d`
       * per changed key to each subscriber of the affected collection, then a
       * single `uptodate` boundary per touched socket. Multiple changes to a key
       * within the drain collapse to the latest op.
       */
      #drainAndBroadcast(): void {
        const sql = this.#sql
        const last = getDrainCursor(sql)
        const changes = readChangesSince(sql, last)
        if (changes.length === 0) return
        const cursor = String(changes[changes.length - 1]!.seq)

        const byTable = new Map<string, Array<(typeof changes)[number]>>()
        for (const c of changes) {
          let arr = byTable.get(c.tbl)
          if (!arr) {
            arr = []
            byTable.set(c.tbl, arr)
          }
          arr.push(c)
        }

        for (const [tbl, tableChanges] of byTable) {
          const coll = this.#registry.collections.get(tbl)
          if (!coll) continue
          const latest = new Map<string, (typeof tableChanges)[number]>()
          for (const c of tableChanges) latest.set(c.key, c)
          const liveKeys = [...latest.values()].filter((c) => c.op !== "delete").map((c) => c.key)
          const hydrated = hydrateRows(sql, tbl, coll.pk, liveKeys)

          // Enqueue into the coalescer; it flushes one `d` per surviving key plus a
          // single `uptodate` boundary per socket (on the tick, or via flushOne).
          for (const { ws, sub } of this.#subs.forCollection(tbl)) {
            for (const [key, change] of latest) {
              const row = hydrated.get(key)
              // Always-emit rule (no before-image, ADR-0002 C4): a key that is
              // deleted, gone, or no longer matches this sub's predicate -> a
              // synthetic delete (idempotent; move-out). A matching live row ->
              // its current state with the actual op (move-in via update upserts
              // on the client — verified). Predicate is always-true when unfiltered.
              if (change.op === "delete" || !row || !sub.predicate(row)) {
                this.#broadcaster.enqueue(ws, { subId: sub.subId, key, op: "delete" }, cursor)
              } else {
                // Full row as the partial patch; column-level diffs arrive later.
                this.#broadcaster.enqueue(ws, { subId: sub.subId, key, op: change.op, cols: row }, cursor)
              }
            }
          }
        }

        setDrainCursor(sql, changes[changes.length - 1]!.seq)
        this.#maybeCompact()
      }

      /**
       * Opportunistic GC: every `compactionEvery` drained mutations, collapse the
       * change log to latest-op-per-key and sweep expired dedup entries. Deferred
       * via `ctx.waitUntil` so it rides just after a burst of work — it never
       * blocks a mutation's response, and (unlike an alarm) never wakes an idle DO.
       */
      #maybeCompact(): void {
        if (++this.#writesSinceCompaction < this.compactionEvery) return
        this.#writesSinceCompaction = 0
        this.ctx.waitUntil(
          (async (): Promise<void> => {
            compactChanges(this.#sql)
            pruneChanges(this.#sql, this.changelogRetentionMs, Date.now())
            sweepDedup(this.#sql, this.dedupRetentionMs, Date.now())
            // Orphaned subscription rows (socket died without webSocketClose —
            // hard termination): hygiene rides compaction, nothing polls
            // (ADR-0019 D4). Every id-tagged socket carries SYNC_TAG, so the
            // tagged query is the complete live set.
            const liveIds = new Set<string>()
            for (const ws of this.ctx.getWebSockets(SYNC_TAG)) {
              const sid = this.#socketIdFor(ws)
              if (sid) liveIds.add(sid)
            }
            sweepOrphanSubs(this.#sql, liveIds)
          })(),
        )
      }

      /** Full-collection subscribe: emit every current row as a snapshot, then a
       *  boundary. */
      #handleSub(ws: WebSocket, frame: Extract<ClientFrame, { t: "sub" }>): void {
        const coll = this.#registry.collections.get(frame.collection)
        if (!coll) {
          // Unknown collection: drop the subscriber's view. Richer sub-error
          // signalling is deferred; for now reset is the honest minimum.
          this.#send(ws, { t: "reset", sub: frame.subId })
          return
        }

        // Per-socket subscription cap (ADR-0012). A re-sub on an existing subId
        // replaces the old entry (SubscriptionRegistry.add semantics) — count
        // only new subIds against the cap.
        const existingCount = this.#subs.countFor(ws)
        const existingSub = this.#subs.forWs(ws).find((s) => s.subId === frame.subId)
        if (!existingSub && existingCount >= this.maxSubsPerSocket) {
          console.error(`sub '${frame.subId}' refused: maxSubsPerSocket (${this.maxSubsPerSocket}) reached`)
          this.#send(ws, { t: "reset", sub: frame.subId })
          return
        }
        // Lower where/orderBy/limit/offset into SQLite. An un-lowerable predicate
        // (outside the supported floor) is rejected, not silently full-scanned.
        let query: { sql: string; params: Array<unknown> }
        try {
          query = compileSubsetQuery(frame.collection, {
            where: frame.where,
            orderBy: frame.orderBy,
            limit: frame.limit,
            offset: frame.offset,
          })
        } catch (e) {
          if (e instanceof UnsupportedPredicateError) {
            console.error(`sub '${frame.subId}' on '${frame.collection}' rejected: ${e.message}`)
            this.#send(ws, { t: "reset", sub: frame.subId })
            return
          }
          throw e
        }

        // C1′ (ADR-0011, generalizing ADR-0002 C1): what follows is a synchronous
        // cursor-advancing emission — a snapshot's `snap-end` or a catch-up's
        // `uptodate` carries the CURRENT seq, which may include changes whose
        // deltas are still buffered in the coalescer for this socket. Flush them
        // first, or the client's cursor claims a seq it never applied and a drop
        // before the tick loses the write (reconnect resumes past it).
        this.#broadcaster.flushOne(ws)

        // Registering compiles the predicate in @tanstack/db's evaluator. If the
        // predicate is outside the JS floor (e.g. an operator the SQL floor somehow
        // let through), that throws UnsupportedPredicateError — reject with `reset`
        // rather than letting it escape uncaught and hang the client (ADR-0013).
        let sub: Sub
        try {
          sub = this.#subs.add(ws, frame.subId, frame.collection, frame.where)
        } catch (e) {
          if (e instanceof UnsupportedPredicateError) {
            console.error(`sub '${frame.subId}' on '${frame.collection}' rejected: ${e.message}`)
            this.#send(ws, { t: "reset", sub: frame.subId })
            return
          }
          throw e
        }
        // Write through (ADR-0019): the accepted sub becomes durable, so a
        // hibernation wake can restore it onto the surviving socket. Only
        // after `add` succeeds — a rejected predicate must not persist.
        {
          const sid = this.#socketIdFor(ws)
          if (sid) persistSub(this.#sql, sid, frame.subId, frame.collection, frame.where)
        }
        const seq = String(currentSeq(this.#sql))

        // Reconnect catch-up: a `since` cursor asks for changes after that point
        // rather than a fresh snapshot. Serve a windowed delta while the change log
        // still reaches back that far; otherwise fall back to reset + snapshot.
        //
        // The floor is `minChangeSeq` — no persisted watermark needed (ADR-0009).
        const since = frame.since != null ? Number(frame.since) : 0
        if (since > 0) {
          const floor = minChangeSeq(this.#sql)
          if (floor !== 0 && since >= floor - 1) {
            this.#emitCatchUp(ws, sub, coll, since, seq)
            return
          }
          this.#send(ws, { t: "reset", sub: frame.subId })
        }

        const rows = Array.from(this.#sql.exec(query.sql, ...query.params)) as Array<Record<string, SqlStorageValue>>
        for (const row of rows) {
          this.#send(ws, { t: "snap", sub: frame.subId, key: row[coll.pk], row, seq })
        }
        this.#send(ws, { t: "snap-end", sub: frame.subId, seq })
      }

      /** Windowed catch-up: the latest op per changed key since `since`, resolved
       *  through the sub's predicate (move-in/out), then an `uptodate` boundary. */
      #emitCatchUp(
        ws: WebSocket,
        sub: { subId: string; predicate: (row: Record<string, unknown>) => boolean },
        coll: { table: string; pk: string },
        since: number,
        seq: string,
      ): void {
        const changes = readChangesSinceFor(this.#sql, coll.table, since)
        const latest = new Map<string, (typeof changes)[number]>()
        for (const c of changes) latest.set(c.key, c)
        const liveKeys = [...latest.values()].filter((c) => c.op !== "delete").map((c) => c.key)
        const hydrated = hydrateRows(this.#sql, coll.table, coll.pk, liveKeys)
        for (const [key, change] of latest) {
          const row = hydrated.get(key)
          if (change.op === "delete" || !row || !sub.predicate(row)) {
            this.#send(ws, { t: "d", sub: sub.subId, key, op: "delete", seq })
          } else {
            this.#send(ws, { t: "d", sub: sub.subId, key, op: change.op, cols: row, seq })
          }
        }
        // Sub-scoped terminal: this catch-up is one subscription's bootstrap, not
        // a socket-wide boundary (ADR-0011 D3). Still advances the client cursor.
        this.#send(ws, { t: "uptodate", seq, sub: sub.subId })
      }

      /** Encode and send a server frame on one socket. Warns — and still sends
       *  whole — when the encoded frame exceeds `WARN_OUTBOUND_FRAME_BYTES`
       *  (ADR-0018): observability for hydrated full-row re-sends, whose real
       *  fix is column projection (issue #28). */
      #send(ws: WebSocket, frame: ServerFrame): void {
        const encoded = this.#codec.encode(frame)
        const bytes = typeof encoded === "string" ? encoded.length : encoded.byteLength
        if (bytes > WARN_OUTBOUND_FRAME_BYTES) {
          // Resolve the sub's collection only on the warn path (linear scan
          // over this socket's subs — never on the hot path).
          const subId = (frame as { sub?: string }).sub
          const collection = subId ? this.#subs.forWs(ws).find((s) => s.subId === subId)?.collection : undefined
          const where = collection ? ` for collection '${collection}'` : subId ? ` for sub '${subId}'` : ""
          console.warn(
            `oversize outbound '${frame.t}' frame (${bytes} bytes > ${WARN_OUTBOUND_FRAME_BYTES})${where} — ` +
              `full-row rebroadcast; column projection (issue #28) is the real fix`,
          )
        }
        // A socket can transition to CLOSING/CLOSED between when we decided to
        // send and now — a client disposing, navigating away, or a forced
        // reconnect dropping mid-snapshot (issue #40). `webSocketClose` already
        // tears down this socket's subs (#dropSocketSubs), so a frame it can no
        // longer read is a benign no-op, NOT an error to surface as an uncaught
        // throw. Pre-check readyState; the try/catch covers the OPEN→closed race
        // the check can't (state can change under us between check and send).
        if (ws.readyState !== WebSocket.OPEN) return
        try {
          ws.send(encoded)
        } catch {
          // "Can't call send() after close()" — the close beat us here. See above.
        }
      }

      /** The attachment bound at upgrade, surviving hibernation. */
      #userFor(ws: WebSocket): TUser {
        return ws.deserializeAttachment() as TUser
      }
    }

    // Explicit, NAMEABLE return type — required for declaration emit (a raw
    // `class extends Base` would emit an anonymous class type whose inherited
    // protected `ctx`/`env` trip TS4094). A single `(...args: any[])` construct
    // signature (so a mixed base's own constructor params don't leak into a
    // generic subclass's `super(...)`) yielding the host instance intersected
    // with the sync surface. The protected tuning knobs stay protected fields at
    // runtime; `SyncDurableObject` re-declares them for override typing.
    return SyncableMixin as unknown as abstract new (
      ...args: Array<unknown>
    ) => InstanceType<TBase> & SyncMixin<Env, TUser>
  }
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
