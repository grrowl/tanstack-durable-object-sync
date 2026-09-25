// Test worker entry — declares the DO classes bound by vitest.config's
// miniflare.durableObjects, and routes WebSocket upgrades to the sync DO.

import { DurableObject } from "cloudflare:workers"
import { Syncable } from "../src/server/mixin.ts"
import { defineSync, type StandardSchemaV1 } from "../src/server/registry.ts"
import { SyncDurableObject } from "../src/server/sync-do.ts"

/** Bare DO for the M1 CDC tests; they drive storage via runInDurableObject. */
export class TestDO extends DurableObject {}

interface Claims {
  userId: string
}

/** Sync DO exercised by the WS lifecycle (M2) and read-path (M3) tests. The
 *  framework owns sub/mut/call dispatch; the subclass only declares its
 *  collections (mutations/commands arrive with the write-path increment). */
interface MsgRow {
  id: string
  body: string
}

interface FileRow {
  id: string
  name: string
}

interface ValidatedRow {
  id: string
  body: string
}

/** A hand-rolled Standard Schema (no validator dependency): rejects when `body`
 *  is present but not a non-empty string. Used as both the insert (full-row) and
 *  update (partial) schema for the `validated` collection, to exercise the
 *  runtime validation gate. */
function nonEmptyBody<T extends { body?: unknown }>(): StandardSchemaV1<T> {
  return {
    "~standard": {
      version: 1,
      vendor: "test",
      validate: (value) => {
        const v = value as { body?: unknown }
        if ("body" in v && (typeof v.body !== "string" || v.body.trim() === "")) {
          return { issues: [{ message: "body must be a non-empty string" }] }
        }
        return { value: value as T }
      },
    },
  }
}

/** A TRANSFORMING Standard Schema — its `validate` returns a MUTATED value
 *  (uppercased `body`). The gate must DISCARD that output and hand the handler the
 *  ORIGINAL wire value (ADR-0014 gate-not-parser); if the transform ever leaked
 *  through, the stored/broadcast row would diverge from the row the client applied
 *  optimistically (ADR-0001 D9). Used by the `transformed` collection. */
function upcasingBody<T extends { body?: unknown }>(): StandardSchemaV1<T> {
  return {
    "~standard": {
      version: 1,
      vendor: "test",
      validate: (value) => {
        const v = value as { body?: unknown }
        return { value: { ...(v as object), body: typeof v.body === "string" ? v.body.toUpperCase() : v.body } as T }
      },
    },
  }
}

const sync = defineSync<Claims>()

// The same collections/mutations/commands as before, authored via the
// object-schema API. The schema VALUE is registered on the DO below. Exported so
// the host-matrix test can re-register to drive the trigger reaper (ADR-0008).
export const testSchema = sync.schema({
  collections: {
    messages: sync.collection<MsgRow>({
      pk: "id",
      mutations: {
        insert: {
          authorize: ({ op }) => {
            if (op.cols.body === "FORBIDDEN") throw new Error("forbidden body")
          },
          execute: ({ op, sql }) => {
            sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", op.cols.id, op.cols.body)
          },
        },
        update: {
          execute: ({ op, sql }) => {
            sql.exec("UPDATE messages SET body = ? WHERE id = ?", op.cols.body, op.key)
          },
        },
        delete: {
          execute: ({ op, sql }) => {
            sql.exec("DELETE FROM messages WHERE id = ?", op.key)
          },
        },
      },
    }),
    // A second collection on the same DO — exercises multiplexing over one WS.
    files: sync.collection<FileRow>({
      pk: "id",
      mutations: {
        insert: {
          execute: ({ op, sql }) => {
            sql.exec("INSERT INTO files(id, name) VALUES (?, ?)", op.cols.id, op.cols.name)
          },
        },
        // `files:delete` exercises afterCommit: the synchronous execute is the
        // durable write; afterCommit is fire-and-forget async post-work (here it
        // records a marker proving both `sql` and `env` reached the hook).
        delete: {
          execute: ({ op, sql }) => {
            sql.exec("DELETE FROM files WHERE id = ?", op.key)
          },
          afterCommit: async ({ op, sql, env }) => {
            // A genuine async hop, to prove afterCommit awaits and runs off the
            // request path. `_afterlog` is a plain side table (no CDC triggers).
            await Promise.resolve()
            if (op.key === "boom") throw new Error("afterCommit boom")
            sql.exec("CREATE TABLE IF NOT EXISTS _afterlog (key TEXT PRIMARY KEY, tag TEXT)")
            const hasEnv = (env as { SYNC_DO?: unknown }).SYNC_DO ? "has-env" : "no-env"
            sql.exec("INSERT OR REPLACE INTO _afterlog(key, tag) VALUES (?, ?)", op.key, hasEnv)
          },
        },
      },
    }),
    // Carries Standard Schemas so the runtime validation gate is exercised: the
    // insert schema validates the full row, the update schema the partial patch.
    validated: sync.collection<ValidatedRow>({
      pk: "id",
      mutations: {
        insert: {
          schema: nonEmptyBody<ValidatedRow>(),
          execute: ({ op, sql }) => {
            sql.exec("INSERT INTO validated(id, body) VALUES (?, ?)", op.cols.id, op.cols.body)
          },
        },
        update: {
          schema: nonEmptyBody<Partial<ValidatedRow>>(),
          execute: ({ op, sql }) => {
            sql.exec("UPDATE validated SET body = ? WHERE id = ?", op.cols.body, op.key)
          },
        },
      },
    }),
    // Its insert schema TRANSFORMS (uppercases `body`), but the gate discards that
    // output and the handler stores the RAW `op.cols.body` — a subscriber sees the
    // wire value, not the transform (ADR-0014 gate-not-parser; ADR-0001 D9).
    transformed: sync.collection<ValidatedRow>({
      pk: "id",
      mutations: {
        insert: {
          schema: upcasingBody<ValidatedRow>(),
          execute: ({ op, sql }) => {
            sql.exec("INSERT INTO transformed(id, body) VALUES (?, ?)", op.cols.id, op.cols.body)
          },
        },
      },
    }),
  },
  commands: {
    echo: sync.command<unknown>()(({ args }) => ({ echoed: args })),
    // A SQL-WRITING command. Unlike `echo` (pure RPC), this mutates rows:
    // its `DELETE` fires the same CDC triggers a mutation would, so the
    // removed rows broadcast to other subscribers as ordinary `delete`
    // deltas — AND it returns a result (the count) on `committed`. That
    // pairing (bulk write + result) is exactly what a typed
    // insert/update/delete mutation can't express, and why commands are
    // the escape hatch for non-CRUD operations.
    clearMessages: sync.command()(({ sql }) => {
      const before = Array.from(sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
      sql.exec("DELETE FROM messages")
      return { deleted: before }
    }),
    boom: sync.command()(() => {
      throw new Error("command boom")
    }),
    // Carries an args Standard Schema — exercises command-args validation. As for
    // a mutation, a validation failure surfaces to the client with its reason and a
    // VALIDATION code (uniform surfacing; ADR-0014 revises ADR-0012 D3).
    requireBody: sync.command(nonEmptyBody<{ body: string }>(), ({ args }) => ({ echoed: args.body })),
    // A command whose `authorize` THROWS — its reason must surface to the client
    // (ADR-0014 unifies command + mutation authorize surfacing; ADR-0012 D3 had
    // sanitized a command's authorize to a generic message). An `execute` throw
    // stays sanitized — that's `boom`.
    denyCall: sync.command()({
      authorize: () => {
        throw new Error("call denied by authorize")
      },
      execute: () => ({ ok: true }),
    }),
  },
})

/** The schema's type, for client-side tests to type their transport + collections
 *  (`new WebSocketTransport<TestApi>(...)`, `doCollectionOptions<TestApi, "messages">`). */
export type TestApi = typeof testSchema

export class SyncTestDO extends SyncDurableObject<unknown, Claims> {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      // The author owns table creation; the framework wires sync after (ADR-0007).
      this.sql.exec(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`)
      this.sql.exec(`CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, name TEXT)`)
      this.sql.exec(`CREATE TABLE IF NOT EXISTS validated (id TEXT PRIMARY KEY, body TEXT)`)
      this.sql.exec(`CREATE TABLE IF NOT EXISTS transformed (id TEXT PRIMARY KEY, body TEXT)`)
      this.registerSync(testSchema)
    })
  }

  protected override parseAttachment(req: Request): Claims {
    const userId = req.headers.get("x-user") ?? "anon"
    // Sentinel for the auth-gate tests: parseAttachment is the ONE gate for both
    // the WS upgrade and the SSR readSyncSnapshot read (ADR-0011 D1) — a rejecting
    // identity must reject both paths, so a tenant check can't be bypassed by SSR.
    if (userId === "forbidden") throw new Response("forbidden", { status: 403 })
    return { userId }
  }
}

/** A sync DO that never calls registerSync — proves accessing sync before
 *  registration fails loud (ADR-0007). */
export class UnregisteredDO extends SyncDurableObject<unknown, Claims> {}

/** Same collections as SyncTestDO, but with a tiny compaction threshold so the
 *  opportunistic `maybeCompact` housekeeping (compaction + retention prune +
 *  dedup sweep) fires after a few writes instead of 200 — lets tests exercise
 *  the real drain → maybeCompact → waitUntil path. Keeps the default 2-day
 *  `changelogRetentionMs` so the retention wiring is tested as shipped. */
export class MaintTestDO extends SyncTestDO {
  protected override readonly compactionEvery = 3
}

/** Same collections as SyncTestDO with an effectively-infinite coalescer tick:
 *  enqueued deltas stay buffered until something calls flushOne explicitly.
 *  Lets the cursor-barrier tests (ADR-0011 C1′) hold "pending egress" still
 *  while a snapshot/catch-up is served on the same socket. */
export class SlowTickDO extends SyncTestDO {
  protected override readonly tickMs = 30_000
}

/** Same collections as SyncTestDO with tiny inbound limits — lets wire-hardening
 *  tests (ADR-0012) exercise the limit paths without sending 128+ ops or opening
 *  256+ subscriptions. */
export class LimitsTestDO extends SyncTestDO {
  protected override readonly maxOpsPerMutation = 2
  protected override readonly maxSubsPerSocket = 2
}

// ---- In-flight duplicate fixtures (inflight-txid.test.ts) -----------------
//
// An `authorize` that awaits a real non-storage promise, released by a LATER
// event (the test's runInDurableObject). While it is parked the input gate is
// open, so a second frame for the same txId can be dispatched: the window an
// I/O-bound authorize opens in production (ADR-0025 amendment, in-flight).

/** Gates keyed by a tag the test chooses; module-scoped because `authorize`
 *  has no instance. `parked` counts authorize calls that reached the gate. */
const gates = new Map<string, { parked: number; open: boolean; waiters: Array<() => void> }>()

function gateFor(tag: string) {
  let g = gates.get(tag)
  if (!g) gates.set(tag, (g = { parked: 0, open: false, waiters: [] }))
  return g
}

async function parkAt(tag: string): Promise<void> {
  const g = gateFor(tag)
  g.parked++
  if (!g.open) await new Promise<void>((r) => g.waiters.push(r))
}

export const gatedSchema = sync.schema({
  collections: {
    gated: sync.collection<MsgRow>({
      pk: "id",
      mutations: {
        insert: {
          authorize: ({ op }) => parkAt(op.cols.body),
          execute: ({ op, sql }) => {
            sql.exec("INSERT INTO gated(id, body) VALUES (?, ?)", op.cols.id, op.cols.body)
          },
        },
      },
    }),
  },
  commands: {
    // A side effect that must run once per txId: bumps a counter, returns it.
    bump: sync.command<{ tag: string }>()({
      authorize: ({ args }) => parkAt(args.tag),
      execute: ({ sql }) => {
        sql.exec("UPDATE bumps SET n = n + 1")
        return { n: Array.from(sql.exec("SELECT n FROM bumps"))[0]!.n as number }
      },
    }),
    echo: sync.command<unknown>()(({ args }) => ({ echoed: args })),
  },
})

export type GatedApi = typeof gatedSchema

export class GatedTestDO extends SyncDurableObject<unknown, Claims> {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec(`CREATE TABLE IF NOT EXISTS gated (id TEXT PRIMARY KEY, body TEXT)`)
      this.sql.exec(`CREATE TABLE IF NOT EXISTS bumps (n INTEGER NOT NULL)`)
      if (Array.from(this.sql.exec("SELECT n FROM bumps")).length === 0) this.sql.exec("INSERT INTO bumps(n) VALUES (0)")
      this.registerSync(gatedSchema)
    })
  }

  protected override parseAttachment(): Claims {
    return { userId: "anon" }
  }

  /** How many authorize calls have reached the gate `tag`. */
  gateParked(tag: string): number {
    return gateFor(tag).parked
  }

  /** Open the gate `tag`: release every parked authorize, and let later ones through. */
  gateRelease(tag: string): void {
    const g = gateFor(tag)
    g.open = true
    for (const w of g.waiters.splice(0)) w()
  }
}

// ---- Host-matrix fixtures (host-matrix.test.ts) ---------------------------
//
// A fake partyserver-like host base and the Syncable mixin applied over it. The
// fake mimics partyserver's OBSERVABLE contract without pulling the ~13 MB agents
// dependency (ADR-0015 test plan): it owns sockets stamped with a `__pk`
// attachment, filters foreign sockets in every hibernation handler, exposes a
// `sql` tagged-template method (the member the mixin must NOT shadow), and
// upgrades on its own `/_host` path — everything else falls through.

const HOST_TAG = "__host"

export class FakeHost extends DurableObject {
  /** Host-delivered frames, for cross-delivery assertions. */
  hostInbox: Array<string> = []
  /** Count of host-owned socket closes seen by the host handler. */
  hostClosed = 0

  /** partyserver-style tagged-template query (partyserver dist/index.js:557).
   *  The mixin must never define a `sql` property that shadows this. */
  sql<T = Record<string, unknown>>(strings: TemplateStringsArray, ...values: Array<unknown>): Array<T> {
    let query = ""
    strings.forEach((s, i) => {
      query += s + (i < values.length ? "?" : "")
    })
    return Array.from(this.ctx.storage.sql.exec(query, ...values)) as Array<T>
  }

  /** partyserver's `__pk` discriminator (dist/index.js:14 to 35): a socket is the
   *  host's iff its attachment carries a `__pk` key. */
  #isHostSocket(ws: WebSocket): boolean {
    const a = ws.deserializeAttachment() as { __pk?: unknown } | null
    return a != null && typeof a === "object" && "__pk" in a
  }

  override async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url)
    if (req.headers.get("Upgrade") === "websocket" && url.pathname.endsWith("/_host")) {
      const pair = new WebSocketPair()
      pair[1].serializeAttachment({ __pk: crypto.randomUUID() })
      this.ctx.acceptWebSocket(pair[1], [HOST_TAG])
      return new Response(null, { status: 101, webSocket: pair[0] })
    }
    return new Response("host: not found", { status: 404 })
  }

  override webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    if (!this.#isHostSocket(ws)) return // partyserver short-circuits foreign sockets
    this.hostInbox.push(typeof message === "string" ? message : "<binary>")
  }

  override webSocketClose(ws: WebSocket): void {
    if (!this.#isHostSocket(ws)) return
    this.hostClosed++
  }

  override webSocketError(_ws: WebSocket): void {}

  /** partyserver-style broadcast: touches only the host's own sockets. */
  hostBroadcast(msg: string): void {
    for (const ws of this.ctx.getWebSockets(HOST_TAG)) ws.send(msg)
  }
}

/** The mixin over the fake host. Defaults: auto-response OFF, pragma OFF (base is
 *  not DurableObject). Registers the same collections as SyncTestDO, plus a
 *  host-owned `cf_agents_state` table that is deliberately NOT registered. */
export class SyncOverHostDO extends Syncable<unknown, Claims>()(FakeHost) {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    // Author-owned auth hook via the facade (the mixed base has no legacy
    // `parseAttachment` override).
    this.sync.configure({ parseAttachment: (req) => ({ userId: req.headers.get("x-user") ?? "anon" }) })
    ctx.blockConcurrencyWhile(async () => {
      // NOTE: reach `ctx.storage.sql` directly — `this.sql` here is the HOST's
      // tagged-template method (ADR-0015), the whole point of dropping the getter.
      const sql = ctx.storage.sql
      sql.exec(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, name TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS validated (id TEXT PRIMARY KEY, body TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS transformed (id TEXT PRIMARY KEY, body TEXT)`)
      // A host-owned table the author never registers (cf. cf_agents_state).
      sql.exec(`CREATE TABLE IF NOT EXISTS cf_agents_state (key TEXT PRIMARY KEY, value TEXT)`)
      this.sync.registerSync(testSchema)
    })
  }
}

/** Same base, but opts the two DO-global side effects ON — proves `configure`
 *  restores 0.4.0-style auto-response + case-sensitive LIKE over a non-DO base. */
export class SyncOverHostOptInDO extends Syncable<unknown, Claims>()(FakeHost) {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    this.sync.configure({
      autoResponse: true,
      caseSensitiveLike: true,
      parseAttachment: (req) => ({ userId: req.headers.get("x-user") ?? "anon" }),
    })
    ctx.blockConcurrencyWhile(async () => {
      const sql = ctx.storage.sql
      sql.exec(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, name TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS validated (id TEXT PRIMARY KEY, body TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS transformed (id TEXT PRIMARY KEY, body TEXT)`)
      this.sync.registerSync(testSchema)
    })
  }
}

interface Env {
  TEST_DO: DurableObjectNamespace
  SYNC_DO: DurableObjectNamespace
  MAINT_DO: DurableObjectNamespace
  SLOW_DO: DurableObjectNamespace
  LIMITS_DO: DurableObjectNamespace
  GATED_DO: DurableObjectNamespace
  HOST_DO: DurableObjectNamespace
  HOST_OPTIN_DO: DurableObjectNamespace
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)
    if (url.pathname.startsWith("/sync/")) {
      const name = url.pathname.slice("/sync/".length) || "default"
      return env.SYNC_DO.get(env.SYNC_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/maint/")) {
      const name = url.pathname.slice("/maint/".length) || "default"
      return env.MAINT_DO.get(env.MAINT_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/slow/")) {
      const name = url.pathname.slice("/slow/".length) || "default"
      return env.SLOW_DO.get(env.SLOW_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/limits/")) {
      const name = url.pathname.slice("/limits/".length) || "default"
      return env.LIMITS_DO.get(env.LIMITS_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/gated/")) {
      const name = url.pathname.slice("/gated/".length) || "default"
      return env.GATED_DO.get(env.GATED_DO.idFromName(name)).fetch(req)
    }
    // Host-matrix routes: /host/<name>/... and /host-optin/<name>/... forward the
    // WHOLE request so the DO sees the trailing /_sync or /_host discriminator.
    if (url.pathname.startsWith("/host/")) {
      const name = url.pathname.slice("/host/".length).split("/")[0] || "default"
      return env.HOST_DO.get(env.HOST_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/host-optin/")) {
      const name = url.pathname.slice("/host-optin/".length).split("/")[0] || "default"
      return env.HOST_OPTIN_DO.get(env.HOST_OPTIN_DO.idFromName(name)).fetch(req)
    }
    return new Response("test-worker")
  },
}
