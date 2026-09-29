import { createCollection, createLiveQueryCollection, ilike } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

// bugbash F5 (design gap; the fix is a wire ADR, not written yet). WHY: every
// refusal cause (unknown collection, malformed cursor, unsupported predicate,
// sub cap) answers with the frame a genuinely EMPTY result gets — `page
// rows:[]` for a fetch, `reset` with no snapshot for a sub — so the app sees a
// successful empty load and a typo'd table or an off-floor query renders as
// "no rows". The only trace is a server-side console.error.
//
// These are the scenarios a "refusals fail loud" design must satisfy, one per
// cause and surface, each `it.fails` until that design lands. They assert only
// what any such design must deliver — a correlated reply that is neither the
// empty result's nor the below-floor `reset`; a load that rejects — not a frame
// name or error type; the ADR picks those, and its fix should tighten these to
// them. The passing tests pin what holds before AND after: an empty result
// stays a success, and every refusal settles promptly (so a hang cannot hide
// behind a `.fails`).
//
// Existing tests that pin "refusal = empty" as correct today; a fix must
// supersede them deliberately (not changed here):
//   - sync-read.test.ts        "subscribing to an unknown collection fails safe with reset"
//   - wire-hardening.test.ts   "subscription cap: third sub on 2-cap DO → reset; …"
//   - on-demand.test.ts        "resolves loadSubset on a rejected sub (reset, no snap-end) instead of hanging"
//   - on-demand.test.ts        "rejects a malformed cursor (missing a half) instead of scanning the table" (expects [])
//   - on-demand-contracts.test.ts "a request covered by a pending watch that is then refused opens its own" (fake reset)
//   - on-demand-contracts.test.ts "an unsupported predicate beside a valid subset: …" swallows bad.preload()'s outcome;
//     its containment assertions should survive a fix.
// Precedent: the SSR read path already fails loud on an unknown collection
// (read-sync-snapshot.test.ts "throws on an unknown collection (fail loud, not empty-success)").

const codec = createFrameCodec()
const ref = (col: string) => ({ type: "ref", path: [col] })
const val = (value: unknown) => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>) => ({ type: "func", name, args })
const ILIKE = fn("ilike", ref("body"), val("%b%")) // outside the SQL predicate floor (ADR-0013)
const HALF_CURSOR = { whereFrom: fn("gt", ref("body"), val("")) } // whereCurrent missing (ADR-0005)

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket!
  ws.accept()
  return ws
}

type Correlated = ServerFrame & { fetchId?: string; sub?: string; subId?: string }

/** Send `f`, resolve with the first frame correlated to it that is not a row
 *  (`snap`/`d`). Never rejects: a missing reply resolves `undefined`. */
function reply(ws: WebSocket, f: ClientFrame, id: string, timeoutMs = 2000): Promise<Correlated | undefined> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      ws.removeEventListener("message", onMsg)
      resolve(undefined)
    }, timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      const x = codec.decode(e.data as ArrayBuffer) as Correlated
      if (x.t === "snap" || x.t === "d") return
      if (x.fetchId === id || x.sub === id || x.subId === id) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(x)
      }
    }
    ws.addEventListener("message", onMsg)
    ws.send(codec.encode(f))
  })
}

async function seed(ns: DurableObjectNamespace, room: string): Promise<void> {
  await runInDurableObject(ns.get(ns.idFromName(room)), (inst, s) => {
    for (let i = 1; i <= 3; i++) s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", `m${i}`, `b${i}`)
    ;(inst as unknown as { sync: { drainAndBroadcast(): void } }).sync.drainAndBroadcast()
  })
}

type FetchBody = Omit<Extract<ClientFrame, { t: "fetch" }>, "t" | "fetchId">
type SubBody = Omit<Extract<ClientFrame, { t: "sub" }>, "t" | "subId">
type WireCase = { name: string; kind: "fetch" | "sub"; run: () => Promise<Correlated | undefined> }

// Each case on a fresh LimitsTestDO (maxSubsPerSocket = 2) seeded with 3 rows.
async function onLimitsSocket<T>(fn: (ws: WebSocket) => Promise<T>): Promise<T> {
  const room = `f5-wire-${crypto.randomUUID()}`
  await seed(env.LIMITS_DO, room)
  const ws = await openWs(`/limits/${room}`)
  try {
    return await fn(ws)
  } finally {
    ws.close()
  }
}
const fetchCase = (name: string, body: FetchBody): WireCase => ({
  name: `fetch refused for ${name}`,
  kind: "fetch",
  run: () => onLimitsSocket((ws) => reply(ws, { t: "fetch", fetchId: "f1", ...body }, "f1")),
})
const subCase = (name: string, body: SubBody): WireCase => ({
  name: `sub refused for ${name}`,
  kind: "sub",
  run: () => onLimitsSocket((ws) => reply(ws, { t: "sub", subId: "s1", ...body }, "s1")),
})
const WIRE_CASES: Array<WireCase> = [
  fetchCase("unknown collection", { collection: "nope" }),
  fetchCase("malformed cursor (missing whereCurrent)", { collection: "messages", cursor: HALF_CURSOR as never }),
  fetchCase("unsupported predicate (ilike)", { collection: "messages", where: ILIKE }),
  subCase("unknown collection", { collection: "nope" }),
  subCase("unsupported predicate (ilike)", { collection: "messages", where: ILIKE }),
  {
    name: "sub refused over maxSubsPerSocket",
    kind: "sub",
    run: () =>
      onLimitsSocket(async (ws) => {
        await reply(ws, { t: "sub", subId: "s1", collection: "messages" }, "s1")
        await reply(ws, { t: "sub", subId: "s2", collection: "files" }, "s2")
        return reply(ws, { t: "sub", subId: "s3", collection: "validated" }, "s3")
      }),
  },
]

describe("F5 wire: a refusal is distinguishable from an empty result", () => {
  it("control: a genuinely empty fetch is `page rows:[]` and a genuinely empty sub is `snap-end`", async () => {
    const none = fn("eq", ref("body"), val("none"))
    const [page, snap] = await onLimitsSocket(async (ws) => [
      await reply(ws, { t: "fetch", fetchId: "f-empty", collection: "messages", where: none }, "f-empty"),
      await reply(ws, { t: "sub", subId: "s-empty", collection: "messages", where: none }, "s-empty"),
    ])
    expect(page).toMatchObject({ t: "page", rows: [] })
    expect(snap?.t).toBe("snap-end")
  })

  it("every refusal gets a prompt, correlated reply (never silence)", async () => {
    for (const c of WIRE_CASES) expect(await c.run(), c.name).toBeDefined()
  })

  for (const c of WIRE_CASES) {
    it.fails(`${c.name}: the reply is neither the empty result nor a bare reset`, async () => {
      const r = await c.run()
      expect(r, "no correlated reply").toBeDefined()
      if (c.kind === "fetch") {
        const { fetchId: _id, seq: _seq, ...shape } = r as Correlated & { seq?: string }
        expect(shape).not.toEqual({ t: "page", rows: [] }) // today: exactly this
      } else {
        expect(r!.t).not.toBe("snap-end")
        expect(r!.t).not.toBe("reset") // today: `reset`, which also means "below the floor, resnapshot"
      }
    })
  }
})

function realTransport(path: string): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com${path}`,
    timeoutMs: 5000,
    open: async () => (await openWs(path)) as unknown as WebSocketLike,
  })
}

/** How a promise settles within `ms`; never throws. */
async function outcome(p: Promise<unknown>, ms = 1500): Promise<string> {
  return Promise.race([
    p.then(
      () => "resolved",
      (e: unknown) => `rejected: ${String(e)}`,
    ),
    new Promise<string>((r) => setTimeout(() => r("pending"), ms)),
  ])
}

// `status` is set for eager collections: a refused eager sub should fail the
// collection's readiness (markError), the route an eager receipt failure takes.
type AppCase = { name: string; run: () => Promise<{ o: string; status?: string }> }

async function withTransport<T>(path: "sync" | "limits", fn: (t: WebSocketTransport<TestApi>) => Promise<T>): Promise<T> {
  const room = `f5-app-${crypto.randomUUID()}`
  await seed(path === "sync" ? env.SYNC_DO : env.LIMITS_DO, room)
  const t = realTransport(`/${path}/${room}`)
  try {
    return await fn(t)
  } finally {
    t.close()
  }
}
const fetchApp = (name: string, body: FetchBody): AppCase => ({
  name: `transport.fetch with ${name} rejects instead of resolving []`,
  run: () =>
    withTransport("sync", async (t) => {
      await t.connect()
      return { o: await outcome(t.fetch({ t: "fetch", fetchId: "f1", ...body })) }
    }),
})
const APP_CASES: Array<AppCase> = [
  {
    name: "eager collection on an unknown table: preload() rejects and the collection is in error",
    run: () =>
      withTransport("sync", async (t) => {
        const c = createCollection(doCollectionOptions({ transport: t, table: "nope" as "messages", getKey: (r) => r.id }))
        return { o: await outcome(c.preload()), status: c.status }
      }),
  },
  {
    name: "eager collection with an unsupported static `where`: preload() rejects and the collection is in error",
    run: () =>
      withTransport("sync", async (t) => {
        const c = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id, where: ILIKE }))
        return { o: await outcome(c.preload()), status: c.status } // today: ready, size 0; the server holds 3 matching rows
      }),
  },
  {
    name: "eager collection past the sub cap: its preload() rejects and it is in error",
    run: () =>
      withTransport("limits", async (t) => {
        // maxSubsPerSocket = 2; three collections share one socket
        await createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id })).preload()
        await createCollection(doCollectionOptions({ transport: t, table: "files", getKey: (r) => r.id })).preload()
        const c = createCollection(doCollectionOptions({ transport: t, table: "validated", getKey: (r) => r.id }))
        return { o: await outcome(c.preload()), status: c.status }
      }),
  },
  {
    name: "on-demand live query on an unknown table: preload() rejects",
    run: () =>
      withTransport("sync", async (t) => {
        const c = createCollection(doCollectionOptions({ transport: t, table: "nope" as "messages", getKey: (r) => r.id, syncMode: "on-demand" }))
        return { o: await outcome(createLiveQueryCollection((qb) => qb.from({ n: c })).preload()) }
      }),
  },
  {
    name: "on-demand live query with an unsupported predicate (ilike): preload() rejects",
    run: () =>
      withTransport("sync", async (t) => {
        const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }))
        const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => ilike(m.body, "%b%")))
        return { o: await outcome(q.preload()) } // today: ready, size 0; the server holds 3 matching rows
      }),
  },
  fetchApp("an unknown collection", { collection: "nope" }),
  fetchApp("a malformed cursor", { collection: "messages", cursor: HALF_CURSOR as never }),
  fetchApp("an unsupported predicate (ilike)", { collection: "messages", where: ILIKE }),
]

describe("F5 app: a refused load rejects instead of settling ready + empty", () => {
  it("control: an eager collection on a registered table preloads ready with its rows", async () => {
    const { o, size } = await withTransport("sync", async (t) => {
      const c = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }))
      return { o: await outcome(c.preload()), size: c.size }
    })
    expect(o).toBe("resolved")
    expect(size).toBe(3)
  })

  it("every refused load settles promptly (never hangs)", async () => {
    for (const c of APP_CASES) expect((await c.run()).o, c.name).not.toBe("pending")
  })

  for (const c of APP_CASES) {
    it.fails(c.name, async () => {
      const { o, status } = await c.run()
      expect(o).toMatch(/^rejected/) // today: resolved
      if (status !== undefined) expect(status).toBe("error") // today: ready
    })
  }
})
