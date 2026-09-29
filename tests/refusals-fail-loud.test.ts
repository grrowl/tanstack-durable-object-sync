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
// cause and surface, each `it.fails` until that design lands. They assert the
// OBSERVABLE contract (an error-typed reply correlated to the request; a load
// that rejects), not a frame name — the ADR picks those. The passing controls
// pin the other half: an empty result stays a success, and prove the harness
// so a `.fails` cannot pass on a broken setup alone.
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

type Correlated = ServerFrame & { fetchId?: string; sub?: string; subId?: string; code?: unknown }

/** Send `f`, resolve with the first frame correlated to it that is not a row
 *  (`snap`/`d`). Never rejects: a missing reply resolves `undefined`, so a
 *  `.fails` below fails on its assertion, not on the harness. */
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

/** The refusal contract: an error-typed reply carrying a code, not the empty
 *  result's `page`/`snap-end` nor the below-floor `reset`. */
function expectErrorReply(r: Correlated | undefined): void {
  expect(r, "no correlated reply").toBeDefined()
  expect(r!.t).toMatch(/error/)
  expect(typeof r!.code).toBe("string")
}

describe("F5 wire: a refusal is distinguishable from an empty result", () => {
  it("control: a genuinely empty fetch is `page rows:[]` and a genuinely empty sub is `snap-end`", async () => {
    const room = `f5-wire-empty-${crypto.randomUUID()}`
    await seed(env.LIMITS_DO, room)
    const ws = await openWs(`/limits/${room}`)
    const none = fn("eq", ref("body"), val("none"))
    const page = await reply(ws, { t: "fetch", fetchId: "f-empty", collection: "messages", where: none }, "f-empty")
    const snap = await reply(ws, { t: "sub", subId: "s-empty", collection: "messages", where: none }, "s-empty")
    ws.close()
    expect(page).toMatchObject({ t: "page", rows: [] })
    expect(snap?.t).toBe("snap-end")
  })

  const fetchCases: Array<[string, Omit<Extract<ClientFrame, { t: "fetch" }>, "t" | "fetchId">]> = [
    ["unknown collection", { collection: "nope" }],
    ["malformed cursor (missing whereCurrent)", { collection: "messages", cursor: HALF_CURSOR as never }],
    ["unsupported predicate (ilike)", { collection: "messages", where: ILIKE }],
  ]
  for (const [cause, body] of fetchCases) {
    it.fails(`fetch refused for ${cause}: an error reply correlated by fetchId, not an empty page`, async () => {
      const room = `f5-wire-fetch-${crypto.randomUUID()}`
      await seed(env.LIMITS_DO, room)
      const ws = await openWs(`/limits/${room}`)
      const r = await reply(ws, { t: "fetch", fetchId: "f1", ...body }, "f1")
      ws.close()
      expectErrorReply(r)
    })
  }

  const subCases: Array<[string, Omit<Extract<ClientFrame, { t: "sub" }>, "t" | "subId">]> = [
    ["unknown collection", { collection: "nope" }],
    ["unsupported predicate (ilike)", { collection: "messages", where: ILIKE }],
  ]
  for (const [cause, body] of subCases) {
    it.fails(`sub refused for ${cause}: an error reply correlated by subId, not a bare reset`, async () => {
      const room = `f5-wire-sub-${crypto.randomUUID()}`
      await seed(env.LIMITS_DO, room)
      const ws = await openWs(`/limits/${room}`)
      const r = await reply(ws, { t: "sub", subId: "s1", ...body }, "s1")
      ws.close()
      expectErrorReply(r)
    })
  }

  it.fails("sub refused over maxSubsPerSocket: an error reply correlated by subId, not a bare reset", async () => {
    const room = `f5-wire-cap-${crypto.randomUUID()}`
    await seed(env.LIMITS_DO, room) // LimitsTestDO: maxSubsPerSocket = 2
    const ws = await openWs(`/limits/${room}`)
    await reply(ws, { t: "sub", subId: "s1", collection: "messages" }, "s1")
    await reply(ws, { t: "sub", subId: "s2", collection: "files" }, "s2")
    const r = await reply(ws, { t: "sub", subId: "s3", collection: "validated" }, "s3")
    ws.close()
    expectErrorReply(r)
  })
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

describe("F5 app: a refused load rejects instead of settling ready + empty", () => {
  it("control: an eager collection on a registered table preloads ready with its rows", async () => {
    const room = `f5-app-ok-${crypto.randomUUID()}`
    await seed(env.SYNC_DO, room)
    const t = realTransport(`/sync/${room}`)
    const c = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }))
    const o = await outcome(c.preload())
    t.close()
    expect(o).toBe("resolved")
    expect(c.size).toBe(3)
  })

  it.fails("eager collection on an unknown table: preload() rejects and the collection is in error", async () => {
    const room = `f5-app-eager-unk-${crypto.randomUUID()}`
    await seed(env.SYNC_DO, room)
    const t = realTransport(`/sync/${room}`)
    const c = createCollection(doCollectionOptions({ transport: t, table: "nope" as "messages", getKey: (r) => r.id }))
    const o = await outcome(c.preload())
    t.close()
    expect(o).toMatch(/^rejected/) // today: resolved, status ready, size 0
    expect(c.status).toBe("error")
  })

  it.fails("eager collection with an unsupported static `where`: preload() rejects and the collection is in error", async () => {
    const room = `f5-app-eager-pred-${crypto.randomUUID()}`
    await seed(env.SYNC_DO, room)
    const t = realTransport(`/sync/${room}`)
    const c = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id, where: ILIKE }))
    const o = await outcome(c.preload())
    t.close()
    expect(o).toMatch(/^rejected/) // today: resolved, ready, size 0 — the server holds 3 matching rows
    expect(c.status).toBe("error")
  })

  it.fails("eager collection past the sub cap: its preload() rejects and it is in error", async () => {
    const room = `f5-app-eager-cap-${crypto.randomUUID()}`
    await seed(env.LIMITS_DO, room) // maxSubsPerSocket = 2; three collections share one socket
    const t = realTransport(`/limits/${room}`)
    const a = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }))
    const b = createCollection(doCollectionOptions({ transport: t, table: "files", getKey: (r) => r.id }))
    const c = createCollection(doCollectionOptions({ transport: t, table: "validated", getKey: (r) => r.id }))
    await a.preload()
    await b.preload()
    const o = await outcome(c.preload())
    t.close()
    expect(o).toMatch(/^rejected/) // today: resolved, ready, size 0
    expect(c.status).toBe("error")
  })

  it.fails("on-demand live query on an unknown table: preload() rejects", async () => {
    const room = `f5-app-od-unk-${crypto.randomUUID()}`
    await seed(env.SYNC_DO, room)
    const t = realTransport(`/sync/${room}`)
    const c = createCollection(
      doCollectionOptions({ transport: t, table: "nope" as "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    )
    const q = createLiveQueryCollection((qb) => qb.from({ n: c }))
    const o = await outcome(q.preload())
    t.close()
    expect(o).toMatch(/^rejected/) // today: resolved, ready, size 0
  })

  it.fails("on-demand live query with an unsupported predicate (ilike): preload() rejects", async () => {
    const room = `f5-app-od-pred-${crypto.randomUUID()}`
    await seed(env.SYNC_DO, room)
    const t = realTransport(`/sync/${room}`)
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }))
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => ilike(m.body, "%b%")))
    const o = await outcome(q.preload())
    t.close()
    expect(o).toMatch(/^rejected/) // today: resolved, ready, size 0 — the server holds 3 matching rows
  })

  const fetchCases: Array<[string, Omit<Extract<ClientFrame, { t: "fetch" }>, "t" | "fetchId">]> = [
    ["an unknown collection", { collection: "nope" }],
    ["a malformed cursor", { collection: "messages", cursor: HALF_CURSOR as never }],
    ["an unsupported predicate (ilike)", { collection: "messages", where: ILIKE }],
  ]
  for (const [cause, body] of fetchCases) {
    it.fails(`transport.fetch with ${cause} rejects instead of resolving []`, async () => {
      const room = `f5-app-fetch-${crypto.randomUUID()}`
      await seed(env.SYNC_DO, room)
      const t = realTransport(`/sync/${room}`)
      await t.connect()
      const o = await outcome(t.fetch({ t: "fetch", fetchId: "f1", ...body }))
      t.close()
      expect(o).toMatch(/^rejected/)
    })
  }
})
