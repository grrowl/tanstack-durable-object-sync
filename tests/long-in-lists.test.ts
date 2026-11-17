import { BTreeIndex, createCollection, createLiveQueryCollection, eq, inArray } from "@tanstack/db"
import { env, evictDurableObject, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: Durable Object SQLite allows at most 100 bound parameters in one
// statement. The `where` compiler used to bind one per `in` value, so a list of
// 101 failed in SQLite: the `sub` got no reply (the live query stayed loading
// forever), a `fetch` timed out, and the refused sub stayed registered. An app
// reaches this without writing `inArray`: @tanstack/db's join loader sends one
// `inArray` over every join key. A long list must work on every path that runs
// the compiled SQL, and a predicate that still cannot fit within SQLite's
// limits must be refused on purpose, with the refusal reply and nothing of the
// sub left registered (ADR-0013 and ADR-0012 amendments, 2026-11-17).

const codec = createFrameCodec()
const room = (tag: string): string => `lil-${tag}-${crypto.randomUUID()}`
const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))

async function openWs(r: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}
const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 3000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })
/** `n` ids m0..m{n-1}; the rows seeded below are a few of them plus outsiders. */
const idList = (n: number): Array<string> => Array.from({ length: n }, (_, i) => `m${i}`)
const inIds = (n: number): unknown => fn("in", ref("id"), val(idList(n)))

/** Seed rows straight into SQLite and drain, before any subscriber exists. */
async function seed(r: string, ids: Array<string>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    for (const id of ids) s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", id, `body-${id}`)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
const persistedSubs = (r: string): Promise<Array<string>> =>
  runInDurableObject(stubFor(r), (_i, s) =>
    Array.from(s.storage.sql.exec<{ sub_id: string }>("SELECT sub_id FROM _sync_subs")).map((x) => x.sub_id),
  )
/** Not registered in memory: a write to `key` on this socket would flush a
 *  delta before `committed` (ADR-0002 C1) if any sub were still registered. */
async function expectNoDeltaFor(ws: WebSocket, key: string): Promise<void> {
  const txId = `probe-${crypto.randomUUID()}`
  send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "update", key, cols: { body: "changed" } }] })
  const after = await collectUntil(ws, (f) => f.t === "committed" && f.txId === txId)
  expect(after.filter((f) => f.t === "d")).toEqual([])
}
const snapKeys = (frames: Array<ServerFrame>, sub: string): Array<string> =>
  frames.flatMap((f) => (f.t === "snap" && f.sub === sub ? [f.key as string] : [])).sort()
const pageIds = (frames: Array<ServerFrame>): Array<string> =>
  frames.flatMap((f) => (f.t === "page" ? (f.rows as Array<{ id: string }>).map((row) => row.id) : [])).sort()

// Seeded: three ids the long lists name (m0, m1 and the last of 101, m100) and
// one they never name.
const SEEDED = ["m0", "m1", "m100", "zz-outside"]

describe("an `in` list longer than the bind limit (sub)", () => {
  for (const n of [100, 101, 3000]) {
    it(`in(${n}) gets its snapshot`, async () => {
      const r = room(`sub${n}`)
      await seed(r, SEEDED)
      const ws = await openWs(r)
      send(ws, { t: "sub", subId: "s1", collection: "messages", where: inIds(n) })
      const frames = await collectUntil(ws, (f) => f.t === "snap-end" || f.t === "reset")
      expect(frames.at(-1)!.t).toBe("snap-end")
      expect(snapKeys(frames, "s1")).toEqual(n > 100 ? ["m0", "m1", "m100"] : ["m0", "m1"])
      ws.close()
    })
  }

  it("two lists of 60 in one predicate get their snapshot (the rule is per statement, not per list)", async () => {
    const r = room("two60")
    await seed(r, SEEDED)
    const ws = await openWs(r)
    const a = idList(60) // m0..m59
    const b = [...idList(60).slice(1), "m100"] // m1..m59, m100
    const where = fn("or", fn("in", ref("id"), val(a)), fn("in", ref("id"), val(b)))
    send(ws, { t: "sub", subId: "s1", collection: "messages", where, limit: 50, offset: 0 })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end" || f.t === "reset")
    expect(frames.at(-1)!.t).toBe("snap-end")
    expect(snapKeys(frames, "s1")).toEqual(["m0", "m1", "m100"])
    ws.close()
  })

  it("a reconnect below the retention floor re-snapshots a long list", async () => {
    const r = room("floor")
    await seed(r, SEEDED)
    const ws = await openWs(r)
    send(ws, { t: "sub", subId: "s0", collection: "messages" })
    const first = await collectUntil(ws, (f) => f.t === "snap-end")
    const since = (first.at(-1) as Extract<ServerFrame, { t: "snap-end" }>).seq
    // The change log no longer reaches back to `since`: the server must answer
    // with reset + a full snapshot of the predicate.
    await runInDurableObject(stubFor(r), (_i, s) => {
      s.storage.sql.exec("DELETE FROM _sync_changes")
    })
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: inIds(101), since })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end" && f.sub === "s1")
    expect(frames[0]).toMatchObject({ t: "reset", sub: "s1" })
    expect(snapKeys(frames, "s1")).toEqual(["m0", "m1", "m100"])
    ws.close()
  })

  it("a windowed catch-up resolves a long list's membership", async () => {
    const r = room("catchup")
    await seed(r, ["zz-before"]) // a non-zero cursor to resume from
    const ws = await openWs(r)
    send(ws, { t: "sub", subId: "s0", collection: "messages" })
    const first = await collectUntil(ws, (f) => f.t === "snap-end")
    const since = (first.at(-1) as Extract<ServerFrame, { t: "snap-end" }>).seq
    send(ws, { t: "unsub", subId: "s0" })
    send(ws, {
      t: "mut",
      txId: "tx1",
      collection: "messages",
      ops: SEEDED.map((id) => ({ type: "insert" as const, key: id, cols: { id, body: `body-${id}` } })),
    })
    await collectUntil(ws, (f) => f.t === "committed")
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: inIds(101), since })
    const frames = await collectUntil(ws, (f) => f.t === "uptodate" && f.sub === "s1")
    const live = frames.flatMap((f) => (f.t === "d" && f.sub === "s1" && f.op !== "delete" ? [f.key as string] : []))
    const out = frames.flatMap((f) => (f.t === "d" && f.sub === "s1" && f.op === "delete" ? [f.key as string] : []))
    expect(live.sort()).toEqual(["m0", "m1", "m100"])
    expect(out).toEqual(["zz-outside"])
    ws.close()
  })

  it("a long-list sub restored after hibernation keeps receiving its deltas", async () => {
    const r = room("hib")
    await seed(r, SEEDED)
    const wsA = await openWs(r)
    send(wsA, { t: "sub", subId: "s1", collection: "messages", where: inIds(3000) })
    await collectUntil(wsA, (f) => f.t === "snap-end")
    await evictDurableObject(stubFor(r))

    const wsB = await openWs(r)
    const toA = collectUntil(wsA, (f) => f.t === "d" && f.key === "m2999")
    send(wsB, {
      t: "mut",
      txId: "after-wake",
      collection: "messages",
      ops: [
        { type: "insert", key: "m2999", cols: { id: "m2999", body: "in the list" } },
        { type: "insert", key: "zz-late", cols: { id: "zz-late", body: "not in the list" } },
      ],
    })
    await collectUntil(wsB, (f) => f.t === "committed")
    const frames = await toA
    expect(frames.find((f) => f.t === "d" && f.key === "m2999")).toMatchObject({ sub: "s1", op: "insert" })
    wsA.close()
    wsB.close()
  })
})

describe("an `in` list longer than the bind limit (fetch)", () => {
  it("a plain fetch gets its page", async () => {
    const r = room("fetch")
    await seed(r, SEEDED)
    const ws = await openWs(r)
    send(ws, { t: "fetch", fetchId: "f1", collection: "messages", where: inIds(3000) })
    const frames = await collectUntil(ws, (f) => f.t === "page")
    expect(pageIds(frames)).toEqual(["m0", "m1", "m100"])
    ws.close()
  })

  it("both halves of a cursor fetch run with a long base list", async () => {
    const r = room("cursor")
    await seed(r, SEEDED)
    const ws = await openWs(r)
    const orderBy = [{ expression: { type: "ref", path: ["id"] }, compareOptions: { direction: "asc", nulls: "first" } }]
    send(ws, {
      t: "fetch",
      fetchId: "f1",
      collection: "messages",
      where: inIds(101),
      orderBy,
      limit: 1,
      // ties: id = m0; next page: id > m0 (bounded by limit 1 → m1)
      cursor: { whereCurrent: fn("eq", ref("id"), val("m0")), whereFrom: fn("gt", ref("id"), val("m0")) },
    } as ClientFrame)
    const frames = await collectUntil(ws, (f) => f.t === "page")
    expect(pageIds(frames)).toEqual(["m0", "m1"])
    ws.close()
  })
})

describe("a predicate over SQLite's limits is refused on purpose", () => {
  // 101 `eq` terms bind 101 values: no SQL form fits the limit.
  const orOf101 = fn("or", ...idList(101).map((id) => fn("eq", ref("id"), val(id))))
  // Binds nothing, but its expression tree is deeper than the 100 levels
  // workerd allows SQLite. The compiler refuses it before SQLite sees it.
  const deep = fn("or", ...Array.from({ length: 120 }, () => fn("isNull", ref("body"))))

  for (const [label, where] of [
    ["an `or` of 101 `eq` terms", orOf101],
    ["a predicate deeper than SQLite allows", deep],
  ] as const) {
    it(`sub: ${label} → reset, and nothing of the sub stays registered`, async () => {
      const r = room("refuse")
      await seed(r, SEEDED)
      const ws = await openWs(r)
      send(ws, { t: "sub", subId: "s1", collection: "messages", where })
      const frames = await collectUntil(ws, (f) => f.t === "reset" || f.t === "snap-end")
      // No snapshot row precedes the refusal: the client treats a reset before
      // snap-end as "holds no rows" (ADR-0023 D5).
      expect(frames.map((f) => f.t)).toEqual(["reset"])
      expect(await persistedSubs(r)).toEqual([])
      await expectNoDeltaFor(ws, "m0")
      ws.close()
    })

    it(`fetch: ${label} → an empty page`, async () => {
      const r = room("refuse-fetch")
      await seed(r, SEEDED)
      const ws = await openWs(r)
      send(ws, { t: "fetch", fetchId: "f1", collection: "messages", where })
      const frames = await collectUntil(ws, (f) => f.t === "page")
      expect(frames.at(-1)).toMatchObject({ t: "page", fetchId: "f1", rows: [] })
      ws.close()
    })
  }

  it("a re-sub that fails drops the subId it replaced", async () => {
    const r = room("resub")
    await seed(r, SEEDED)
    const ws = await openWs(r)
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: inIds(2) })
    await collectUntil(ws, (f) => f.t === "snap-end")
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: orOf101 })
    await collectUntil(ws, (f) => f.t === "reset")
    expect(await persistedSubs(r)).toEqual([])
    await expectNoDeltaFor(ws, "m0")
    ws.close()
  })

  it("a re-sub naming an unknown collection drops the subId it replaced", async () => {
    const r = room("resub-unknown")
    await seed(r, SEEDED)
    const ws = await openWs(r)
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: inIds(2) })
    await collectUntil(ws, (f) => f.t === "snap-end")
    send(ws, { t: "sub", subId: "s1", collection: "no_such_collection" })
    await collectUntil(ws, (f) => f.t === "reset")
    expect(await persistedSubs(r)).toEqual([])
    await expectNoDeltaFor(ws, "m0")
    ws.close()
  })
})

describe("SSR read (readSyncSnapshot)", () => {
  it("reads a long list", async () => {
    const r = room("ssr")
    await seed(r, SEEDED)
    const stub = stubFor(r) as unknown as {
      readSyncSnapshot: (q: unknown, req: Request) => Promise<{ rows: Array<{ id: string }> }>
    }
    const { rows } = await stub.readSyncSnapshot(
      { collection: "messages", where: inIds(3000) },
      new Request("https://example.com/ssr", { headers: { "x-user": "anon" } }),
    )
    expect(rows.map((row) => row.id).sort()).toEqual(["m0", "m1", "m100"])
  })
})

describe("through a real client", () => {
  function transportFor(r: string): WebSocketTransport<TestApi> {
    return new WebSocketTransport<TestApi>({
      url: `https://example.com/sync/${r}`,
      open: async () => (await openWs(r)) as unknown as WebSocketLike,
    })
  }
  const ready = (p: Promise<unknown>, ms = 3000): Promise<unknown> =>
    Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(`not ready after ${ms} ms`)), ms))])

  it("an on-demand live query over inArray of 101 ids becomes ready with its rows", async () => {
    const r = room("app")
    await seed(r, SEEDED)
    const t = transportFor(r)
    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    const ids = idList(101)
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => inArray(m.id, ids)))
    await ready(q.preload())
    expect(q.toArray.map((m) => m.id).sort()).toEqual(["m0", "m1", "m100"])
    t.close()
  })

  it("a join over more than 100 keys loads the on-demand side", async () => {
    const r = room("join")
    // 150 files, each naming one message id; only three of those messages exist.
    await runInDurableObject(stubFor(r), (instance, s) => {
      for (let i = 0; i < 150; i++) s.storage.sql.exec("INSERT INTO files(id, name) VALUES (?, ?)", `f${i}`, `m${i}`)
      for (const id of SEEDED) s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", id, `body-${id}`)
      ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
    })
    const t = transportFor(r)
    const keys: Array<number> = []
    const subscribe = t.subscribe.bind(t)
    t.subscribe = (...a) => {
      const where = a[3] as { name?: string; args?: Array<{ value?: unknown }> } | undefined
      if (where?.name === "in" && Array.isArray(where.args?.[1]?.value)) keys.push(where.args[1].value.length)
      return subscribe(...a)
    }
    const files = createCollection(doCollectionOptions({ transport: t, table: "files", getKey: (f) => f.id }))
    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    // The join loads the on-demand side by key only through an index on it.
    messages.createIndex((m) => m.id, { indexType: BTreeIndex })
    await files.preload()
    const q = createLiveQueryCollection((qb) =>
      // A left join loads its right side lazily, by the left side's keys.
      qb
        .from({ f: files })
        .join({ m: messages }, ({ f, m }) => eq(f.name, m.id), "left")
        .select(({ f, m }) => ({ file: f.id, message: m?.id })),
    )
    await ready(q.preload())
    // Premise: the join loader sent one `in` over more than 100 keys.
    expect(Math.max(0, ...keys)).toBeGreaterThan(100)
    const joined = (): Array<string> => q.toArray.flatMap((row) => (row.message ? [row.message] : [])).sort()
    await waitFor(() => joined().length === 3)
    expect(joined()).toEqual(["m0", "m1", "m100"])
    t.close()
  })
})
