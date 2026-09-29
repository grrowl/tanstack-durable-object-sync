// WHY (bugbash F4-ties): when more rows tie on the order value than the limit,
// WHICH rows land in the window is decided by the tie-break. @tanstack/db breaks
// ties by row key, ascending regardless of the order direction (db-ivm
// `createKeyedComparator` -> `compareKeys`, identical in 0.8.6/db-ivm 0.1.19 and
// 0.9.2/db-ivm 0.1.22). SQLite's top-k otherwise picks by scan order, so the
// server would ship a different window than the client's own live query holds.
// Every bounded path builds its ORDER BY through `compileSubsetQuery`; these tests
// drive the real sub and fetch paths and compare with TanStack's own answer.
import { createCollection, createLiveQueryCollection, localOnlyCollectionOptions } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()
const K = 3

type Row = { id: string; body: string | null }

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), 3000)
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

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))
const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })

// Inserted in an order that is NOT key order, so scan order != key order. "10" < "9"
// as strings: the tie-break is string comparison, not numeric.
const ids = ["z", "9", "a", "m", "10", "b"]

async function seed(room: string, body: string | null): Promise<Array<Row>> {
  const rows = ids.map((id) => ({ id, body }))
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const r of rows) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", r.id, r.body)
  })
  return rows
}

// TanStack's own answer for the window over the same rows.
async function tanstackWindow(rows: Array<Row>, direction: "asc" | "desc", nulls: "first" | "last"): Promise<Array<string>> {
  const local = createCollection(localOnlyCollectionOptions<Row>({ id: `t-${crypto.randomUUID()}`, getKey: (r) => r.id, initialData: rows }))
  const q = createLiveQueryCollection((b) =>
    b.from({ m: local }).orderBy(({ m }) => m.body, { direction, nulls, stringSort: "lexical" }).limit(K),
  )
  await q.preload()
  return q.toArray.map((r) => r.id)
}

const orderBy = (direction: "asc" | "desc", nulls: "first" | "last") =>
  [{ expression: ref("body"), compareOptions: { direction, nulls, stringSort: "lexical" } }] as never

describe("bounded ORDER BY breaks ties by row key like @tanstack/db (bugbash F4-ties)", () => {
  for (const direction of ["asc", "desc"] as const) {
    for (const nulls of ["first", "last"] as const) {
      it(`sub snapshot: all-NULL ties, ${direction} nulls=${nulls}`, async () => {
        const room = `ties-sub-${crypto.randomUUID()}`
        const rows = await seed(room, null)
        const ws = await openWs(room)
        send(ws, { t: "sub", subId: "s1", collection: "messages", orderBy: orderBy(direction, nulls), limit: K })
        const frames = await collectUntil(ws, (f) => f.t === "snap-end")
        ws.close()
        const got = frames.filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap").map((f) => f.key as string)
        expect(got).toEqual(await tanstackWindow(rows, direction, nulls))
        expect(got).toEqual(["10", "9", "a"])
      })

      it(`fetch page: all-NULL ties, ${direction} nulls=${nulls}`, async () => {
        const room = `ties-fetch-${crypto.randomUUID()}`
        const rows = await seed(room, null)
        const ws = await openWs(room)
        send(ws, { t: "fetch", fetchId: "f1", collection: "messages", orderBy: orderBy(direction, nulls), limit: K })
        const [page] = await collectUntil(ws, (f) => f.t === "page")
        ws.close()
        const got = (page as Extract<ServerFrame, { t: "page" }>).rows.map((r) => (r as Row).id)
        expect(got).toEqual(await tanstackWindow(rows, direction, nulls))
      })
    }
  }

  it("fetch page after a cursor: the bounded next page picks tied rows by key", async () => {
    const room = `ties-cursor-${crypto.randomUUID()}`
    await seed(room, "v")
    const ws = await openWs(room)
    // whereFrom = gt("a"): every seeded row qualifies and all tie on body = "v".
    send(ws, {
      t: "fetch", fetchId: "f1", collection: "messages", orderBy: orderBy("asc", "first"), limit: K,
      cursor: { whereFrom: fn("gt", ref("body"), val("a")) as never, whereCurrent: fn("eq", ref("body"), val("a")) as never },
    })
    const [page] = await collectUntil(ws, (f) => f.t === "page")
    ws.close()
    expect((page as Extract<ServerFrame, { t: "page" }>).rows.map((r) => (r as Row).id)).toEqual(["10", "9", "a"])
  })
})
