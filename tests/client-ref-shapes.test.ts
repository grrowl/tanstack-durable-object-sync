import * as db from "@tanstack/db"
import { and, BTreeIndex, createCollection, createLiveQueryCollection, eq, gt, localOnlyCollectionOptions, not, or } from "@tanstack/db"
import dbPackage from "@tanstack/db/package.json"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { type SnapshotRead, SsrSnapshotTransport } from "../src/client/ssr-transport.ts"
import { type Transport, type WebSocketLike, WebSocketTransport } from "../src/client/transport.ts"
import { columnRefProblem } from "../src/wire/column-ref.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { TestApi } from "./test-worker.ts"

// WHY (ADR-0013 and ADR-0023 amendments 2026-11-17): the server reads a column
// ref only in the one shape `{ type: "ref", path: [name] }`, because any other
// shape is read differently by @tanstack/db's evaluator. Which shapes the
// library's client sends depends on the @tanstack/db version: 0.8.6 pushes an
// `orderBy` and `limit` through a subquery or a join to the source collection,
// with refs such as `["sub", "body"]` or `["f", "name"]` that the server cannot
// sort by. So the on-demand adapter sends such a request without its `orderBy`
// and `limit` (the client sorts and cuts the rows it holds). A `where` that holds
// such a ref (a nested field, or core's tie request for an order on one) is sent
// as the tightest `where` the server can read that contains it (the client
// filters the rows it holds).
//
// Each shape below runs through the real adapter and Durable Object, and checks:
// 1. the refs core asks the adapter for, per version line, as written down here.
//    A later @tanstack/db that sends another shape fails this first; the weekly
//    job runs the suite against the newest release;
// 2. that no ref the adapter puts on the wire is one the server refuses;
// 3. that the live query ends ready with the rows and order @tanstack/db itself
//    gives over the same rows, before and after its window grows;
// and each shape is also rendered as a server render does, through
// `SsrSnapshotTransport` reading `readSyncSnapshot`, where a refused read throws.

const line = ((): string => {
  const [major, minor] = (dbPackage as { version: string }).version.split(".")
  return `${major}.${minor}`
})()
const db08 = "isWhereSubset" in db
const codec = createFrameCodec()

type Msg = { id: string; body: string }
type File = { id: string; name: string }
// Bodies of varying length, so an order by `body.length` differs from one by
// `body`; two equal bodies, so a second order clause matters.
const MESSAGES: Array<Msg> = [
  { id: "m0", body: "ccc" },
  { id: "m1", body: "a" },
  { id: "m2", body: "bb" },
  { id: "m3", body: "a" },
  { id: "m4", body: "d" },
  { id: "m5", body: "bbbbb" },
]
const FILES: Array<File> = MESSAGES.map((m, i) => ({ id: m.id, name: "zyxwvu"[i]! }))

type Build = (m: any, f: any) => (q: any) => any
/** `index`: a range index on `messages`, so growing the window loads more with a
 *  cursor (0.8 re-requests a window only through one). */
const SHAPES: Record<string, { build: Build; windowed: boolean; index?: (m: any) => unknown }> = {
  "orderBy with a limit": { windowed: true, build: (m) => (q) => q.from({ m }).orderBy(({ m }: any) => m.body).limit(2) },
  "where, orderBy desc, limit": {
    windowed: true,
    build: (m) => (q) => q.from({ m }).where(({ m }: any) => gt(m.body, "a")).orderBy(({ m }: any) => m.body, "desc").limit(2),
  },
  "join, orderBy on the from side": {
    windowed: true,
    build: (m, f) => (q) => q.from({ m }).join({ f }, ({ m, f }: any) => eq(m.id, f.id)).orderBy(({ m }: any) => m.body).limit(2),
  },
  // 0.8.6 sends `orderBy ["sub", "body"]` with the limit to `messages`.
  "subquery with a select of the same name, orderBy outside": {
    windowed: true,
    build: (m) => (q) =>
      q.from({ sub: q.from({ m }).select(({ m }: any) => ({ id: m.id, body: m.body })) }).orderBy(({ sub }: any) => sub.body).limit(2),
  },
  // 0.8.6 sends `orderBy ["sub", "title"]` with the limit to `messages`.
  "subquery with a renaming select, orderBy outside": {
    windowed: true,
    build: (m) => (q) =>
      q.from({ sub: q.from({ m }).select(({ m }: any) => ({ id: m.id, title: m.body })) }).orderBy(({ sub }: any) => sub.title).limit(2),
  },
  // 0.8.6 sends `orderBy [["body"], ["f", "name"]]` with the limit to `messages`.
  "join, two orderBy clauses on different aliases": {
    windowed: true,
    build: (m, f) => (q) =>
      q.from({ m }).join({ f }, ({ m, f }: any) => eq(m.id, f.id)).orderBy(({ m }: any) => m.body).orderBy(({ f }: any) => f.name).limit(2),
  },
  // Every version sends `orderBy ["body", "length"]` with the limit.
  "a nested field": { windowed: true, build: (m) => (q) => q.from({ m }).orderBy(({ m }: any) => m.body.length).limit(2) },
  "orderBy with a limit, indexed": {
    windowed: true,
    index: (m) => m.body,
    build: (m) => (q) => q.from({ m }).orderBy(({ m }: any) => m.body).limit(2),
  },
  "subquery with a select of the same name, indexed": {
    windowed: true,
    index: (m) => m.body,
    build: (m) => (q) =>
      q.from({ sub: q.from({ m }).select(({ m }: any) => ({ id: m.id, body: m.body })) }).orderBy(({ sub }: any) => sub.body).limit(2),
  },
  "a nested field, indexed": {
    windowed: true,
    index: (m) => m.body.length,
    build: (m) => (q) => q.from({ m }).orderBy(({ m }: any) => m.body.length).limit(2),
  },
  // A `where` on a nested field, alone and inside an `and`, an `or` and a `not`.
  // The adapter sends the readable part (or none); the client filters.
  "where on a nested field": { windowed: false, build: (m) => (q) => q.from({ m }).where(({ m }: any) => eq(m.body.length, 1)) },
  "where, and of a column and a nested field, ordered": {
    windowed: true,
    build: (m) => (q) =>
      q.from({ m }).where(({ m }: any) => and(gt(m.body, "a"), eq(m.body.length, 1))).orderBy(({ m }: any) => m.body).limit(2),
  },
  "where, or of a column and a nested field": {
    windowed: false,
    build: (m) => (q) => q.from({ m }).where(({ m }: any) => or(eq(m.body, "ccc"), eq(m.body.length, 1))),
  },
  "where, not of a nested field, ordered": {
    windowed: true,
    build: (m) => (q) => q.from({ m }).where(({ m }: any) => not(eq(m.body.length, 1))).orderBy(({ m }: any) => m.body, "desc").limit(2),
  },
  "where, a column and not(or(column, nested field))": {
    windowed: false,
    build: (m) => (q) =>
      q.from({ m }).where(({ m }: any) => and(gt(m.body, "a"), not(or(eq(m.body, "d"), eq(m.body.length, 2))))),
  },
  findOne: { windowed: false, build: (m) => (q) => q.from({ m }).where(({ m }: any) => eq(m.id, "m2")).findOne() },
}

// What core asks the adapter for, per shape and version line: each ref as
// `collection field path`, with `+key` for any key beside `type` and `path`. A
// line not listed here (a later release) is held to the newest one.
const SINCE_09: Record<string, Array<string>> = {
  "orderBy with a limit": ["messages orderBy body"],
  "where, orderBy desc, limit": ["messages orderBy body", "messages where body"],
  "join, orderBy on the from side": ["files where id", "messages orderBy body"],
  // 0.9 and later load the whole source for these, with no orderBy.
  "subquery with a select of the same name, orderBy outside": [],
  "subquery with a renaming select, orderBy outside": [],
  "join, two orderBy clauses on different aliases": ["files where id"],
  // The tie request (on the first load, and when the window grows) is a `where`
  // on the order expression.
  "a nested field": ["messages orderBy body.length", "messages where body.length"],
  "orderBy with a limit, indexed": ["messages orderBy body"],
  "subquery with a select of the same name, indexed": [],
  "a nested field, indexed": ["messages cursor body.length", "messages orderBy body.length", "messages where body.length"],
  "where on a nested field": ["messages where body.length"],
  "where, and of a column and a nested field, ordered": ["messages orderBy body", "messages where body", "messages where body.length"],
  "where, or of a column and a nested field": ["messages where body", "messages where body.length"],
  "where, not of a nested field, ordered": ["messages orderBy body", "messages where body.length"],
  "where, a column and not(or(column, nested field))": ["messages where body", "messages where body.length"],
  findOne: ["messages where id"],
}

const SENT: Record<string, Record<string, Array<string>>> = {
  "0.8": {
    "orderBy with a limit": ["messages orderBy body"],
    "where, orderBy desc, limit": ["messages orderBy body", "messages where body"],
    "join, orderBy on the from side": ["files where id", "messages orderBy body"],
    "subquery with a select of the same name, orderBy outside": ["messages orderBy sub.body"],
    "subquery with a renaming select, orderBy outside": ["messages orderBy sub.title"],
    "join, two orderBy clauses on different aliases": ["files where id", "messages orderBy body", "messages orderBy f.name"],
    "a nested field": ["messages orderBy body.length"],
    "orderBy with a limit, indexed": ["messages cursor body", "messages orderBy body"],
    "subquery with a select of the same name, indexed": ["messages orderBy sub.body"],
    "a nested field, indexed": ["messages orderBy body.length"],
    "where on a nested field": ["messages where body.length"],
    "where, and of a column and a nested field, ordered": ["messages orderBy body", "messages where body", "messages where body.length"],
    "where, or of a column and a nested field": ["messages where body", "messages where body.length"],
    "where, not of a nested field, ordered": ["messages orderBy body", "messages where body.length"],
    "where, a column and not(or(column, nested field))": ["messages where body", "messages where body.length"],
    findOne: ["messages where id"],
  },
  "0.9": SINCE_09,
  "1.0": SINCE_09,
}

const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await sleep(5)
  }
}

function refs(v: unknown, where: string, out: Array<string>): Array<string> {
  if (Array.isArray(v)) v.forEach((x) => refs(x, where, out))
  else if (v && typeof v === "object") {
    const o = v as Record<string, unknown>
    if (o.type === "ref") {
      const extra = Object.keys(o).filter((k) => k !== "type" && k !== "path")
      out.push(`${where} ${(o.path as Array<string>).join(".")}${extra.map((k) => `+${k}`).join("")}`)
    } else for (const x of Object.values(o)) refs(x, where, out)
  }
  return out
}

/** A real transport to the room; `wire` gets every frame the client sends. */
function transportFor(r: string, wire: Array<Record<string, unknown>>): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 0,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket!
      ws.accept()
      return {
        send: (d) => {
          wire.push(codec.decode(d as ArrayBuffer) as Record<string, unknown>)
          ws.send(d as never)
        },
        close: (c, reason) => ws.close(c, reason),
        addEventListener: (type, fn) => ws.addEventListener(type as never, fn as never),
        removeEventListener: () => {},
      } satisfies WebSocketLike
    },
  })
}

/** An on-demand collection whose `loadSubset` requests are recorded in `asked`. */
function onDemand(t: Transport<TestApi>, table: "messages" | "files", asked: Array<string>, index?: (r: any) => unknown) {
  const opts = doCollectionOptions({ transport: t, table, getKey: (r: { id: string }) => r.id, syncMode: "on-demand" }) as any
  const sync = opts.sync.sync
  opts.sync = {
    ...opts.sync,
    sync: (p: unknown) => {
      const res = sync(p)
      const load = res.loadSubset
      res.loadSubset = (o: Record<string, unknown>) => {
        for (const field of ["where", "orderBy", "cursor"]) refs(o[field], `${table} ${field}`, asked)
        return load(o)
      }
      return res
    },
  }
  const c = createCollection(opts)
  if (index) c.createIndex(index, { indexType: BTreeIndex })
  return c
}

/** Refs in a request that the server refuses: a path of more than one element
 *  shows as `a.b`, an extra key as `+key`. */
const refusedRefs = (req: Record<string, unknown>): Array<string> =>
  ["where", "orderBy", "cursor"].flatMap((field) => refs(req[field], field, [])).filter((r) => /[.+]/.test(r))

async function seed(r: string): Promise<void> {
  await runInDurableObject(stubFor(r), (inst, s) => {
    for (const m of MESSAGES) s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", m.id, m.body)
    for (const f of FILES) s.storage.sql.exec("INSERT INTO files(id, name) VALUES (?, ?)", f.id, f.name)
    ;(inst as unknown as { sync: { drainAndBroadcast(): void } }).sync.drainAndBroadcast()
  })
}

/** @tanstack/db's own answer for `build` over the same rows, all local. */
async function oracle(r: string, build: Build) {
  const local = createLiveQueryCollection({
    startSync: true,
    query: build(
      createCollection(localOnlyCollectionOptions<Msg>({ id: `lm-${r}`, getKey: (x) => x.id, initialData: MESSAGES })),
      createCollection(localOnlyCollectionOptions<File>({ id: `lf-${r}`, getKey: (x) => x.id, initialData: FILES })),
    ),
  })
  await local.preload()
  return local
}

/** Rows without virtual `$` props, comparable across a local and a synced collection. */
const plain = (v: unknown): unknown =>
  Array.isArray(v)
    ? v.map(plain)
    : v && typeof v === "object"
      ? Object.fromEntries(Object.entries(v).filter(([k]) => !k.startsWith("$")).map(([k, x]) => [k, plain(x)]))
      : v
const rowsOf = (lq: { toArray: Array<unknown> }): string => JSON.stringify(plain(lq.toArray))

async function setWindow(lq: unknown, limit: number): Promise<void> {
  const r = (lq as { utils: { setWindow: (w: { offset: number; limit: number }) => true | Promise<void> } }).utils.setWindow({ offset: 0, limit })
  if (r !== true) await r
}

describe(`the shapes the library's client sends (@tanstack/db ${line})`, () => {
  for (const [name, { build, windowed, index }] of Object.entries(SHAPES)) {
    it(name, async () => {
      const r = `refshape-${crypto.randomUUID()}`
      await seed(r)
      const wire: Array<Record<string, unknown>> = []
      const asked: Array<string> = []
      const t = transportFor(r, wire)
      const lq = createLiveQueryCollection({ startSync: true, query: build(onDemand(t, "messages", asked, index), onDemand(t, "files", asked)) })
      const local = await oracle(r, build)
      await lq.preload()
      await waitFor(() => lq.status === "ready" && rowsOf(lq) === rowsOf(local)).catch(() => {})
      expect(lq.status).toBe("ready")
      expect(rowsOf(lq)).toBe(rowsOf(local))
      if (windowed && (index || !db08)) {
        // 0.8.x re-requests a window only through an index (on-demand-contracts.test.ts).
        await setWindow(local, 4)
        await setWindow(lq, 4)
        await waitFor(() => rowsOf(lq) === rowsOf(local)).catch(() => {})
        expect(rowsOf(lq)).toBe(rowsOf(local))
      }

      for (const f of wire) {
        if (f.t === "sub" || f.t === "fetch") expect(refusedRefs(f), `${f.t} on the wire`).toEqual([])
      }
      const expected = SENT[line] ?? SENT["1.0"]!
      expect([...new Set(asked)].sort(), `refs @tanstack/db ${line} asks for`).toEqual(expected[name] ?? [])
      t.close()
    })
  }
})

// A server render reads each request once with `readSyncSnapshot`, and a refused
// read throws, so `preload()` rejects. On 0.9 and later a limited query ordered by
// a nested field also makes a tie request whose `where` holds the nested path.
describe(`a server render of the same shapes (@tanstack/db ${line})`, () => {
  for (const [name, { build, index }] of Object.entries(SHAPES)) {
    it(name, async () => {
      const r = `refssr-${crypto.randomUUID()}`
      await seed(r)
      const stub = stubFor(r) as unknown as { readSyncSnapshot: (req: Parameters<SnapshotRead>[0], request: Request) => ReturnType<SnapshotRead> }
      const reads: Array<Record<string, unknown>> = []
      const t = new SsrSnapshotTransport<TestApi>({
        read: (req) => {
          reads.push(req as Record<string, unknown>)
          return stub.readSyncSnapshot(req, new Request("https://example.com/ssr", { headers: { "x-user": "anon" } }))
        },
      })
      const lq = createLiveQueryCollection({ startSync: true, query: build(onDemand(t, "messages", [], index), onDemand(t, "files", [])) })
      const local = await oracle(r, build)
      await lq.preload()
      expect(lq.status).toBe("ready")
      expect(rowsOf(lq)).toBe(rowsOf(local))
      for (const req of reads) expect(refusedRefs(req), "a read").toEqual([])
    })
  }
})

// The adapter rewrites such a request, and core releases it with the object it
// loaded. These pin that the release still finds the acquisition (so the rows
// and the server sub go), and that two requests rewritten to the same one share it.
describe("a rewritten request is shared and released", () => {
  const nested = (dir: "asc" | "desc") => (m: any) => (q: any) => q.from({ m }).orderBy(({ m }: any) => m.body.length, dir).limit(2)
  const serverSubs = (r: string): Promise<number> =>
    runInDurableObject(stubFor(r), (_i, s) => Number(Array.from(s.storage.sql.exec("SELECT count(*) AS c FROM _sync_subs"))[0]!.c))

  it("its first load is one sub and nothing else, and the release empties the collection and the server", async () => {
    const r = `refrel-${crypto.randomUUID()}`
    await seed(r)
    const wire: Array<Record<string, unknown>> = []
    const t = transportFor(r, wire)
    const messages = onDemand(t, "messages", [])
    const lq = createLiveQueryCollection({ startSync: true, query: nested("asc")(messages) })
    const local = await oracle(r, nested("asc") as Build)
    await lq.preload()
    await waitFor(() => rowsOf(lq) === rowsOf(local))
    await sleep(50)
    // On 0.9 and later core also asks for the boundary ties, with a `where` on the
    // nested path. Widened, that is the request already held: no frame.
    expect(wire.filter((f) => f.t === "sub" || f.t === "fetch").map((f) => f.t)).toEqual(["sub"])
    await lq.cleanup()
    await waitFor(() => messages.size === 0)
    await waitFor(() => wire.some((f) => f.t === "unsub"))
    await sleep(50)
    expect(await serverSubs(r)).toBe(0)
    t.close()
  })

  it("a cursor request under a watch that holds its whole where sends nothing", async () => {
    // Indexed, so growing the window makes core ask for more with a cursor (0.9 and
    // later; 0.8.6 sends none for this shape). The watch was opened with no limit
    // and holds every row, so there is nothing to fetch.
    const r = `refcursor-${crypto.randomUUID()}`
    await seed(r)
    const wire: Array<Record<string, unknown>> = []
    const asked: Array<string> = []
    const t = transportFor(r, wire)
    const lq = createLiveQueryCollection({ startSync: true, query: nested("asc")(onDemand(t, "messages", asked, (m) => m.body.length)) })
    const local = await oracle(r, nested("asc") as Build)
    await lq.preload()
    await setWindow(local, 4)
    await setWindow(lq, 4)
    await waitFor(() => rowsOf(lq) === rowsOf(local))
    await sleep(50)
    expect(asked.some((a) => a.includes(" cursor "))).toBe(!db08)
    expect(wire.filter((f) => f.t === "sub" || f.t === "fetch").map((f) => f.t)).toEqual(["sub"])
    t.close()
  })

  it("two such queries with different orders share one sub and send nothing else, and releasing both empties everything", async () => {
    const r = `refshare-${crypto.randomUUID()}`
    await seed(r)
    const wire: Array<Record<string, unknown>> = []
    const t = transportFor(r, wire)
    const messages = onDemand(t, "messages", [])
    const asc = createLiveQueryCollection({ startSync: true, query: nested("asc")(messages) })
    const desc = createLiveQueryCollection({ startSync: true, query: nested("desc")(messages) })
    await Promise.all([asc.preload(), desc.preload()])
    await waitFor(() => asc.size === 2 && desc.size === 2)
    await sleep(50)
    // Both are rewritten to the same request: one acquisition, one sub, no fetch.
    expect(wire.filter((f) => f.t === "sub" || f.t === "fetch").map((f) => f.t)).toEqual(["sub"])
    await asc.cleanup()
    await sleep(50)
    expect(desc.size).toBe(2) // the other owner still holds the shared load
    await desc.cleanup()
    await waitFor(() => messages.size === 0)
    await waitFor(() => wire.some((f) => f.t === "unsub"))
    await sleep(50)
    expect(await serverSubs(r)).toBe(0)
    t.close()
  })
})

describe("one rule for a column ref, on the client and in the compiler", () => {
  it("columnRefProblem is what the compiler refuses in an orderBy", async () => {
    const { compileSubsetQuery } = await import("../src/server/sql-compiler.ts")
    const CASES: Array<unknown> = [
      { type: "ref", path: ["body"] },
      { type: "ref", path: ["sub", "body"] },
      { type: "ref", path: ["body"], sourceAlias: "body" },
      { type: "ref", path: [["body"]] },
      { type: "ref", path: { 0: "body", length: 1 } },
      { type: "ref", path: ["has space"] },
      { type: "ref", path: [] },
      { type: "func", name: "upper", args: [{ type: "ref", path: ["body"] }] },
      { type: "val", value: "body" },
      null,
    ]
    for (const expression of CASES) {
      let compiled = true
      try {
        compileSubsetQuery("messages", { pk: "id", orderBy: [{ expression, compareOptions: { direction: "asc" } }] })
      } catch {
        compiled = false
      }
      expect(compiled, JSON.stringify(expression)).toBe(columnRefProblem(expression) === null)
    }
  })
})
