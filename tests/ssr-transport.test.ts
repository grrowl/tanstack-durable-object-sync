import * as db from "@tanstack/db"
import { and, BTreeIndex, collectionOptions, createCollection, createLiveQueryCollection, DbClient, eq } from "@tanstack/db"
import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { SsrReadOnlyError, SsrSnapshotTransport, type SnapshotRead } from "../src/client/ssr-transport.ts"
import { settled } from "./settled.ts"

// WHY (ADR-0011 D2): server rendering must run the SAME collection adapter the
// browser runs — one code path, swapped at the transport seam — with one
// snapshot read per subscription and no socket. These pin: eager preload
// materializes the DO's rows; on-demand loadSubset works under a server-side
// live query preload (the upstream SSR fixture's flagship pattern); the
// render's cursor is the durable high-water mark; and any write during SSR
// fails loud as the design error it is.

interface Msg {
  id: string
  body: string
}
interface FileRow {
  id: string
  name: string
}

/** Structural schema Api the branded transport carries, so `doCollectionOptions`
 *  infers Row = Msg for `table: "messages"`. */
type Api = { collections: { messages: { __row?: Msg }; files: { __row?: FileRow } } }

/** Exactly what an SSR worker passes: the DO stub's RPC, as a function. */
function makeRead(room: string): SnapshotRead {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room)) as unknown as {
    readSyncSnapshot: (r: Parameters<SnapshotRead>[0], request: Request) => ReturnType<SnapshotRead>
  }
  // The author closes over the claims-bearing Request; the transport's read
  // contract stays {collection, where, ...} only.
  return (req) => stub.readSyncSnapshot(req, new Request("https://example.com/ssr", { headers: { "x-user": "anon" } }))
}

async function seed(room: string, rows: Array<[string, string]>): Promise<void> {
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const [id, body] of rows) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", id, body)
  })
}

describe("SsrSnapshotTransport (server-side render path, ADR-0011 D2)", () => {
  it("eager: preload materializes the DO's rows and a resumable cursor, no socket", async () => {
    const room = `ssrt-eager-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "hi"],
      ["b", "yo"],
    ])

    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id }),
    )
    await messages.preload()

    expect(messages.size).toBe(2)
    expect(messages.get("a")).toMatchObject({ id: "a", body: "hi" })
    expect(BigInt(transport.appliedCursor)).toBeGreaterThan(0n) // dehydration exports this
  })

  it("on-demand: a server-side live query preload drives loadSubset through one read", async () => {
    const room = `ssrt-od-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "keep"],
      ["b", "drop"],
    ])

    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    )
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    expect(kept.get("a")).toMatchObject({ id: "a", body: "keep" })
    expect(kept.get("b")).toBeUndefined()
    expect(BigInt(transport.appliedCursor)).toBeGreaterThan(0n)
  })

  it("rejects writes during SSR — read-only, fail loud", async () => {
    const room = `ssrt-ro-${crypto.randomUUID()}`
    await seed(room, [["a", "hi"]])

    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id }),
    )
    await messages.preload()

    const tx = messages.insert({ id: "x", body: "nope" })
    await expect(settled(tx)).rejects.toThrow(SsrReadOnlyError)
  })
})

// WHY (ADR-0011 amendment 2026-11-17): a render refuses writes, never reads.
// On-demand loads more than snapshots: a request an earlier watch covers is a
// cursor-less `fetch` (ADR-0023 D2), and core asks for boundary ties, a full
// source, or the rest of an underfilled window with a cursor, inside a first
// load (0.9 and later) or when a window moves. The browser's DO answers each;
// the render must too, with the rows each request asks for. Each test below
// makes the fetched rows visible, so a fetch that read nothing would fail it.
//
// 0.9.0 removed the subset-algebra helpers, so their presence marks 0.8.x,
// whose loader sends no tie, full-source or first-load cursor requests.
const db08 = "isWhereSubset" in db
const lexical = { stringSort: "lexical" } as const

async function exec(room: string, ...statements: Array<string>): Promise<void> {
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const stmt of statements) s.storage.sql.exec(stmt)
  })
}

type ReadReq = Parameters<SnapshotRead>[0]
const isPage = (req: ReadReq): boolean => /"name":"(gt|lt)"/.test(JSON.stringify(req.where ?? null))

/** The real read, recorded; `before` runs ahead of each read (to write between reads). */
function recorded(room: string, before?: (req: ReadReq, done: ReadonlyArray<ReadReq & { rows: number }>) => Promise<void>) {
  const read = makeRead(room)
  const reads: Array<ReadReq & { rows: number }> = []
  const transport = new SsrSnapshotTransport<Api>({
    read: async (req) => {
      await before?.(req, reads)
      const res = await read(req)
      reads.push({ ...req, rows: res.rows.length })
      return res
    },
  })
  return { transport, reads }
}

function onDemand(transport: SsrSnapshotTransport<Api>, indexed = false) {
  // Indexed as core's own warning recommends for orderBy with limit: an index
  // on every order column, built on demand.
  const index = indexed ? { autoIndex: "eager", defaultIndexType: BTreeIndex } : {}
  return createCollection({
    ...doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    ...index,
  } as never) as unknown as ReturnType<typeof createCollection<Msg, string>>
}

const ids = (c: { toArray: Array<Msg> }): Array<string> => c.toArray.map((r) => r.id)

describe("on-demand reads during SSR are answered", () => {
  for (const order of ["same tick", "after the first loaded"] as const) {
    it(`a covered query loads its rows from its fetch and is ready (${order})`, async () => {
      const room = `ssrt-cover-${crypto.randomUUID()}`
      await seed(room, [
        ["a", "keep"],
        ["b", "drop"],
        ["c", "keep"],
      ])
      const messages = onDemand(new SsrSnapshotTransport<Api>({ read: makeRead(room) }), true)
      // The list holds only `a`: the detail's row `c` can come only from its fetch.
      const list = createLiveQueryCollection((q) =>
        q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")).orderBy(({ m }) => m.id, lexical).limit(1),
      )
      const first = list.preload()
      if (order === "after the first loaded") await first
      // Covered by the list's watch: every conjunct of the list's where is one of its.
      const detail = createLiveQueryCollection((q) =>
        q.from({ m: messages }).where(({ m }) => and(eq(m.body, "keep"), eq(m.id, "c"))),
      )
      await Promise.all([first, detail.preload()])
      expect([list.status, detail.status]).toEqual(["ready", "ready"])
      expect(ids(list)).toEqual(["a"])
      expect(ids(detail)).toEqual(["c"])
    })
  }

  it("a covered fetch keeps its own order: an ordered, limited query under another's watch", async () => {
    const room = `ssrt-order-${crypto.randomUUID()}`
    // Inserted in id order, so the first rows by rowid are the last by `id desc`.
    await seed(room, [
      ["a", "keep"],
      ["b", "keep"],
      ["c", "keep"],
      ["d", "keep"],
    ])
    const messages = onDemand(new SsrSnapshotTransport<Api>({ read: makeRead(room) }), true)
    const first = createLiveQueryCollection((q) =>
      q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")).orderBy(({ m }) => m.id, lexical).limit(1),
    )
    await first.preload()
    const last2 = createLiveQueryCollection((q) =>
      q
        .from({ m: messages })
        .where(({ m }) => eq(m.body, "keep"))
        .orderBy(({ m }) => m.id, { direction: "desc", ...lexical })
        .limit(2),
    )
    await last2.preload()
    expect(last2.status).toBe("ready")
    expect(ids(last2)).toEqual(["d", "c"])
  })

  it("a single ordered, limited query under the default sort loads the window the client picks", async () => {
    // The server orders text by bytes, the client by locale (ADR-0023
    // amendment 2026-11-17). The bounded read returns Banana, Date, apple; 0.9
    // and later then ask for the full source, a covered fetch, and correct
    // the window to apple, Banana, cherry. 0.8.x keeps the bounded window.
    const room = `ssrt-top-${crypto.randomUUID()}`
    await seed(room, [
      ["apple", "keep"],
      ["Banana", "keep"],
      ["cherry", "keep"],
      ["Date", "keep"],
    ])
    const messages = onDemand(new SsrSnapshotTransport<Api>({ read: makeRead(room) }))
    const top = createLiveQueryCollection((q) =>
      q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")).orderBy(({ m }) => m.id).limit(3),
    )
    await top.preload()
    expect(top.status).toBe("ready")
    expect(ids(top)).toEqual(db08 ? ["apple", "Banana", "Date"] : ["apple", "Banana", "cherry"])
  })

  it.runIf(!db08)("a boundary tie request loads the tied rows past the limit", async () => {
    const room = `ssrt-tie-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "b1"],
      ["b", "b2"],
      ["c", "b2"],
      ["d", "b3"],
    ])
    const { transport, reads } = recorded(room)
    const messages = onDemand(transport, true)
    const top = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, lexical).limit(2))
    await top.preload()
    expect(top.status).toBe("ready")
    expect(ids(top)).toEqual(["a", "b"])
    expect(reads.length).toBeGreaterThan(1) // the tie request reached the reader
    expect(messages.get("c")).toMatchObject({ body: "b2" }) // only the tie request reads `c`
  })

  it.runIf(!db08)("a cursor request inside a first load (an underfilled window) is answered", async () => {
    // With an index and an order core can express as a cursor, core asks for
    // the rest of an underfilled window with a cursor on its first load. `d`
    // joins between the first read and the cursor's page read, so only that
    // page can bring it.
    const room = `ssrt-under-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "keep"],
      ["b", "keep"],
      ["c", "keep"],
      ["x", "drop"],
    ])
    const { transport, reads } = recorded(room, async (req) => {
      if (isPage(req) && !reads.some(isPage)) await exec(room, "INSERT INTO messages(id,body) VALUES('d','keep')")
    })
    const messages = onDemand(transport, true)
    const top = createLiveQueryCollection((q) =>
      q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")).orderBy(({ m }) => m.id, lexical).limit(10),
    )
    await top.preload()
    expect(top.status).toBe("ready")
    expect(reads.some(isPage)).toBe(true)
    expect(ids(top)).toEqual(["a", "b", "c", "d"])
  })

  it.runIf(!db08)("a row moved past the boundary between the ties read and the page read is written once", async () => {
    // A render reads the two halves of a cursor request at two positions. `z`
    // joins at the boundary value before the ties read and moves past it
    // before the page read, so both return it; the page must not insert it
    // twice (a duplicate sync insert fails the load).
    const room = `ssrt-race-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "b1"],
      ["b", "b2"],
      ["c", "b3"],
    ])
    const { transport } = recorded(room, async (req, done) => {
      const where = JSON.stringify(req.where ?? null)
      const tieReads = done.filter((r) => /"name":"eq"/.test(JSON.stringify(r.where ?? null))).length
      if (!isPage(req) && where.includes('"eq"') && tieReads === 1) {
        await exec(room, "INSERT INTO messages(id,body) VALUES('z','b3')") // before the cursor's ties read
      }
      if (isPage(req)) await exec(room, "UPDATE messages SET body='b5' WHERE id='z'")
    })
    const messages = onDemand(transport, true)
    const top = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, lexical).limit(10))
    await top.preload()
    expect(top.status).toBe("ready")
    // The first copy wins (the ties read); hydration's catch-up brings b5.
    expect(top.toArray.map((r) => `${r.id}=${r.body}`)).toEqual(["a=b1", "b=b2", "c=b3", "z=b3"])
  })

  it("a window that moves during the render loads its next rows", async () => {
    const room = `ssrt-window-${crypto.randomUUID()}`
    const rows = Array.from({ length: 20 }, (_, i) => String(i + 1).padStart(2, "0"))
    await seed(room, rows.map((n) => [`m${n}`, n] as [string, string]))
    const { transport, reads } = recorded(room)
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    )
    // As in on-demand-scenarios: 0.8.x grows a default-sort indexed window by a
    // cursor request (load more); 0.9 and later grow a lexical one by covered
    // tie and prefix fetches.
    const sort = db08 ? ({ direction: "desc" } as const) : ({ direction: "desc", ...lexical } as const)
    if (db08) messages.createIndex((m) => m.body, { indexType: BTreeIndex })
    else messages.createIndex((m) => m.body, { indexType: BTreeIndex, options: { compareOptions: lexical } })
    const win = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, sort).limit(5))
    await win.preload()
    await (win as unknown as { utils: { setWindow: (w: { offset?: number; limit?: number }) => true | Promise<void> } }).utils.setWindow({
      offset: 0,
      limit: 10,
    })
    expect(win.status).toBe("ready")
    expect(win.toArray.map((r) => r.body)).toEqual(rows.slice(10).reverse())
    if (db08) expect(reads.some(isPage)).toBe(true) // load more went through a cursor request
  })

  it("two collections on one transport export one cursor: the MIN across both", async () => {
    const room = `ssrt-two-${crypto.randomUUID()}`
    await seed(room, [["a", "keep"]])
    await exec(room, "INSERT INTO files(id,name) VALUES('f1','x')")
    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const client = new DbClient()
    const messages = client.collection(
      collectionOptions(doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }) as never) as never,
    )
    const files = client.collection(
      collectionOptions(
        doCollectionOptions({ transport, table: "files", getKey: (r) => r.id, syncMode: "on-demand" }) as never,
      ) as never,
    )
    const kept = createLiveQueryCollection((q) => q.from({ m: messages as never }).where(({ m }: { m: Msg }) => eq(m.body, "keep")))
    await kept.preload()
    const first = transport.appliedCursor
    await exec(room, "INSERT INTO files(id,name) VALUES('f2','x')") // the second read is at a later position
    const named = createLiveQueryCollection((q) =>
      q.from({ f: files as never }).where(({ f }: { f: FileRow }) => eq(f.name, "x")),
    )
    await named.preload()
    expect((named.toArray as Array<FileRow>).map((r) => r.id).sort()).toEqual(["f1", "f2"])
    const state = client.dehydrate()
    expect(state.collections.map((c) => [c.collectionId, (c.syncMeta as { cursor: string }).cursor]).sort()).toEqual([
      ["files", first],
      ["messages", first],
    ])
  })
})

// The transport alone: what each fetch shape asks the reader.
describe("SsrSnapshotTransport.fetch", () => {
  const none = () => {}
  const handler = { onSnap: none, onSnapEnd: none, onDelta: none, onUptodate: none, onReset: none }

  it("answers a cursor-less fetch with one read; its position joins the render's min cursor", async () => {
    const reads: Array<ReadReq> = []
    const cursors = ["9", "5"] // the subscribe reads at 9, the fetch at 5
    const transport = new SsrSnapshotTransport<Api>({
      read: async (req) => {
        reads.push(req)
        return { rows: [{ id: `r${reads.length}`, body: "keep" }], cursor: cursors[reads.length - 1]! }
      },
    })
    await transport.subscribe("s", "messages", handler)
    expect(transport.appliedCursor).toBe("9")
    const where = eq({ __ref: true } as never, "keep") // a class instance: must reach the reader clone-safe
    const rows = await transport.fetch({ t: "fetch", fetchId: "f", collection: "messages", where: where as never, limit: 3 })
    expect(rows).toEqual([{ id: "r2", body: "keep" }])
    expect(reads[1]).toMatchObject({ collection: "messages", limit: 3 })
    expect(Object.getPrototypeOf(reads[1]!.where)).toBe(Object.prototype)
    // Its rows dehydrate with the rest, so the resume point must not be above
    // the position they were read at (ADR-0011 D2/D3: MIN across reads).
    expect(transport.appliedCursor).toBe("5")
  })

  it("answers a cursor fetch as the server does: the ties unbounded, then the page bounded; both join the MIN", async () => {
    const reads: Array<ReadReq> = []
    const cursors = ["7", "4"]
    const transport = new SsrSnapshotTransport<Api>({
      read: async (req) => {
        reads.push(req)
        return { rows: [{ id: `r${reads.length}`, body: "keep" }], cursor: cursors[reads.length - 1]! }
      },
    })
    const w = { type: "func", name: "eq", args: [{ type: "ref", path: ["body"] }, { type: "val", value: "keep" }] }
    const current = { type: "func", name: "eq", args: [{ type: "ref", path: ["id"] }, { type: "val", value: "m" }] }
    const from = { type: "func", name: "gt", args: [{ type: "ref", path: ["id"] }, { type: "val", value: "m" }] }
    const orderBy = [{ expression: { type: "ref", path: ["id"] }, compareOptions: { direction: "asc" } }]
    const rows = await transport.fetch({
      t: "fetch",
      fetchId: "f",
      collection: "messages",
      where: w as never,
      cursor: { whereFrom: from as never, whereCurrent: current as never },
      orderBy: orderBy as never,
      limit: 3,
    })
    expect(rows).toEqual([
      { id: "r1", body: "keep" },
      { id: "r2", body: "keep" },
    ])
    expect(reads).toEqual([
      { collection: "messages", where: { type: "func", name: "and", args: [w, current] }, orderBy, limit: undefined },
      { collection: "messages", where: { type: "func", name: "and", args: [w, from] }, orderBy, limit: 3 },
    ])
    expect(transport.appliedCursor).toBe("4")
  })

  it("refuses a cursor with a missing half, as the server does, and reads nothing", async () => {
    // The server refuses it (`#handleFetch`, "malformed cursor"): the ties read
    // would be an unbounded scan. A render refuses it loud, like any refused read.
    let reads = 0
    const transport = new SsrSnapshotTransport<Api>({
      read: async () => {
        reads++
        return { rows: [{ id: "a", body: "keep" }], cursor: "1" }
      },
    })
    const from = { type: "func", name: "gt", args: [{ type: "ref", path: ["id"] }, { type: "val", value: "m" }] }
    for (const cursor of [{ whereFrom: from }, { whereCurrent: from }, {}]) {
      const p = transport.fetch({ t: "fetch", fetchId: "f", collection: "messages", cursor: cursor as never, limit: 3 })
      await expect(p).rejects.toThrow(/malformed cursor/)
    }
    expect(reads).toBe(0)
  })

  it("writes still fail loud", async () => {
    const transport = new SsrSnapshotTransport<Api>({ read: async () => ({ rows: [], cursor: "1" }) })
    await expect(transport.sendMut({ t: "mut", txId: "t", collection: "messages", ops: [] })).rejects.toThrow(SsrReadOnlyError)
  })
})
