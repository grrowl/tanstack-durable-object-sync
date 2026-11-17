import { and, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
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

/** Structural schema Api the branded transport carries, so `doCollectionOptions`
 *  infers Row = Msg for `table: "messages"`. */
type Api = { collections: { messages: { __row?: Msg } } }

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

// WHY (ADR-0011 amendment 2026-11-17): an on-demand request that an earlier
// watch covers loads its rows with a cursor-less `fetch` (ADR-0023 D2), and the
// render must answer it, as the browser's DO does. Two overlapping queries on
// one page (a list and a detail row of the same table) is ordinary code, and
// on 0.9 and later core sends covered requests for a single ordered, limited
// query too. Only a cursor fetch (load more) stays refused: the snapshot read
// has no cursor, so it cannot answer one.
describe("covered on-demand requests during SSR", () => {
  async function render(room: string, order: "same tick" | "after the first loaded") {
    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    )
    const list = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    const first = list.preload()
    if (order === "after the first loaded") await first
    // Covered by the list's watch: every conjunct of the list's where is one of its.
    const detail = createLiveQueryCollection((q) =>
      q.from({ m: messages }).where(({ m }) => and(eq(m.body, "keep"), eq(m.id, "c"))),
    )
    await Promise.all([first, detail.preload()])
    return { list, detail }
  }

  for (const order of ["same tick", "after the first loaded"] as const) {
    it(`a covered query loads its rows and is ready (${order})`, async () => {
      const room = `ssrt-cover-${crypto.randomUUID()}`
      await seed(room, [
        ["a", "keep"],
        ["b", "drop"],
        ["c", "keep"],
      ])
      const { list, detail } = await render(room, order)
      expect(list.status).toBe("ready")
      expect(detail.status).toBe("ready")
      expect(detail.toArray.map((r) => r.id)).toEqual(["c"])
      expect(list.toArray.map((r) => r.id).sort()).toEqual(["a", "c"])
    })
  }

  it("a single ordered, limited query loads (core may send it covered requests)", async () => {
    const room = `ssrt-top-${crypto.randomUUID()}`
    await seed(room, [
      ["c", "keep"],
      ["a", "keep"],
      ["b", "keep"],
      ["d", "drop"],
    ])
    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    )
    const top = createLiveQueryCollection((q) =>
      q
        .from({ m: messages })
        .where(({ m }) => eq(m.body, "keep"))
        .orderBy(({ m }) => m.id)
        .limit(2),
    )
    await top.preload()
    expect(top.status).toBe("ready")
    expect(top.toArray.map((r) => r.id)).toEqual(["a", "b"])
  })

  it("answers a cursor-less fetch with one read; its position joins the render's min cursor", async () => {
    const reads: Array<Parameters<SnapshotRead>[0]> = []
    const cursors = ["9", "5"] // the subscribe reads at 9, the fetch at 5
    const transport = new SsrSnapshotTransport<Api>({
      read: async (req) => {
        reads.push(req)
        return { rows: [{ id: `r${reads.length}`, body: "keep" }], cursor: cursors[reads.length - 1]! }
      },
    })
    const none = () => {}
    await transport.subscribe("s", "messages", { onSnap: none, onSnapEnd: none, onDelta: none, onUptodate: none, onReset: none })
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

  it("a cursor fetch (load more) still fails loud, and reads nothing", async () => {
    let reads = 0
    const transport = new SsrSnapshotTransport<Api>({
      read: async () => {
        reads++
        return { rows: [], cursor: "1" }
      },
    })
    const cursorWhere = eq({ __ref: true } as never, "x") as never
    const p = transport.fetch({
      t: "fetch",
      fetchId: "f",
      collection: "messages",
      cursor: { whereFrom: cursorWhere, whereCurrent: cursorWhere },
      limit: 3,
    })
    await expect(p).rejects.toThrow(SsrReadOnlyError)
    await expect(p).rejects.toThrow(/^cursor fetch during SSR/)
    expect(reads).toBe(0)
    expect(transport.hasPosition).toBe(false)
  })
})
