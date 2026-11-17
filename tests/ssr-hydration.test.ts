import * as tanstackDb from "@tanstack/db"
import { and, BTreeIndex, createLiveQueryCollection, DbClient, collectionOptions, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { SsrSnapshotTransport, type SnapshotRead } from "../src/client/ssr-transport.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { ClientFrame } from "../src/wire/frames.ts"

// WHY (ADR-0011, end to end): the whole point of SSR support is one specific
// promise — the browser renders the worker's dehydrated rows IMMEDIATELY, then
// CONVERGES to the DO's current truth without wedging, flashing empty, or
// stranding a deleted row. Rows ride TanStack's DehydratedDbState; our cursor
// rides the opaque syncMeta. These tests run the REAL upstream DbClient
// (released ≥0.8.5 — PR #1564 as merged) on both sides: a server-side DbClient + snapshot
// transport renders and dehydrates; a client-side DbClient hydrates over a
// WebSocket transport; writes land on the DO between the two. Convergence —
// updates applied, deletes applied, no DuplicateKeySyncError, even with no
// resume point — is the contract.

interface Msg {
  id: string
  body: string
}

/** Structural schema Api the branded transports carry, so `doCollectionOptions`
 *  infers Row = Msg for `table: "messages"`. */
type Api = { collections: { messages: { __row?: Msg } } }

function makeRead(room: string): SnapshotRead {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room)) as unknown as {
    readSyncSnapshot: (r: Parameters<SnapshotRead>[0], request: Request) => ReturnType<SnapshotRead>
  }
  return (req) => stub.readSyncSnapshot(req, new Request("https://example.com/ssr", { headers: { "x-user": "anon" } }))
}

function makeWsTransport(room: string): WebSocketTransport<Api> {
  return new WebSocketTransport<Api>({
    url: `https://example.com/sync/${room}`,
    reconnectDelay: 20,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
}

async function sql(room: string, ...statements: Array<string>): Promise<void> {
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const stmt of statements) s.storage.sql.exec(stmt)
  })
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

/** The branded options DbClient wants, around our adapter. One per "process". */
function makeOptions(
  transport: WebSocketTransport<Api> | SsrSnapshotTransport<Api>,
  syncMode?: "eager" | "on-demand",
  where?: unknown,
) {
  return collectionOptions(
    doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode, where }) as never,
  ) as never
}

/** Server render: per-request DbClient + snapshot transport → dehydrated state. */
async function serverRender(room: string, syncMode?: "eager" | "on-demand", where?: unknown) {
  const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
  const db = new DbClient()
  const col = db.collection(makeOptions(transport, syncMode, where)) as unknown as {
    preload: () => Promise<void>
    get: (k: string) => Msg | undefined
  }
  if (syncMode === "on-demand") {
    const kept = createLiveQueryCollection((q) =>
      q.from({ m: col as never }).where(({ m }: { m: Msg }) => eq(m.body, "keep")),
    )
    await kept.preload()
  } else {
    await col.preload()
  }
  return db.dehydrate()
}

const whereEq = (field: string, value: unknown): unknown => ({
  type: "func",
  name: "eq",
  args: [
    { type: "ref", path: [field] },
    { type: "val", value },
  ],
})

describe("SSR round trip: dehydrate on the worker, hydrate + converge in the browser", () => {
  it("eager: hydrated rows render immediately, then converge (update applied, delete applied)", async () => {
    const room = `rt-eager-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','v1'),('b','doomed'),('c','calm')")

    const state = await serverRender(room)
    const chunk = state.collections[0]!
    expect(chunk.collectionId).toBe("messages")
    expect(chunk.rows).toHaveLength(3)
    expect(chunk.syncMeta).toMatchObject({ v: 1 })
    const dehydratedCursor = (chunk.syncMeta as { cursor: string }).cursor
    expect(BigInt(dehydratedCursor)).toBeGreaterThan(0n)

    // While the HTML is in flight, the DO moves on: a changes, b dies.
    await sql(room, "UPDATE messages SET body='v2' WHERE id='a'", "DELETE FROM messages WHERE id='b'")

    // Browser: hydrate, then go live.
    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws)) as unknown as {
      preload: () => Promise<void>
      get: (k: string) => Msg | undefined
      size: number
    }
    await col.preload()

    // First paint: the dehydrated rows, stale and ALL present — ready never
    // waited for the socket (stale-while-revalidate, ADR-0011 D3).
    expect(col.get("a")).toMatchObject({ body: "v1" })
    expect(col.get("b")).toBeDefined()

    // Convergence: catch-up applies the update AND the tombstone.
    await waitFor(() => col.get("a")?.body === "v2" && col.get("b") === undefined)
    expect(col.get("c")).toMatchObject({ body: "calm" })
    expect(BigInt(ws.appliedCursor)).toBeGreaterThan(BigInt(dehydratedCursor))
    ws.close()
  })

  it("eager: a first dial that fails leaves the hydrated rows usable, and no error is left once it converges", async () => {
    // A hydrated collection is ready at once (stale-while-revalidate), so a
    // dial that fails is not a failed load: the transport retries, and the
    // catch-up converges the rows. Reporting it as one left the collection in
    // error for good. The catch-up brings no snapshot to clear it before
    // @tanstack/db 1.0, and from 1.0 `fail` ends the sync run (ADR-0028).
    const room = `rt-offline-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','v1'),('b','doomed')")
    const state = await serverRender(room)
    await sql(room, "UPDATE messages SET body='v2' WHERE id='a'", "DELETE FROM messages WHERE id='b'")

    let down = true
    let dials = 0
    const ws = new WebSocketTransport<Api>({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 5,
      open: async () => {
        dials++
        if (down) throw new Error("dial failed: server unreachable")
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const sock = res.webSocket
        if (!sock) throw new Error("no webSocket")
        sock.accept()
        return sock as unknown as WebSocketLike
      },
    })
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws)) as unknown as {
      preload: () => Promise<void>
      get: (k: string) => Msg | undefined
      status: string
      health?: { hasErrors: boolean }
    }
    await col.preload()
    expect(col.get("a")).toMatchObject({ body: "v1" })
    await waitFor(() => dials > 1) // the first dial failed and the transport is retrying
    expect(col.status).toBe("ready")

    down = false
    await waitFor(() => col.get("a")?.body === "v2" && col.get("b") === undefined)
    expect(col.status).toBe("ready")
    expect(col.health?.hasErrors ?? false).toBe(false) // `health` exists from 1.0
    ws.close()
  })

  it("eager with NO resume point (pruned log → cursor 0): snapshot reconcile removes the dead row", async () => {
    const room = `rt-zero-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','hi'),('b','doomed')")
    // Retention pruned everything; nothing was ever drained. High-water is
    // honestly 0 — there is no resume point.
    await sql(room, "DELETE FROM _sync_changes")

    const state = await serverRender(room)
    expect((state.collections[0]!.syncMeta as { cursor: string }).cursor).toBe("0")

    await sql(room, "DELETE FROM messages WHERE id='b'") // dies while HTML is in flight

    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws)) as unknown as {
      preload: () => Promise<void>
      get: (k: string) => Msg | undefined
    }
    await col.preload()
    expect(col.get("b")).toBeDefined() // stale first paint, not a flash-to-empty

    // The fresh snapshot is authoritative SET semantics: b is reconciled away.
    await waitFor(() => col.get("b") === undefined)
    expect(col.get("a")).toMatchObject({ body: "hi" })
    ws.close()
  })

  it("eager with no resume point and a WIPED table: the empty snapshot still reconciles everything away", async () => {
    const room = `rt-wipe-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','hi'),('b','yo')")
    await sql(room, "DELETE FROM _sync_changes") // no resume point

    const state = await serverRender(room)
    expect(state.collections[0]!.rows).toHaveLength(2)

    // Everything dies while the HTML is in flight: the catch-up snapshot has
    // ZERO rows — which must still count as the authoritative (empty) set.
    await sql(room, "DELETE FROM messages")

    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws)) as unknown as {
      preload: () => Promise<void>
      size: number
    }
    await col.preload()
    expect(col.size).toBe(2) // stale first paint
    await waitFor(() => col.size === 0) // honest convergence, not stale-forever
    ws.close()
  })

  it("a FUTURE-VERSIONED syncMeta throws from hydrate, yet the collection still converges", async () => {
    const room = `rt-vskew-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','hi'),('b','doomed')")

    const state = await serverRender(room)
    // A newer serializer wrote meta this client can't read.
    ;(state.collections[0]! as { syncMeta: unknown }).syncMeta = { v: 99, cursor: "999" }
    await sql(room, "DELETE FROM messages WHERE id='b'") // dies while in flight

    const ws = makeWsTransport(room)
    const db = new DbClient()
    const col = db.collection(makeOptions(ws)) as unknown as {
      preload: () => Promise<void>
      get: (k: string) => Msg | undefined
    }
    expect(() => db.hydrate(state as never)).toThrow(/unrecognized sync meta/) // loud...
    await col.preload()
    expect(col.get("b")).toBeDefined() // rows landed regardless (no upstream veto)
    await waitFor(() => col.get("b") === undefined) // ...but SAFE: reconcile converges
    expect(col.get("a")).toMatchObject({ body: "hi" })
    ws.close()
  })

  it("a CHANGED eager where between render and hydrate downgrades to snapshot reconcile", async () => {
    const room = `rt-where-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','keep'),('b','other')")

    // Rendered under where body='keep' — only 'a' is dehydrated, and the
    // cursor is fingerprinted to THAT filter.
    const state = await serverRender(room, undefined, whereEq("body", "keep"))
    expect(state.collections[0]!.rows.map((r) => r.key)).toEqual(["a"])

    // The browser ships a different filter (deploy skew). 'a' never changes
    // after the render, so a since-catch-up would NEVER remove it — the
    // foreign cursor must be refused and the snapshot reconciled instead.
    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws, undefined, whereEq("body", "other"))) as unknown as {
      preload: () => Promise<void>
      get: (k: string) => Msg | undefined
    }
    await col.preload()
    await waitFor(() => col.get("b") !== undefined && col.get("a") === undefined)
    ws.close()
  })

  it("on-demand: transient catch-up converges hydrated subsets, then leaves (no eager leak)", async () => {
    const room = `rt-od-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','keep'),('b','keep'),('c','drop')")

    const state = await serverRender(room, "on-demand")
    // Dehydrated = the loaded subset only.
    expect(state.collections[0]!.rows.map((r) => (r.value as unknown as Msg).id).sort()).toEqual(["a", "b"])

    // While in flight: b dies, d joins the subset.
    await sql(room, "DELETE FROM messages WHERE id='b'", "INSERT INTO messages(id,body) VALUES('d','keep')")

    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const colRaw = db.collection(makeOptions(ws, "on-demand"))
    const col = colRaw as unknown as { get: (k: string) => Msg | undefined }
    const kept = createLiveQueryCollection((q) =>
      q.from({ m: colRaw as never }).where(({ m }: { m: Msg }) => eq(m.body, "keep")),
    )
    await kept.preload()

    // Convergence across hydrated rows: the tombstone for b (only the
    // transient catch-up can deliver it — b is gone from every fresh subset
    // snapshot) and the new subset member d.
    await waitFor(() => col.get("b") === undefined && col.get("d") !== undefined)
    expect(col.get("a")).toMatchObject({ body: "keep" })

    // The catch-up sub must be GONE: an unfiltered leftover would stream
    // out-of-subset rows. Write e (outside) then f (inside) through a real
    // mut; f's arrival is the ordering sentinel proving e had its chance.
    const writer = makeWsTransport(room)
    const mut = (id: string, body: string): Extract<ClientFrame, { t: "mut" }> => ({
      t: "mut",
      txId: `tx-${id}-${crypto.randomUUID()}`,
      collection: "messages",
      ops: [{ type: "insert", key: id, cols: { id, body } }],
    })
    await writer.sendMut(mut("e", "drop"))
    await writer.sendMut(mut("f", "keep"))
    await waitFor(() => col.get("f") !== undefined)
    expect(col.get("e")).toBeUndefined()
    expect(col.get("c")).toBeUndefined() // never loaded; on-demand stayed on-demand
    writer.close()
    ws.close()
  })

  it("on-demand with a covered query: its rows dehydrate under the min cursor, and both queries go live", async () => {
    // A list and a detail row of one table: the detail's request is covered by
    // the list's watch, so the render answers it with a fetch (ADR-0023 D2,
    // ADR-0011 amendment 2026-11-17). Its row z is written AFTER the list's
    // read, so the fetch reads at a later position than the list did. The
    // exported cursor must stay at the list's position (D3: MIN across reads),
    // or the catch-up would skip changes the list's rows never saw.
    const room = `rt-od-cover-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','keep'),('b','keep'),('c','drop')")

    const keep = (m: Msg) => eq(m.body, "keep")
    const isZ = (m: Msg) => and(eq(m.body, "keep"), eq(m.id, "z"))

    const ssr = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const server = new DbClient()
    const serverCol = server.collection(makeOptions(ssr, "on-demand"))
    const serverList = createLiveQueryCollection((q) => q.from({ m: serverCol as never }).where(({ m }: { m: Msg }) => keep(m)))
    await serverList.preload()
    const listCursor = ssr.appliedCursor
    await sql(room, "INSERT INTO messages(id,body) VALUES('z','keep')")
    const serverDetail = createLiveQueryCollection((q) => q.from({ m: serverCol as never }).where(({ m }: { m: Msg }) => isZ(m)))
    await serverDetail.preload()
    expect(serverDetail.status).toBe("ready")
    const state = server.dehydrate()

    const chunk = state.collections[0]!
    expect(chunk.rows.map((r) => (r.value as unknown as Msg).id).sort()).toEqual(["a", "b", "z"])
    expect((chunk.syncMeta as { cursor: string }).cursor).toBe(listCursor)

    // While in flight: a leaves the list, z dies, d joins the list.
    await sql(
      room,
      "UPDATE messages SET body='drop' WHERE id='a'",
      "DELETE FROM messages WHERE id='z'",
      "INSERT INTO messages(id,body) VALUES('d','keep')",
    )

    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const colRaw = db.collection(makeOptions(ws, "on-demand"))
    const col = colRaw as unknown as { get: (k: string) => Msg | undefined }
    const list = createLiveQueryCollection((q) => q.from({ m: colRaw as never }).where(({ m }: { m: Msg }) => keep(m)))
    const detail = createLiveQueryCollection((q) => q.from({ m: colRaw as never }).where(({ m }: { m: Msg }) => isZ(m)))
    await Promise.all([list.preload(), detail.preload()])

    const ids = (c: { toArray: Array<unknown> }) => (c.toArray as Array<Msg>).map((r) => r.id).sort()
    await waitFor(() => ids(list).join() === "b,d" && col.get("z") === undefined && col.get("a")?.body === "drop")
    expect(ids(detail)).toEqual([])

    // Both are live on the WebSocket: z comes back through a real mutation.
    const writer = makeWsTransport(room)
    await writer.sendMut({
      t: "mut",
      txId: `tx-z-${crypto.randomUUID()}`,
      collection: "messages",
      ops: [{ type: "insert", key: "z", cols: { id: "z", body: "keep" } }],
    })
    await waitFor(() => ids(detail).join() === "z")
    expect(ids(list)).toEqual(["b", "d", "z"])
    expect([list.status, detail.status]).toEqual(["ready", "ready"])
    writer.close()
    ws.close()
  })

  // 0.8.x sends no cursor request inside a first load (0.9.0 removed the
  // subset-algebra helpers, so their presence marks 0.8.x).
  it.runIf(!("isWhereSubset" in tanstackDb))(
    "on-demand with a cursor request read at two positions: what changed between them converges on hydration",
    async () => {
      // A render answers a cursor request with two reads, the boundary ties and
      // then the page, at two positions (ADR-0011 amendment 2026-11-17). Between
      // them, `a` changes and `x` joins AT the boundary value, so neither read
      // returns `x`: the render misses it. Both reads join the MIN cursor, so
      // the hydration catch-up replays both changes.
      const room = `rt-od-two-${crypto.randomUUID()}`
      await sql(room, "INSERT INTO messages(id,body) VALUES('a','b1'),('b','b2'),('c','b3')")
      const isPage = (where: unknown) => /"name":"(gt|lt)"/.test(JSON.stringify(where ?? null))
      const read = makeRead(room)
      let firstCursor: string | null = null
      const ssr = new SsrSnapshotTransport<Api>({
        read: async (req) => {
          if (isPage(req.where) && firstCursor === null) {
            firstCursor = ssr.appliedCursor
            await sql(room, "UPDATE messages SET body='b0' WHERE id='a'", "INSERT INTO messages(id,body) VALUES('x','b3')")
          }
          return read(req)
        },
      })
      const lexical = { stringSort: "lexical" } as const
      const indexed = (t: SsrSnapshotTransport<Api> | WebSocketTransport<Api>) =>
        collectionOptions({
          ...(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }) as object),
          autoIndex: "eager",
          defaultIndexType: BTreeIndex,
        } as never) as never
      const top = (col: unknown) =>
        createLiveQueryCollection((q) =>
          q
            .from({ m: col as never })
            .orderBy(({ m }: { m: Msg }) => m.body, lexical)
            .limit(10),
        )

      const server = new DbClient()
      const serverTop = top(server.collection(indexed(ssr)))
      await serverTop.preload()
      expect(firstCursor).not.toBeNull() // the cursor request was read
      const rendered = (serverTop.toArray as Array<Msg>).map((r) => `${r.id}=${r.body}`)
      expect(rendered).toEqual(["a=b1", "b=b2", "c=b3"]) // `a` from before, no `x`
      const state = server.dehydrate()
      expect((state.collections[0]!.syncMeta as { cursor: string }).cursor).toBe(firstCursor)

      const ws = makeWsTransport(room)
      const db = new DbClient()
      db.hydrate(state as never)
      const live = top(db.collection(indexed(ws)))
      await live.preload()
      const rows = () => (live.toArray as Array<Msg>).map((r) => `${r.id}=${r.body}`).join(" ")
      await waitFor(() => rows() === "a=b0 b=b2 c=b3 x=b3")
      expect(live.status).toBe("ready")
      ws.close()
    },
  )
})
