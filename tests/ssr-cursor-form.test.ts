import { createLiveQueryCollection, collectionOptions, DbClient, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { SsrSnapshotTransport, type SnapshotRead } from "../src/client/ssr-transport.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"

// WHY (ADR-0012 amendment 2026-11-17, `since`), through the real client:
//   1. A dehydrated cursor that BigInt reads but that is not in the plain form
//      ("05") still resumes. The server drops any other spelling of `since`
//      with no reply, so the client must send the plain form, or the hydrated
//      collection silently stops receiving updates.
//   2. A cursor past the server's current position (the object's storage was
//      recreated, so its positions started again) gets `reset` and a snapshot.
//      It used to get `uptodate`: the client was told it was up to date and
//      kept the old object's rows for ever. Here the rows are rendered from one
//      object and hydrated against another whose log is shorter.

interface Msg {
  id: string
  body: string
}
type Api = { collections: { messages: { __row?: Msg } } }
type Col = { preload: () => Promise<void>; get: (k: string) => Msg | undefined }

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

function makeOptions(transport: WebSocketTransport<Api> | SsrSnapshotTransport<Api>, syncMode?: "eager" | "on-demand") {
  return collectionOptions(
    doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode }) as never,
  ) as never
}

const keepQuery = (col: unknown) =>
  createLiveQueryCollection((q) => q.from({ m: col as never }).where(({ m }: { m: Msg }) => eq(m.body, "keep")))

async function serverRender(room: string, syncMode?: "eager" | "on-demand") {
  const db = new DbClient()
  const col = db.collection(makeOptions(new SsrSnapshotTransport<Api>({ read: makeRead(room) }), syncMode)) as unknown as Col
  if (syncMode === "on-demand") await keepQuery(col).preload()
  else await col.preload()
  return db.dehydrate()
}

const cursorOf = (state: Awaited<ReturnType<typeof serverRender>>): string =>
  (state.collections[0]!.syncMeta as { cursor: string }).cursor

describe("a hydrated cursor reaches the server in its plain form", () => {
  it("eager: a cursor spelt with a leading zero still catches up (update and delete applied)", async () => {
    const room = `form-eager-${crypto.randomUUID()}`
    await sql(room, "INSERT INTO messages(id,body) VALUES('a','v1'),('b','doomed')")
    const state = await serverRender(room)
    const meta = state.collections[0]!.syncMeta as { cursor: string }
    meta.cursor = `0${meta.cursor}`
    await sql(room, "UPDATE messages SET body='v2' WHERE id='a'", "DELETE FROM messages WHERE id='b'")

    const ws = makeWsTransport(room)
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws)) as unknown as Col
    await col.preload()
    await waitFor(() => col.get("a")?.body === "v2" && col.get("b") === undefined)
    ws.close()
  })
})

describe("a since past the server's position gets reset and a snapshot", () => {
  /** Rows rendered from `old`, whose log is longer than `fresh`'s. */
  async function renderedAhead(syncMode?: "eager" | "on-demand") {
    const old = `ahead-old-${crypto.randomUUID()}`
    const fresh = `ahead-new-${crypto.randomUUID()}`
    for (let i = 0; i < 5; i++) await sql(old, `INSERT INTO messages(id,body) VALUES('old${i}','keep')`)
    await sql(fresh, "INSERT INTO messages(id,body) VALUES('new','keep')")
    const state = await serverRender(old, syncMode)
    expect(BigInt(cursorOf(state))).toBeGreaterThan(1n) // `fresh` is at position 1
    return { state, fresh }
  }

  it("eager: the old object's rows are replaced by the new object's", async () => {
    const { state, fresh } = await renderedAhead()
    const ws = makeWsTransport(fresh)
    const db = new DbClient()
    db.hydrate(state as never)
    const col = db.collection(makeOptions(ws)) as unknown as Col & { size: number }
    await col.preload()
    expect(col.get("old0")).toBeDefined() // first paint: the hydrated rows
    await waitFor(() => col.get("new") !== undefined && col.get("old0") === undefined)
    expect(col.size).toBe(1)
    ws.close()
  })

  it("on-demand: the hydration catch-up's reset drops the old rows, and the query reloads from the new object", async () => {
    const { state, fresh } = await renderedAhead("on-demand")
    const ws = makeWsTransport(fresh)
    const db = new DbClient()
    db.hydrate(state as never)
    const colRaw = db.collection(makeOptions(ws, "on-demand"))
    const col = colRaw as unknown as Col
    const kept = keepQuery(colRaw)
    await kept.preload()
    await waitFor(() => col.get("new") !== undefined && col.get("old0") === undefined)
    expect(kept.size).toBe(1)
    ws.close()
  })
})
