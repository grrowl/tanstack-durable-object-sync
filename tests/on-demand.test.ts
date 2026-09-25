import { createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { ClientFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: on-demand loads ONLY the subsets live queries request, instead of
// syncing the whole collection. These pin: distinct `where`s become distinct
// refcounted server subs (shared + released correctly), a live query loads only
// its subset (other rows stay absent), and a write outside every loaded subset
// is confirmed without stranding an optimistic phantom.

interface Msg {
  id: string
  body: string
}

const whereFunc = (name: string, field: string, value: unknown): unknown => ({
  type: "func",
  name,
  args: [
    { type: "ref", path: [field] },
    { type: "val", value },
  ],
})
const whereEq = (field: string, value: unknown): unknown => whereFunc("eq", field, value)

// `present` models rows already in the collection (synced or from a prior
// snapshot), so insert-if-absent in loadMore can be exercised.
const spyControls = (present: Set<string> = new Set()) => {
  const calls: Array<[string, ...Array<unknown>]> = []
  return {
    calls,
    controls: {
      collection: { get: (key: string) => (present.has(key) ? { id: key } : undefined) },
      begin: () => calls.push(["begin"]),
      write: (m: unknown) => calls.push(["write", m]),
      commit: () => calls.push(["commit"]),
      markReady: () => calls.push(["markReady"]),
      truncate: () => calls.push(["truncate"]),
    },
  }
}

type FetchFrame = Extract<ClientFrame, { t: "fetch" }>

// A fake transport that records sub/unsub and immediately completes each
// subscription's (empty) snapshot, for deterministic refcount tests. `fetch`
// records the issued frame and returns the canned `page` rows — cursor
// load-more is one atomic fetch carrying both `ties` and `where`.
function fakeTransport(page: Array<unknown> = []) {
  const subs: Array<{ subId: string; where: unknown }> = []
  const unsubs: Array<string> = []
  const fetches: Array<FetchFrame> = []
  const transport = {
    connect: async () => {},
    subscribe: async (subId: string, _table: string, handler: SubHandler, where?: unknown) => {
      subs.push({ subId, where })
      handler.onSnapEnd() // empty snapshot -> resolves loadSubset
    },
    unsubscribe: (subId: string) => unsubs.push(subId),
    fetch: async (frame: FetchFrame) => {
      fetches.push(frame)
      return page
    },
    sendMut: async () => ({}),
    close: () => {},
  } as unknown as WebSocketTransport<TestApi>
  return { subs, unsubs, fetches, transport }
}

interface LoadOpts {
  where?: unknown
  orderBy?: unknown
  limit?: number
  cursor?: { whereFrom: unknown; whereCurrent: unknown; lastKey?: unknown }
}
type OnDemand = {
  loadSubset: (o: LoadOpts) => true | Promise<void>
  unloadSubset: (o: LoadOpts) => void
}
const startOnDemand = (transport: WebSocketTransport<TestApi>, present?: Set<string>) => {
  const { calls, controls } = spyControls(present)
  const adapter = doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" })
  const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync(controls)
  return { calls, res }
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("on-demand loadSubset (M11) — refcounting", () => {
  it("shares one server sub per distinct where and releases it on the last unload", async () => {
    const { subs, unsubs, transport } = fakeTransport()
    const { res } = startOnDemand(transport)
    const w = { where: whereEq("body", "x") }

    await res.loadSubset(w)
    await res.loadSubset(w) // same where -> no new sub
    expect(subs.length).toBe(1)

    res.unloadSubset(w) // refs 2 -> 1
    expect(unsubs.length).toBe(0)
    res.unloadSubset(w) // refs 1 -> 0 -> unsubscribe
    expect(unsubs).toEqual([subs[0]!.subId])
  })

  it("creates distinct subs for distinct wheres", async () => {
    const { subs, transport } = fakeTransport()
    const { res } = startOnDemand(transport)
    await res.loadSubset({ where: whereEq("body", "x") })
    await res.loadSubset({ where: whereEq("body", "y") })
    expect(subs.map((s) => s.where)).toEqual([whereEq("body", "x"), whereEq("body", "y")])
    expect(subs.length).toBe(2)
  })

  it("resolves loadSubset on a rejected sub (reset, no snap-end) instead of hanging", async () => {
    // An unsupported predicate / unknown collection makes the server send
    // `reset` with NO `snap-end`. The load promise must still settle, or the
    // live query's preload() hangs forever.
    const rejectingTransport = {
      connect: async () => {},
      subscribe: async (_subId: string, _table: string, handler: SubHandler) => {
        handler.onReset() // rejection terminal — no snapshot follows
      },
      unsubscribe: () => {},
      sendMut: async () => ({}),
      close: () => {},
    } as unknown as WebSocketTransport<TestApi>
    const { res } = startOnDemand(rejectingTransport)

    // Resolves (does not hang); a 50ms race guards against regression.
    await Promise.race([
      res.loadSubset({ where: whereEq("body", "x") }),
      new Promise((_r, reject) => setTimeout(() => reject(new Error("loadSubset hung on reset")), 50)),
    ])
  })
})

describe("on-demand loadSubset (M12) — cursor load-more (scroll-back)", () => {
  const cursorOrderBy = [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "desc" } }]

  it("issues ONE atomic fetch shaped as a LoadSubsetOptions (base where + raw cursor)", async () => {
    const page = [
      { id: "t1", body: "16" },
      { id: "n1", body: "15" },
      { id: "n2", body: "14" },
    ]
    const { fetches, subs, transport } = fakeTransport(page)
    const { calls, res } = startOnDemand(transport)

    const base = whereEq("room", "r1")
    const cursor = { whereFrom: whereFunc("lt", "body", "16"), whereCurrent: whereEq("body", "16"), lastKey: "t1" }
    await res.loadSubset({ where: base, orderBy: cursorOrderBy, limit: 5, cursor })

    // A cursor load is a one-shot fetch — it registers no live subscription.
    expect(subs.length).toBe(0)
    // ONE frame, so the server reads both halves at one seq (atomic).
    expect(fetches.length).toBe(1)
    const f = fetches[0]!
    // The frame mirrors TanStack's LoadSubsetOptions: the base `where` is carried
    // SEPARATELY from the raw cursor expressions (which, per TanStack, exclude
    // the base `where`). The server composes base AND each half.
    expect(f.where).toEqual(base)
    expect(f.cursor).toEqual({ whereFrom: cursor.whereFrom, whereCurrent: cursor.whereCurrent })
    expect(f.limit).toBe(5)
    // The whole page lands in the collection as inserts.
    const writes = calls.filter((c) => c[0] === "write").map((c) => (c[1] as { value: unknown }).value)
    expect(writes).toEqual(page)
  })

  it("writes page rows insert-if-absent: a tie already in the collection is skipped", async () => {
    // The boundary tie "t1" is already present (it was in the initial window).
    // Re-inserting it as a sync `insert` with a stale value would throw
    // DuplicateKeySyncError and abort the open transaction — so it must be
    // skipped, leaving the live sub's value intact.
    const page = [
      { id: "t1", body: "16" },
      { id: "n1", body: "15" },
    ]
    const { transport } = fakeTransport(page)
    const { calls, res } = startOnDemand(transport, new Set(["t1"]))

    const base = whereEq("room", "r1")
    const cursor = { whereFrom: whereFunc("lt", "body", "16"), whereCurrent: whereEq("body", "16") }
    await res.loadSubset({ where: base, orderBy: cursorOrderBy, limit: 5, cursor })

    const writes = calls.filter((c) => c[0] === "write").map((c) => (c[1] as { value: { id: string } }).value)
    expect(writes.map((v) => v.id)).toEqual(["n1"]) // t1 (present) skipped, n1 written
  })

  it("takes no live refcount, so a sibling query's sub survives its unload", async () => {
    const { subs, unsubs, transport } = fakeTransport()
    const { res } = startOnDemand(transport)
    const base = whereEq("room", "r1")

    const live = { where: base } // core releases with the object it loaded
    await res.loadSubset(live) // initial live sub, refs = 1
    expect(subs.length).toBe(1)

    const cursor = { whereFrom: whereFunc("lt", "body", "16"), whereCurrent: whereEq("body", "16") }
    const page = { where: base, orderBy: cursorOrderBy, limit: 5, cursor }
    await res.loadSubset(page) // one-shot
    expect(subs.length).toBe(1) // no new sub

    // The framework unloads EVERY loadedSubset, cursor loads included. A cursor
    // unload must be a no-op or it would under-count the still-live base sub.
    res.unloadSubset(page)
    expect(unsubs.length).toBe(0)
    res.unloadSubset(live) // releases the live sub
    expect(unsubs).toEqual([subs[0]!.subId])
  })

  // Hand-computed race (ADR-0003): with the deferred two-frame double request,
  // a live DELETE for a boundary row landing between the ties read and the
  // merge resurrected the deleted row. With ONE atomic fetch the page is a
  // single macrotask whose write completes before the later delete delta is
  // processed, so the delete wins. Pins that ordering: the page is applied,
  // THEN the concurrent delete arrives, and the row stays gone.
  it("does NOT resurrect a row deleted concurrently during scroll-back", async () => {
    // Real collection state as a Map, so insert-if-absent + delete are real.
    const rows = new Map<string, Msg>([["m81", { id: "m81", body: "81" }]]) // boundary row, in window
    const controls = {
      collection: { get: (k: string) => rows.get(k) },
      begin: () => {},
      commit: () => {},
      markReady: () => {},
      truncate: () => rows.clear(),
      write: (m: { type: string; value?: { id: string }; key?: string }) => {
        if (m.type === "delete") rows.delete(m.key!)
        else rows.set(m.value!.id, m.value as Msg)
      },
    }

    let liveHandler: SubHandler | undefined
    const transport = {
      connect: async () => {},
      subscribe: async (_s: string, _t: string, h: SubHandler) => {
        liveHandler = h
        h.onSnapEnd()
      },
      unsubscribe: () => {},
      // One atomic page: ties (m81) + next page (m80) at one seq.
      fetch: async () => [
        { id: "m81", body: "81" },
        { id: "m80", body: "80" },
      ],
      sendMut: async () => ({}),
      close: () => {},
    } as unknown as WebSocketTransport<TestApi>

    const adapter = doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" })
    const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync(controls)

    await res.loadSubset({ where: whereEq("room", "r1") }) // initial sub -> captures liveHandler

    const cursor = { whereFrom: whereFunc("lt", "body", "81"), whereCurrent: whereEq("body", "81") }
    // The page resolves and its merge completes (one macrotask) before the
    // delete delta — the real receive order for a single-frame fetch.
    await res.loadSubset({ where: whereEq("room", "r1"), orderBy: cursorOrderBy, limit: 5, cursor })

    liveHandler!.onDelta("delete", "m81", undefined) // concurrent delete arrives next
    liveHandler!.onUptodate()

    expect(rows.has("m81")).toBe(false) // m81 must stay deleted, not be resurrected
  })
})

describe("on-demand loadSubset (M11) — against the DO", () => {
  function realTransport(room: string): WebSocketTransport<TestApi> {
    return new WebSocketTransport<TestApi>({
      url: `https://example.com/sync/${room}`,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        return ws as unknown as WebSocketLike
      },
    })
  }

  it("a live query loads only its subset; unqueried rows stay absent", async () => {
    const room = "od-subset"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','keep'),('b','drop')")
    })

    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload() // ready, but empty — nothing synced eagerly
    expect(messages.size).toBe(0)

    // A live query's where is pushed to loadSubset -> only 'keep' rows load.
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    await waitFor(() => kept.get("a") !== undefined)
    expect(kept.get("a")).toMatchObject({ id: "a", body: "keep" })
    expect(messages.get("a")).toBeDefined()
    expect(messages.get("b")).toBeUndefined() // 'drop' subset never requested
    t.close()
  })

  it("loads only the bounded window (orderBy + limit), not the whole subset", async () => {
    const room = "od-window"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      for (let i = 1; i <= 20; i++) {
        s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${i}`, String(i).padStart(2, "0"))
      }
    })

    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()

    // Top-5 by body desc — the live query's limit must bound the load. The
    // order is lexical so 0.9 can express it as a cursor: for an order it
    // cannot express (e.g. the default locale string order) 0.9 asks for the
    // full matching subset instead, and the adapter honours that (ADR-0023).
    const top5 = createLiveQueryCollection((q) =>
      q.from({ m: messages }).orderBy(({ m }) => m.body, { direction: "desc", stringSort: "lexical" }).limit(5),
    )
    await top5.preload()
    await waitFor(() => top5.size === 5)

    expect(top5.toArray.map((m) => m.body)).toEqual(["20", "19", "18", "17", "16"])
    // The collection loaded ONLY the bounded window, not all 20.
    expect(messages.size).toBe(5)
    t.close()
  })

  it("fetch returns a bounded ordered page (scroll-back round-trip)", async () => {
    const room = "od-fetch"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      for (let i = 1; i <= 20; i++) {
        s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${i}`, String(i).padStart(2, "0"))
      }
    })

    // Window already showed 20..16; scrolling back fetches the next page
    // strictly below "16", newest-first, bounded by the limit.
    const orderBy = [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "desc" } }]
    const rows = (await t.fetch({
      t: "fetch",
      fetchId: "f1",
      collection: "messages",
      where: whereFunc("lt", "body", "16"),
      orderBy,
      limit: 5,
    })) as Array<Msg>
    expect(rows.map((r) => r.body)).toEqual(["15", "14", "13", "12", "11"])
    t.close()
  })

  it("cursor fetch returns ALL boundary ties (unbounded) + the limited next page, base-filtered", async () => {
    const room = "od-ties"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      const ins = (id: string, body: string) => s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", id, body)
      // Boundary value "30" is tied across 7 rows; only k* are in the base subset.
      for (let i = 1; i <= 5; i++) ins(`k30_${i}`, "30") // ties, in base
      ins("x30_1", "30") // tie, excluded by base
      ins("x30_2", "30") // tie, excluded by base
      for (let i = 1; i <= 3; i++) ins(`k20_${i}`, "20") // next page, in base
      ins("x20_1", "20") // next, excluded by base
      ins("k40_1", "40") // above the boundary (excluded by the cursor)
      ins("k40_2", "40")
    })

    const orderBy = [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "desc" } }]
    const rows = (await t.fetch({
      t: "fetch",
      fetchId: "f-ties",
      collection: "messages",
      where: whereFunc("like", "id", "k%"), // base filter, composed with each half server-side
      cursor: {
        whereCurrent: whereEq("body", "30"), // boundary ties — server applies NO limit
        whereFrom: whereFunc("lt", "body", "30"), // next page — server applies the limit
      },
      orderBy,
      limit: 2,
    })) as Array<Msg>

    // ALL 5 base-matching ties at the boundary come back despite limit:2 (the
    // unbounded whereCurrent), then exactly 2 of the 3 next-page rows.
    expect(rows.filter((r) => r.body === "30").length).toBe(5) // not 7 — x* filtered by base
    expect(rows.filter((r) => r.body === "20").length).toBe(2) // limited, not 3
    expect(rows.some((r) => r.body === "40")).toBe(false) // above the boundary
    expect(rows.every((r) => r.id.startsWith("k"))).toBe(true) // base applied to BOTH halves
    // Ties are emitted before the next page.
    expect(rows.slice(0, 5).every((r) => r.body === "30")).toBe(true)
    t.close()
  })

  it("rejects a malformed cursor (missing a half) instead of scanning the table", async () => {
    const room = "od-badcursor"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      for (let i = 1; i <= 10; i++) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${i}`, String(i))
    })

    // whereCurrent omitted — composing it away would run the ties SELECT
    // unbounded. The server must reject (empty page), not return all 10 rows.
    const orderBy = [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "desc" } }]
    const rows = (await t.fetch({
      t: "fetch",
      fetchId: "f-bad",
      collection: "messages",
      cursor: { whereFrom: whereFunc("lt", "body", "5") } as never, // missing whereCurrent
      orderBy,
      limit: 2,
    })) as Array<Msg>
    expect(rows).toEqual([])
    t.close()
  })

  it("a cold row bumped server-side moves into the collection (no-where live delta upserts)", async () => {
    // The board pattern: an on-demand window with no `where`, so the live sub
    // matches ALL rows. A row outside the loaded window that another client
    // bumps arrives as an `update` delta for an absent key and UPSERTS (move-in,
    // ADR-0002 C4) — the IVM then windows it. Pins that delivery path.
    const room = "od-movein"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(stub, (_i, s) => {
      for (let i = 0; i <= 5; i++) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${i}`, String(i).padStart(2, "0"))
    })

    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()
    // Bounded window — top 3 by body desc (m5, m4, m3). m0 is cold. Lexical so
    // 0.9 pages by cursor rather than loading the full subset (see above).
    const top3 = createLiveQueryCollection((q) =>
      q.from({ m: messages }).orderBy(({ m }) => m.body, { direction: "desc", stringSort: "lexical" }).limit(3),
    )
    await top3.preload()
    await waitFor(() => top3.size === 3)
    expect(messages.get("m0")).toBeUndefined()

    // Another writer bumps the cold m0 to the top (raw write + broadcast — the
    // firehose path). The no-where live sub delivers the update delta.
    await runInDurableObject(stub, (instance, s) => {
      s.storage.sql.exec("UPDATE messages SET body = ? WHERE id = ?", "99", "m0")
      ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
    })

    await waitFor(() => messages.get("m0") !== undefined)
    expect(messages.get("m0")).toMatchObject({ id: "m0", body: "99" })
    await waitFor(() => top3.get("m0") !== undefined) // IVM windowed it to the top
    t.close()
  })

  it("a write outside every loaded subset is confirmed without stranding a phantom", async () => {
    const room = "od-outside"
    const t = realTransport(room)
    await t.connect()
    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    // Insert a row OUTSIDE the loaded subset (body !== "keep").
    await messages.insert({ id: "z", body: "other" }).isPersisted.promise

    // It must not linger in the collection: the out-of-subset row is retired
    // (synthetic delete to the loaded sub; empty-commit backstop).
    await waitFor(() => messages.get("z") === undefined)
    expect(messages.get("z")).toBeUndefined()
    expect(kept.get("z")).toBeUndefined()
    t.close()
  })
})
