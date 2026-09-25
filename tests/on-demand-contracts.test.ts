import * as db from "@tanstack/db"
import { and, BTreeIndex, createCollection, createLiveQueryCollection, eq, gt, ilike, like } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: an on-demand collection is the union of the subsets its live queries
// acquire, kept live by the DO's subscriptions (ADR-0023). These drive the REAL
// upstream path — live-query mount/unmount, window moves, GC cleanup — against
// the DO, on both the floor and the current @tanstack/db (ADR-0022), and pin
// what the user sees: a query's rows are the rows its request asked for; a row
// another loaded query still holds is never deleted by a neighbour's move-out;
// a released subset leaves nothing stale behind; one refused subscription never
// disturbs the others; and cleanup leaves a shared transport alive.
//
// 0.9's ordered loader makes several of these routine (see ADR-0023): it asks
// for boundary ties with a separate `where` (no cursor), asks for the FULL
// matching subset after a published row is deleted or re-ordered, and replays
// acquisitions release-then-reload.

// @tanstack/db 0.9.0 removed the subset-algebra helpers (its CHANGELOG), so
// their presence marks the 0.8.x floor. Used only where upstream itself differs.
const db08 = "isWhereSubset" in db

const room = (tag: string): string => `odc-${tag}-${crypto.randomUUID()}`
const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

/** Raw server-side write + broadcast (the firehose path other clients use). */
async function serverExec(r: string, sql: string, ...args: Array<unknown>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    s.storage.sql.exec(sql, ...args)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
/** Seed `bodies` as messages m00.., drained BEFORE any subscriber exists. */
async function seed(r: string, bodies: Array<string>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    bodies.forEach((b, i) => s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${String(i).padStart(2, "0")}`, b))
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
const pad = (n: number): string => String(n).padStart(2, "0")
const serverSubCount = (r: string): Promise<number> =>
  runInDurableObject(stubFor(r), (_i, s) => Number(Array.from(s.storage.sql.exec("SELECT count(*) AS c FROM _sync_subs"))[0]!.c))

interface Gate {
  hold: boolean
  queue: Array<() => void>
  /** Hold only frames of these types (default: every frame). */
  only?: ReadonlyArray<string>
}
const codec = createFrameCodec()
/** A real transport to the room. `frames` records every sub frame the client
 *  sends; an optional `gate` holds inbound frames (in order) while `hold`. */
function transportFor(r: string, opts: { frames?: Array<string>; gate?: Gate } = {}): WebSocketTransport<TestApi> {
  const t = new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 0,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      const gate = opts.gate
      if (!gate) return ws as unknown as WebSocketLike
      return {
        send: (d) => ws.send(d as never),
        close: (c, reason) => ws.close(c, reason),
        addEventListener: (type, fn) => {
          if (type !== "message") return ws.addEventListener(type as never, fn as never)
          ws.addEventListener("message", (ev) => {
            const held = gate.hold && (!gate.only || gate.only.includes(codec.decode(ev.data as ArrayBuffer).t))
            if (held) gate.queue.push(() => fn(ev as never))
            else fn(ev as never)
          })
        },
        removeEventListener: () => {},
      } satisfies WebSocketLike
    },
  })
  if (opts.frames) {
    const frames = opts.frames
    const sub = t.subscribe.bind(t)
    t.subscribe = (...a) => {
      frames.push(a[0])
      return sub(...a)
    }
  }
  return t
}
const onDemand = (t: WebSocketTransport<TestApi>) =>
  createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }))
type Windowed = { utils: { setWindow: (w: { offset?: number; limit?: number }) => true | Promise<void> } }
const setWindow = async (q: unknown, limit: number): Promise<void> => {
  const r = (q as Windowed).utils.setWindow({ offset: 0, limit })
  if (r !== true) await r
}
const bodies = (q: { toArray: Array<{ body: string }> }): Array<string> => q.toArray.map((m) => m.body)

describe("on-demand subset identity is the whole request (ADR-0023)", () => {
  it("two orders over the same where each get their own window", async () => {
    // Pre-fix (0.8 and 0.9): identity was `where` alone, so the asc query
    // shared the desc query's bounded snapshot and showed its rows.
    const r = room("orders")
    const t = transportFor(r)
    await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1)))
    const messages = onDemand(t)
    const desc = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, "desc").limit(3))
    await desc.preload()
    await waitFor(() => desc.size === 3)
    const asc = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, "asc").limit(3))
    await asc.preload()
    await waitFor(() => bodies(asc).join() === "01,02,03")
    expect(bodies(desc)).toEqual(["20", "19", "18"])
    t.close()
  })

  it("growing an unindexed window loads the rows it grows into (0.9+)", async () => {
    // No index: 0.9 re-requests `{ orderBy, limit: bigger }` with no cursor.
    // Pre-fix it matched the first request's `where` and was answered with that
    // request's already-settled promise — nothing reached the DO. 0.8.x never
    // re-requests an unindexed window at all (upstream; an adapter cannot help),
    // so there the window deliberately stays at its first page.
    const r = room("grow")
    const t = transportFor(r)
    await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1)))
    const messages = onDemand(t)
    const win = createLiveQueryCollection((q) =>
      q.from({ m: messages }).where(({ m }) => gt(m.body, "05")).orderBy(({ m }) => m.body, "asc").limit(3),
    )
    await win.preload()
    await waitFor(() => win.size === 3)
    await setWindow(win, 8)
    if (db08) {
      await sleep(100)
      expect(win.size).toBe(3)
    } else {
      await waitFor(() => win.size === 8)
      expect(bodies(win)).toEqual(["06", "07", "08", "09", "10", "11", "12", "13"])
    }
    t.close()
  })

  it("an indexed top-5 window backfills from the DO after a row in it is deleted", async () => {
    // 0.9: a delete of a published row makes the ordered loader request the
    // full matching subset. Pre-fix that request matched the first page's
    // `where` and settled on the 5-row window, so the loader believed the whole
    // source was local and never refilled (0.8.x refilled via a cursor fetch).
    const r = room("backfill")
    const t = transportFor(r)
    await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1)))
    const messages = onDemand(t)
    messages.createIndex((m) => m.body, { indexType: BTreeIndex })
    const top5 = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, "desc").limit(5))
    await top5.preload()
    await waitFor(() => top5.size === 5)
    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m19") // body "20", the top row
    await waitFor(() => bodies(top5).join() === "19,18,17,16,15")
    t.close()
  })
})

describe("the board regression: a tied, expressible-order window (0.9 prefix-and-tie)", () => {
  // Mirrors examples/board: an ordered on-demand window whose order column is
  // expressible as a cursor (here a lexical string; the board uses a number)
  // and whose seed has tie bursts at the window boundary.
  const tied = Array.from({ length: 60 }, (_, i) => String(10 + Math.floor(i / 5))) // "10".."21", ×5 each
  const lexicalDesc = { direction: "desc", stringSort: "lexical" } as const

  for (const indexed of [false, true]) {
    it(`an optimistic insert at the top of the window stays, and the window still pages (index: ${indexed})`, async () => {
      // Pre-fix on 0.9: the boundary-tie load became a second live sub; its
      // synthetic move-out delete for the new row followed the main sub's
      // insert, the row was lost once `committed` retired the overlay, and the
      // resulting full-source request was swallowed — paging died.
      const r = room(`board-${indexed}`)
      const frames: Array<string> = []
      const t = transportFor(r, { frames })
      await seed(r, tied)
      const messages = onDemand(t)
      // The index must compare like the query (lexical) to serve it.
      if (indexed) messages.createIndex((m) => m.body, { indexType: BTreeIndex, options: { compareOptions: { stringSort: "lexical" } } })
      const win = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, lexicalDesc).limit(8))
      const sub = win.subscribeChanges(() => {})
      await win.preload()
      await waitFor(() => win.size === 8)

      await messages.insert({ id: "new1", body: "99" }).isPersisted.promise
      await sleep(50) // the commit batch and `committed` have both landed
      expect(messages.get("new1")).toMatchObject({ body: "99" })
      expect(bodies(win)[0]).toBe("99")

      await setWindow(win, 30)
      // 0.8.x never pages this window, with or without the lexical index
      // (upstream-only check: no request reaches the source); its cursor
      // paging is pinned by the backfill test above.
      if (!db08) {
        await waitFor(() => win.size === 30)
        expect(bodies(win).slice(0, 3)).toEqual(["99", "21", "21"])
      }
      // B: boundary ties never become extra live subscriptions on the DO.
      expect(await serverSubCount(r)).toBe(1)
      sub.unsubscribe()
      t.close()
    })
  }

  it("a server-side insert at the top of a tied window appears", async () => {
    const r = room("topins")
    const t = transportFor(r)
    await seed(r, tied)
    const messages = onDemand(t)
    const top5 = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, lexicalDesc).limit(5))
    await top5.preload()
    await waitFor(() => top5.size === 5)
    await serverExec(r, "INSERT INTO messages(id,body) VALUES(?,?)", "srv", "99")
    await waitFor(() => bodies(top5)[0] === "99")
    t.close()
  })
})

describe("row ownership across overlapping subsets (ADR-0023)", () => {
  it("a narrow subset's move-out does not delete a row a wider subset still holds", async () => {
    // Every subscription on the socket gets a frame for every changed key: the
    // row if it matches, else a synthetic delete (ADR-0002 C4). Pre-fix the
    // narrow sub's delete hard-deleted a row the wide sub still held.
    const r = room("overlap")
    const t = transportFor(r)
    await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1)))
    const messages = onDemand(t)
    const wide = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => gt(m.body, "10")))
    const narrow = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "16")))
    await wide.preload()
    await narrow.preload()
    await waitFor(() => wide.size === 10 && narrow.size === 1)

    await serverExec(r, "UPDATE messages SET body = ? WHERE id = ?", "18x", "m17") // "18" -> "18x": stays in wide
    await waitFor(() => (messages.get("m17") as { body?: string } | undefined)?.body === "18x")
    await sleep(30) // the narrow sub's synthetic delete rode the same batch
    expect(wide.size).toBe(10)

    await serverExec(r, "UPDATE messages SET body = ? WHERE id = ?", "16x", "m15") // leaves narrow, stays in wide
    await waitFor(() => narrow.size === 0)
    expect(wide.get("m15")).toMatchObject({ body: "16x" })

    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m12") // a real delete leaves both
    await waitFor(() => messages.get("m12") === undefined)
    expect(wide.size).toBe(9)
    t.close()
  })
})

describe("release and reload (0.9 'support a release/load gap')", () => {
  it("a row deleted while its subset was released does not come back on reload", async () => {
    // Pre-fix (0.8 and 0.9): release unsubscribed but kept the rows; nothing
    // kept them live, and the reload snapshot carries no tombstones.
    const r = room("gap")
    const t = transportFor(r)
    await seed(r, ["01", "02", "03"])
    const messages = onDemand(t)
    const mk = () => createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    const q1 = mk()
    await q1.preload()
    await waitFor(() => q1.size === 3)
    await q1.cleanup() // released (as after GC)
    await sleep(20)
    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m01")
    const q2 = mk()
    await q2.preload()
    await waitFor(() => q2.size === 2)
    await sleep(50)
    expect(q2.toArray.map((m) => m.id).sort()).toEqual(["m00", "m02"])
    t.close()
  })

  it("a snapshot still in flight for a released subset never lands in its reload", async () => {
    // Interleaving B (/tmp/w4 hand-compute; ADR-0023): unmount + remount of
    // the same query while the first snapshot is in flight, with the row
    // deleted on the DO in between. Pre-fix both acquisitions shared one
    // subId, so the old snapshot dispatched into the new handler and left the
    // deleted row behind for good.
    const r = room("inflight")
    const gate: Gate = { hold: false, queue: [] }
    const t = transportFor(r, { gate })
    await seed(r, ["01", "02", "03"])
    const messages = onDemand(t)
    await t.connect()
    const mk = () => createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    gate.hold = true
    const q1 = mk()
    void q1.preload().catch(() => {})
    await sleep(30) // q1's snapshot {m00,m01,m02} is now queued at the client
    await q1.cleanup()
    await sleep(30) // the DO has processed the unsub
    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m01")
    const q2 = mk()
    const ready = q2.preload()
    await sleep(30) // q2's snapshot {m00,m02} is queued behind q1's
    gate.hold = false
    for (const deliver of gate.queue.splice(0)) deliver()
    await ready
    await sleep(50)
    expect(messages.get("m01")).toBeUndefined()
    expect(q2.toArray.map((m) => m.id).sort()).toEqual(["m00", "m02"])
    t.close()
  })
})

describe("a refused subscription stays contained", () => {
  it("an unsupported predicate beside a valid subset: the valid rows stay, no resubscribe storm", async () => {
    // Pre-fix the refusal (`reset`, no snapshot) truncated the WHOLE collection:
    // on 0.8 the valid subset's rows were wiped and never reloaded; on 0.9 the
    // truncate replay re-subscribed the refused predicate, forever.
    const r = room("refused")
    const frames: Array<string> = []
    const t = transportFor(r, { frames })
    await seed(r, ["01", "02", "03", "04", "05"])
    const messages = onDemand(t)
    const ok = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => gt(m.body, "02")))
    await ok.preload()
    await waitFor(() => ok.size === 3)
    const bad = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => ilike(m.body, "%0%")))
    await Promise.race([bad.preload().catch(() => {}), sleep(300)])
    await sleep(300)
    expect(frames.length).toBeLessThanOrEqual(3)
    expect(ok.size).toBe(3)
    expect(messages.size).toBe(3)
    t.close()
  })
})

describe("cleanup and restart", () => {
  it("cleaning up an on-demand collection leaves a shared transport serving its other collections", async () => {
    // Pre-fix on-demand cleanup closed the (shared) transport; an eager
    // collection on it silently stopped receiving changes. 0.9.1's GC reclaim
    // runs cleanup for collections that start syncing without subscribers.
    const r = room("shared")
    const t = transportFor(r)
    await seed(r, ["01"])
    const files = createCollection(doCollectionOptions({ transport: t, table: "files", getKey: (f) => f.id }))
    await files.preload()
    const messages = onDemand(t)
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    await q.preload()
    await waitFor(() => q.size === 1)
    await q.cleanup()
    await messages.cleanup()
    await serverExec(r, "INSERT INTO files(id,name) VALUES(?,?)", "f1", "x")
    await waitFor(() => files.get("f1") !== undefined)
    expect(await serverSubCount(r)).toBe(1) // only the eager files sub remains

    // The cleaned-up collection restarts cleanly on the same transport.
    const again = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    await again.preload()
    await waitFor(() => again.size === 1)
    t.close()
  })
})

describe("more ownership edges", () => {
  it("a row moving between two disjoint subsets in one batch stays, in the right query", async () => {
    // One change, two frames in one batch: a move-out delete from the old
    // subset's watch and the row from the new one's, in either order. The row
    // must end present exactly once, in the new query only.
    const r = room("move")
    const t = transportFor(r)
    await seed(r, ["a", "a", "b"])
    const messages = onDemand(t)
    const qa = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "a")))
    const qb = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "b")))
    await qa.preload()
    await qb.preload()
    await waitFor(() => qa.size === 2 && qb.size === 1)
    await serverExec(r, "UPDATE messages SET body = ? WHERE id = ?", "b", "m00") // a -> b
    await waitFor(() => qb.size === 2)
    expect(qa.toArray.map((m) => m.id)).toEqual(["m01"])
    expect(messages.get("m00")).toMatchObject({ body: "b" })
    await serverExec(r, "UPDATE messages SET body = ? WHERE id = ?", "a", "m02") // b -> a, the other order
    await waitFor(() => qa.size === 2)
    expect(qb.toArray.map((m) => m.id)).toEqual(["m00"])
    t.close()
  })

  it("releasing a subset while an optimistic update of one of its rows is in flight", async () => {
    // Release deletes the rows only that subset held (ADR-0023). A row with a
    // pending optimistic update stays visible through its overlay; the write
    // still commits on the DO; once confirmed, nothing holds the row, so the
    // post-mutation empty commit retires the overlay and the row leaves.
    const r = room("release-pending")
    const gate: Gate = { hold: false, queue: [], only: ["committed"] }
    const t = transportFor(r, { gate })
    await seed(r, ["x", "y"])
    const messages = onDemand(t)
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    await q.preload()
    await waitFor(() => q.size === 2)
    gate.hold = true
    const tx = messages.update("m00", (d) => {
      d.body = "x2" // still inside the subset: only the release can remove it
    })
    await sleep(30) // the DO committed it; its `committed` is held
    await q.cleanup() // released while the write is in flight
    await sleep(20)
    expect(messages.get("m00")).toMatchObject({ body: "x2" }) // the overlay still shows it
    gate.hold = false
    for (const deliver of gate.queue.splice(0)) deliver()
    await tx.isPersisted.promise
    // The release's deletes are sync writes, which core applies once the
    // persisting user transaction settles: then nothing holds either row.
    await waitFor(() => messages.get("m00") === undefined && messages.get("m01") === undefined)
    const onServer = await runInDurableObject(stubFor(r), (_i, s) =>
      Array.from(s.storage.sql.exec("SELECT body FROM messages WHERE id = 'm00'")),
    )
    expect(onServer).toEqual([{ body: "x2" }])
    t.close()
  })

  it("an optimistic delete then re-insert of one key round-trips (0.9.1)", async () => {
    const r = room("reinsert")
    const t = transportFor(r)
    await seed(r, ["x"])
    const messages = onDemand(t)
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    await q.preload()
    await waitFor(() => q.size === 1)
    await messages.delete("m00").isPersisted.promise
    await messages.insert({ id: "m00", body: "again" }).isPersisted.promise
    await waitFor(() => (messages.get("m00") as { body?: string } | undefined)?.body === "again")
    expect(q.toArray.map((m) => m.body)).toEqual(["again"])
    t.close()
  })

  it("a subset load settles only once its rows are visible (0.8.5 commit receipts)", async () => {
    // While a user transaction is persisting, core queues sync commits behind it
    // and `commit()` returns a pending receipt. The load must wait for it: a
    // receipt-less core (< 0.8.5) would settle while the rows are invisible.
    const r = room("receipt")
    const gate: Gate = { hold: false, queue: [], only: ["committed"] }
    const t = transportFor(r, { gate })
    await seed(r, ["x", "x"])
    const messages = onDemand(t)
    await t.connect()
    gate.hold = true
    const tx = messages.insert({ id: "w", body: "w" }) // persisting until `committed` is released
    await sleep(20)
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => eq(m.body, "x")))
    let visibleAtSettle: boolean | null = null
    const loaded = q.preload().then(() => {
      visibleAtSettle = messages.get("m00") !== undefined && messages.get("m01") !== undefined
    })
    await sleep(60) // the snapshot has arrived and its commit is queued
    gate.hold = false
    for (const deliver of gate.queue.splice(0)) deliver()
    await tx.isPersisted.promise
    await loaded
    expect(visibleAtSettle).toBe(true)
    t.close()
  })
})

describe("loadSubset / unloadSubset contract pins (fake transport)", () => {
  type Opts = { where?: unknown; orderBy?: unknown; limit?: number; offset?: number; cursor?: unknown; signal?: AbortSignal }
  type OnDemand = { loadSubset: (o: Opts) => true | Promise<void>; unloadSubset: (o: Opts) => void; cleanup: () => void }
  const eqBody = (v: string): unknown => ({ type: "func", name: "eq", args: [{ type: "ref", path: ["body"] }, { type: "val", value: v }] })
  const deepFreeze = <T>(o: T): T => {
    if (o && typeof o === "object" && !Object.isFrozen(o)) {
      Object.freeze(o)
      for (const v of Object.values(o as object)) deepFreeze(v)
    }
    return o
  }
  function harness(page: Array<unknown> = []) {
    const subs: Array<string> = []
    const unsubs: Array<string> = []
    const fetches: Array<unknown> = []
    const handlers = new Map<string, SubHandler>()
    const rows = new Map<string, unknown>()
    let truncates = 0
    let releaseFetch: (() => void) | null = null
    let released = false
    const transport = {
      connect: async () => {},
      subscribe: async (subId: string, _c: string, h: SubHandler) => {
        subs.push(subId)
        handlers.set(subId, h)
        h.onSnapEnd()
      },
      unsubscribe: (subId: string) => unsubs.push(subId),
      // A covered fetch is issued only once its watch is accepted (a microtask
      // later), so a release may come first: then the next fetch answers at once.
      fetch: (f: unknown) => {
        fetches.push(f)
        return new Promise<Array<unknown>>((res) => {
          if (released) {
            released = false
            res(page)
          } else releaseFetch = () => res(page)
        })
      },
      sendMut: async () => ({}),
      close: () => {},
    } as unknown as WebSocketTransport<TestApi>
    const adapter = doCollectionOptions({ transport, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" })
    const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync({
      collection: { get: (k: string) => rows.get(k) },
      begin: () => {},
      write: (m: { type: string; value?: { id: string }; key?: string }) => {
        if (m.type === "delete") rows.delete(m.key!)
        else rows.set(m.value!.id, m.value)
      },
      commit: () => true,
      markReady: () => {},
      truncate: () => {
        truncates++
        rows.clear()
      },
    })
    const release = (): void => {
      const r = releaseFetch
      releaseFetch = null
      if (r) r()
      else released = true
    }
    return { subs, unsubs, fetches, handlers, rows, res, truncates: () => truncates, releaseFetch: release }
  }

  it("request data is never mutated (0.9: LoadSubsetOptions are immutable)", async () => {
    const h = harness([{ id: "a", body: "x" }])
    const base = deepFreeze({ where: eqBody("x"), orderBy: [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "asc" } }], limit: 5 })
    await h.res.loadSubset(base) // opens a watch
    const covered = deepFreeze({ where: eqBody("x"), orderBy: base.orderBy, limit: 50 })
    const p = h.res.loadSubset(covered) // served by a fetch
    h.releaseFetch()
    await p
    const cursor = deepFreeze({ ...base, cursor: { whereFrom: eqBody("y"), whereCurrent: eqBody("x") } })
    const q = h.res.loadSubset(cursor)
    h.releaseFetch()
    await q
    h.res.unloadSubset(covered)
    h.res.unloadSubset(base)
    expect(h.unsubs.length).toBe(1)
  })

  it("release is idempotent per acquisition, and shared owners keep the watch", async () => {
    const h = harness()
    const a = { where: eqBody("x") }
    const b = { where: eqBody("x") } // an identical request from another owner
    await h.res.loadSubset(a)
    await h.res.loadSubset(b)
    expect(h.subs.length).toBe(1)
    const equal = { where: eqBody("x") } // equal, but never loaded
    h.res.unloadSubset(equal)
    h.res.unloadSubset(equal) // must not release a's or b's load
    h.res.unloadSubset(a)
    h.res.unloadSubset(a) // a repeat release is a no-op, not b's release
    expect(h.unsubs).toEqual([])
    h.res.unloadSubset(b)
    expect(h.unsubs).toEqual([h.subs[0]])
  })

  it("a page whose request was aborted and released installs nothing", async () => {
    // Core aborts a request's signal and releases its acquisition together.
    // A covered fetch is guarded by the release (it may be shared: see below);
    // a cursor page (never shared) by the signal itself.
    const h = harness([{ id: "late", body: "x" }])
    await h.res.loadSubset({ where: eqBody("x") })
    const ac = new AbortController()
    const covered = { where: eqBody("x"), limit: 10, signal: ac.signal }
    const p = h.res.loadSubset(covered)
    ac.abort()
    h.res.unloadSubset(covered)
    h.releaseFetch()
    await p
    expect(h.rows.has("late")).toBe(false)

    const ac2 = new AbortController()
    const cursor = { where: eqBody("x"), limit: 10, cursor: { whereFrom: eqBody("w"), whereCurrent: eqBody("x") }, signal: ac2.signal }
    const q = h.res.loadSubset(cursor)
    ac2.abort()
    h.releaseFetch()
    await q
    expect(h.rows.has("late")).toBe(false)
  })

  it("an offset without a cursor is rejected loudly, not answered with the wrong page", async () => {
    const h = harness()
    await expect(h.res.loadSubset({ where: eqBody("x"), limit: 5, offset: 10 })).rejects.toThrow(/offset 10/)
    expect(h.subs).toEqual([])
  })
})

describe("codex diff review: pinned fixes", () => {
  type Opts = { where?: unknown; orderBy?: unknown; limit?: number; signal?: AbortSignal }
  type OnDemand = { loadSubset: (o: Opts) => true | Promise<void>; unloadSubset: (o: Opts) => void }
  const eqBody = (v: string): unknown => ({ type: "func", name: "eq", args: [{ type: "ref", path: ["body"] }, { type: "val", value: v }] })

  /** Deliver the first `n` held frames, drop the rest (lost in a drop). */
  const deliverFirst = (gate: Gate, n: number): void => {
    const q = gate.queue.splice(0)
    q.slice(0, n).forEach((deliver) => deliver())
  }
  const dropSocket = (r: string) =>
    runInDurableObject(stubFor(r), (_i, state) => {
      for (const sock of state.getWebSockets()) sock.close(1000, "drop")
    })

  it("a partial first snapshot restarted after a drop leaves no ghost", async () => {
    // A drop after part of a watch's first snapshot arrived: the transport
    // restarts it from a fresh snapshot (ADR-0023 D7). A row the partial one
    // delivered and the DO deleted meanwhile must not survive — it was still
    // only an insert in the open sync transaction, not yet synced.
    const r = room("partial")
    const gate: Gate = { hold: false, queue: [] }
    const t = transportFor(r, { gate })
    await seed(r, ["a", "b", "c"])
    const messages = onDemand(t)
    await t.connect()
    gate.hold = true
    const q = createLiveQueryCollection((qb) => qb.from({ m: messages }).where(({ m }) => gt(m.body, "")))
    void q.preload().catch(() => {})
    await sleep(30) // snap m00, snap m01, snap m02, snap-end are held
    deliverFirst(gate, 2) // m00 and m01 arrive; the rest is lost
    gate.hold = true
    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m01")
    await sleep(10)
    gate.queue.length = 0 // its delta dies with the socket
    gate.hold = false
    await dropSocket(r)
    await waitFor(() => q.size === 2)
    await sleep(50)
    expect(messages.get("m01")).toBeUndefined()
    expect(q.toArray.map((m) => m.id).sort()).toEqual(["m00", "m02"])
    t.close()
  })

  it("a watch whose own filter is an `and` still covers 0.9's boundary-tie request", async () => {
    // The tie request is `and(subscriptionWhere, tie)`; when the subscription
    // where is itself an `and`, every one of its conjuncts must be matched.
    const r = room("compound")
    const t = transportFor(r)
    await seed(r, Array.from({ length: 30 }, (_, i) => String(10 + Math.floor(i / 5))))
    const messages = onDemand(t)
    const win = createLiveQueryCollection((q) =>
      q
        .from({ m: messages })
        .where(({ m }) => and(gt(m.body, "0"), like(m.id, "m%")))
        .orderBy(({ m }) => m.body, { direction: "desc", stringSort: "lexical" })
        .limit(7),
    )
    await win.preload()
    await waitFor(() => win.size === 7)
    await sleep(50)
    expect(await serverSubCount(r)).toBe(1)
    t.close()
  })

  function harness(page: Array<unknown>) {
    const subs: Array<string> = []
    const unsubs: Array<string> = []
    const handlers = new Map<string, SubHandler>()
    const rows = new Map<string, unknown>()
    let truncates = 0
    let releaseFetch: (() => void) | null = null
    let released = false
    const transport = {
      connect: async () => {},
      subscribe: async (subId: string, _c: string, h: SubHandler) => {
        subs.push(subId)
        handlers.set(subId, h)
        h.onSnapEnd()
      },
      unsubscribe: (subId: string) => unsubs.push(subId),
      fetch: () =>
        new Promise<Array<unknown>>((res) => {
          if (released) {
            released = false
            res(page)
          } else releaseFetch = () => res(page)
        }),
      sendMut: async () => ({}),
      close: () => {},
    } as unknown as WebSocketTransport<TestApi>
    const adapter = doCollectionOptions({ transport, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" })
    const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync({
      collection: { get: (k: string) => rows.get(k) },
      begin: () => {},
      write: (m: { type: string; value?: { id: string }; key?: string }) => {
        if (m.type === "delete") rows.delete(m.key!)
        else rows.set(m.value!.id, m.value)
      },
      commit: () => true,
      markReady: () => {},
      truncate: () => {
        truncates++
        rows.clear()
      },
    })
    const release = (): void => {
      const r = releaseFetch
      releaseFetch = null
      if (r) r()
      else released = true
    }
    return { subs, unsubs, handlers, rows, res, truncates: () => truncates, releaseFetch: release }
  }

  it("a below-floor reset retires every watch at once, and a page from before it installs nothing", async () => {
    const h = harness([{ id: "old", body: "x" }])
    const x = { where: eqBody("x") }
    const y = { where: eqBody("y") }
    await h.res.loadSubset(x)
    await h.res.loadSubset(y)
    const covered = h.res.loadSubset({ where: eqBody("x"), limit: 3 }) // a fetch, in flight
    const [sx, sy] = h.subs
    h.handlers.get(sx!)!.onReset() // bootstrapped: below the retention floor
    expect(h.truncates()).toBe(1)
    expect(h.unsubs.sort()).toEqual([sx, sy].sort()) // both retired: their trailing frames find no handler
    h.releaseFetch()
    await covered
    expect(h.rows.has("old")).toBe(false) // read before the truncate: not installed
    await h.res.loadSubset({ where: eqBody("x") }) // core's replay: a fresh watch, not the old one
    expect(h.subs.length).toBe(3)
  })

  it("one owner's abort does not cancel a covered fetch another owner shares", async () => {
    const h = harness([{ id: "cold", body: "x" }])
    await h.res.loadSubset({ where: eqBody("x"), limit: 1 }) // the watch
    const ac = new AbortController()
    const a = { where: eqBody("x"), limit: 10, signal: ac.signal }
    const b = { where: eqBody("x"), limit: 10 }
    const pa = h.res.loadSubset(a)
    const pb = h.res.loadSubset(b) // identical request: shares a's fetch
    ac.abort()
    h.res.unloadSubset(a) // core releases the aborted owner
    h.releaseFetch()
    await Promise.all([pa, pb])
    expect(h.rows.has("cold")).toBe(true) // b still gets its rows
  })
})

describe("codex second review: pinned fixes", () => {
  type Opts = { where?: unknown; orderBy?: unknown; limit?: number }
  type OnDemand = { loadSubset: (o: Opts) => true | Promise<void>; unloadSubset: (o: Opts) => void }
  const eqBody = (v: string): unknown => ({ type: "func", name: "eq", args: [{ type: "ref", path: ["body"] }, { type: "val", value: v }] })
  const settledWithin = (p: true | Promise<void>, ms = 50): Promise<boolean> =>
    Promise.race([Promise.resolve(p).then(() => true), sleep(ms).then(() => false)])

  /** Fake transport: a sub completes its snapshot at once only if `snaps` says so. */
  function harness(snaps: (where: unknown, limit?: number) => boolean) {
    const subs: Array<string> = []
    const unsubs: Array<string> = []
    const handlers = new Map<string, SubHandler>()
    const transport = {
      connect: async () => {},
      subscribe: async (subId: string, _c: string, h: SubHandler, where?: unknown, _o?: unknown, limit?: number) => {
        subs.push(subId)
        handlers.set(subId, h)
        if (snaps(where, limit)) h.onSnapEnd()
      },
      unsubscribe: (subId: string) => unsubs.push(subId),
      fetch: async () => [],
      sendMut: async () => ({}),
      close: () => {},
    } as unknown as WebSocketTransport<TestApi>
    const adapter = doCollectionOptions({ transport, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" })
    const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync({
      collection: { get: () => undefined },
      begin: () => {},
      write: () => {},
      commit: () => true,
      markReady: () => {},
      truncate: () => {},
    })
    return { subs, unsubs, handlers, res }
  }

  it("a truncate settles a load whose watch never delivered its snapshot", async () => {
    const x = eqBody("x")
    const h = harness((where) => where === x) // only the x watch completes
    await h.res.loadSubset({ where: x })
    const pending = h.res.loadSubset({ where: eqBody("y") }) // its own watch: x does not cover y
    expect(h.subs.length).toBe(2)
    h.handlers.get(h.subs[0]!)!.onReset() // bootstrapped: below the retention floor -> truncate
    expect(await settledWithin(pending)).toBe(true)
  })

  it("a release before the snapshot settles the load", async () => {
    const h = harness(() => false)
    const o = { where: eqBody("z") }
    const pending = h.res.loadSubset(o)
    h.res.unloadSubset(o)
    expect(await settledWithin(pending)).toBe(true)
  })

  it("a request covered by a pending watch that is then refused opens its own", async () => {
    const h = harness((_where, limit) => limit === 5) // the first (unlimited) watch never completes
    const first = h.res.loadSubset({ where: eqBody("x") })
    const second = h.res.loadSubset({ where: eqBody("x"), limit: 5 }) // waits on the pending watch
    expect(h.subs.length).toBe(1)
    h.handlers.get(h.subs[0]!)!.onReset() // refused before its first snapshot
    expect(await settledWithin(first)).toBe(true)
    expect(await settledWithin(second)).toBe(true)
    expect(h.subs.length).toBe(2) // the covered request fell back to its own watch
  })

  it("two compatible queries mounted together share one server sub", async () => {
    const r = room("together")
    const t = transportFor(r)
    await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1)))
    const messages = onDemand(t)
    const desc = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, { direction: "desc", stringSort: "lexical" }).limit(3))
    const asc = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, { direction: "asc", stringSort: "lexical" }).limit(3))
    await Promise.all([desc.preload(), asc.preload()])
    await waitFor(() => bodies(desc).join() === "20,19,18" && bodies(asc).join() === "01,02,03")
    expect(await serverSubCount(r)).toBe(1)
    t.close()
  })
})

describe("codex third review: pinned fixes", () => {
  type Opts = { where?: unknown; orderBy?: unknown; limit?: number }
  type OnDemand = { loadSubset: (o: Opts) => true | Promise<void>; unloadSubset: (o: Opts) => void }
  const eqBody = (v: string): unknown => ({ type: "func", name: "eq", args: [{ type: "ref", path: ["body"] }, { type: "val", value: v }] })
  const outcome = (p: true | Promise<void>, ms = 50): Promise<string> =>
    Promise.race([
      Promise.resolve(p).then(
        () => "resolved",
        (e: Error) => `rejected: ${e.message}`,
      ),
      sleep(ms).then(() => "pending"),
    ])

  function harness(opts: { snaps: (where: unknown, limit?: number) => boolean; failSub?: (where: unknown, limit?: number) => Error | null }) {
    const subs: Array<string> = []
    const handlers = new Map<string, SubHandler>()
    let receipt: true | Promise<void> = true
    const transport = {
      connect: async () => {},
      subscribe: async (subId: string, _c: string, h: SubHandler, where?: unknown, _o?: unknown, limit?: number) => {
        subs.push(subId)
        handlers.set(subId, h)
        const fail = opts.failSub?.(where, limit)
        if (fail) {
          await sleep(10) // e.g. the connect under it fails
          throw fail
        }
        if (opts.snaps(where, limit)) h.onSnapEnd()
      },
      unsubscribe: () => {},
      fetch: async () => [],
      sendMut: async () => ({}),
      close: () => {},
    } as unknown as WebSocketTransport<TestApi>
    const adapter = doCollectionOptions({ transport, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" })
    const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync({
      collection: { get: () => undefined },
      begin: () => {},
      write: () => {},
      commit: () => receipt,
      markReady: () => {},
      truncate: () => {},
    })
    return { subs, handlers, res, setReceipt: (r: true | Promise<void>) => (receipt = r) }
  }

  it("a truncate settles orphaned loads only once the truncate itself is applied", async () => {
    // While a persisting user transaction queues sync commits, the truncate's
    // receipt is pending: a load it orphans must not report success before the
    // truncate (and core's replay barrier) exists; a failed truncate fails it.
    for (const outcomeOfTruncate of ["applied", "failed"] as const) {
      const x = eqBody("x")
      const h = harness({ snaps: (where) => where === x })
      await h.res.loadSubset({ where: x })
      const orphan = h.res.loadSubset({ where: eqBody("y") })
      let apply!: () => void
      let fail!: (e: Error) => void
      h.setReceipt(
        new Promise<void>((res, rej) => {
          apply = res
          fail = rej
        }),
      )
      h.handlers.get(h.subs[0]!)!.onReset() // below the floor: truncate, queued
      expect(await outcome(orphan)).toBe("pending")
      if (outcomeOfTruncate === "applied") {
        apply()
        expect(await outcome(orphan)).toBe("resolved")
      } else {
        fail(new Error("truncate aborted"))
        expect(await outcome(orphan)).toBe("rejected: truncate aborted")
      }
    }
  })

  it("a request waiting on a pending watch fails with it when that watch's subscribe fails", async () => {
    const h = harness({
      snaps: () => true,
      failSub: (_where, limit) => (limit === undefined ? new Error("connect failed") : null),
    })
    const opener = h.res.loadSubset({ where: eqBody("x") })
    const waiter = h.res.loadSubset({ where: eqBody("x"), limit: 5 }) // covered by the pending watch
    expect(await outcome(opener)).toBe("rejected: connect failed")
    expect(await outcome(waiter)).toBe("rejected: connect failed") // not left loading forever
  })
})
