import * as db from "@tanstack/db"
import { BTreeIndex, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: realistic on-demand sessions, end to end (live queries → loadSubset →
// real WebSocketTransport → real DO), pinning ADR-0023 fixes that were only
// pinned by fake transports or not at all (bug bash 2026-09-29, K8: C3, C5, C7,
// C8). Each assertion names the bug it guards. Run at both ends of the
// @tanstack/db matrix (ADR-0022): some regressions bite on one version only,
// and the comments say which.

// @tanstack/db 0.9.0 removed the subset-algebra helpers, so their presence
// marks the 0.8.x floor. Used only where upstream itself differs.
const db08 = "isWhereSubset" in db

const room = (tag: string): string => `ods-${tag}-${crypto.randomUUID()}`
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
const dropSocket = (r: string) =>
  runInDurableObject(stubFor(r), (_i, state) => {
    for (const sock of state.getWebSockets()) sock.close(1000, "drop")
  })

interface Gate {
  hold: boolean
  queue: Array<() => void>
  /** Hold only frames of these types (default: every frame). */
  only?: ReadonlyArray<string>
}
const codec = createFrameCodec()
/** A real transport to the room. `frames` records every sub the client opens,
 *  `pages` every fetch it sends, `paged` the ids of every row a page brought;
 *  `gate` holds inbound frames (in order). */
function transportFor(
  r: string,
  opts: { frames?: Array<string>; pages?: Array<Promise<unknown>>; paged?: Set<string>; gate?: Gate } = {},
): WebSocketTransport<TestApi> {
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
  if (opts.pages) {
    const { pages, paged } = opts
    const fetch = t.fetch.bind(t)
    t.fetch = (frame) => {
      const p = fetch(frame)
      pages.push(p.then((rows) => rows.forEach((row) => paged?.add((row as { id: string }).id))))
      return p
    }
  }
  return t
}
const onDemand = (t: WebSocketTransport<TestApi>) =>
  createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }))
type Windowed = { utils: { setWindow: (w: { offset?: number; limit?: number }) => true | Promise<void> } }
const bodies = (q: { toArray: Array<{ body: string }> }): Array<string> => q.toArray.map((m) => m.body)
const ids = (q: { toArray: Array<{ id: string }> }): Array<string> => q.toArray.map((m) => m.id).sort()

describe("filtered lists on a flaky socket", () => {
  it("a filter abandoned mid-snapshot leaves nothing; a below-floor reconnect reloads every open filter", async () => {
    const r = room("lists")
    const gate: Gate = { hold: false, queue: [], only: ["snap-end"] }
    const frames: Array<string> = []
    const t = transportFor(r, { frames, gate })
    await seed(r, ["a", "a", "b", "b", "c", "c"]) // m00 m01: a · m02 m03: b · m04 m05: c
    const messages = onDemand(t)
    const qa = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "a")))
    const qb = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "b")))
    await qa.preload()
    await qb.preload()
    await waitFor(() => qa.size === 2 && qb.size === 2)

    // C3: open a third filter; its snap rows reach the handler (inside the open
    // sync transaction) but its snap-end is held; the user navigates away.
    gate.hold = true
    const qc = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "c")))
    void qc.preload().catch(() => {})
    await waitFor(() => gate.queue.length === 1) // snap m04, m05 dispatched ahead of it
    expect(messages.isLoadingSubset).toBe(true)
    await qc.cleanup()
    // bugbash C3: the release settles the load (do-collection unloadSubset's
    // `acq.settle()`); pre-fix it stayed loading forever.
    await waitFor(() => !messages.isLoadingSubset)
    gate.hold = false
    for (const deliver of gate.queue.splice(0)) deliver() // the stale snap-end: no handler now
    // A later commit boundary on another filter (a live delta to qa).
    await serverExec(r, "INSERT INTO messages(id,body) VALUES(?,?)", "m06", "a")
    await waitFor(() => qa.size === 3)
    await sleep(30)
    // bugbash C3: the partial snapshot was dropped with its hold (unpin's
    // `dropHolds`), not committed by the release or by qa's boundary.
    expect([...messages.keys()].sort()).toEqual(["m00", "m01", "m02", "m03", "m06"])

    // C5: the socket dies with deltas unread, and the changelog is pruned below
    // the client's cursor, so the reconnect's resubscribes are both reset.
    const subsBefore = frames.length
    gate.only = undefined
    gate.hold = true
    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m01")
    await serverExec(r, "INSERT INTO messages(id,body) VALUES(?,?)", "x1", "b")
    await runInDurableObject(stubFor(r), (_i, s) => {
      s.storage.sql.exec("DELETE FROM _sync_changes")
    })
    gate.queue.length = 0 // these deltas die with the socket
    gate.hold = false
    await dropSocket(r)
    // bugbash C5: every open filter reloads the current server rows. The fix
    // (`truncateAll` on a bootstrapped sub's reset: one truncate, every watch
    // retired, a new generation) makes core's truncate replay reopen each demand
    // with a fresh sub. Reverting it to a plain truncate fails this on the 0.8.x
    // floor ONLY: 0.8 answers its load-then-release replay from the refcount (no
    // new sub), while 0.9's release-then-reload replay rescues it.
    await waitFor(() => qa.size === 2 && qb.size === 3, 4000)
    await sleep(100)
    expect(ids(qa)).toEqual(["m00", "m06"])
    expect(ids(qb)).toEqual(["m02", "m03", "x1"])
    expect([...messages.keys()].sort()).toEqual(["m00", "m02", "m03", "m06", "x1"])
    expect(frames.length - subsBefore).toBe(2) // one fresh sub per open filter, no storm
    t.close()
  })
})

describe("offset windows through a real live query", () => {
  for (const indexed of [false, true]) {
    it(`page 2 of a sorted list shows its own rows beside page 1 (index: ${indexed})`, async () => {
      const r = room(`offset-${indexed}`)
      const t = transportFor(r)
      await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1)))
      const messages = onDemand(t)
      if (indexed) messages.createIndex((m) => m.body, { indexType: BTreeIndex })
      const page1 = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, "asc").limit(3))
      await page1.preload()
      await waitFor(() => page1.size === 3)
      // Core folds a query's offset into the request's limit ({limit: 8}): the
      // adapter never sees a bare offset (it rejects one loudly).
      const page2 = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, "asc").offset(5).limit(3))
      await page2.preload()
      // bugbash C7: page 2 is its own request (identity is where + orderBy +
      // limit, `requestKey`); pre-fix it shared page 1's 3-row snapshot and
      // showed nothing.
      await waitFor(() => page2.size === 3)
      await sleep(30)
      expect(bodies(page1)).toEqual(["01", "02", "03"])
      expect(bodies(page2)).toEqual(["06", "07", "08"])
      // An offset jump: 0.9 fills it. 0.8.x does not (upstream: no request for
      // an unindexed window; one cursor request {limit: 3, offset: 8} for an
      // indexed one, which any exact adapter answers with 09..11), so skip there.
      if (db08) return t.close()
      const p = (page2 as unknown as Windowed).utils.setWindow({ offset: 10, limit: 3 })
      if (p !== true) await p
      await waitFor(() => bodies(page2).join() === "11,12,13")
      expect(bodies(page1)).toEqual(["01", "02", "03"])
      t.close()
    })
  }
})

describe("a paged list: left mid-scroll, then back with a colliding create", () => {
  it("a page landing after unmount installs nothing; a page row is written under an optimistic overlay", async () => {
    const r = room("paged")
    const gate: Gate = { hold: false, queue: [] }
    const pages: Array<Promise<unknown>> = []
    const paged = new Set<string>()
    const t = transportFor(r, { pages, paged, gate })
    await seed(r, Array.from({ length: 20 }, (_, i) => pad(i + 1))) // m00 "01" .. m19 "20"
    const messages = onDemand(t)
    // Growing this window pages from the DO: on 0.8.6 a default-sort indexed
    // window grows by CURSOR request (loadMore); on 0.9.2 a lexical one by
    // covered tie + prefix fetches (a default locale sort is loaded whole at
    // mount there, so nothing would page). Both install through installPage.
    const sort = db08 ? ({ direction: "desc" } as const) : ({ direction: "desc", stringSort: "lexical" } as const)
    if (db08) messages.createIndex((m) => m.body, { indexType: BTreeIndex })
    else messages.createIndex((m) => m.body, { indexType: BTreeIndex, options: { compareOptions: { stringSort: "lexical" } } })
    const mount = async () => {
      const win = createLiveQueryCollection((q) => q.from({ m: messages }).orderBy(({ m }) => m.body, sort).limit(5))
      await win.preload()
      await waitFor(() => win.size === 5 && !messages.isLoadingSubset) // incl. 0.9's boundary-tie fetch
      return win
    }
    const grow = (win: unknown, limit: number) => Promise.resolve((win as Windowed).utils.setWindow({ offset: 0, limit }))

    // C8 late page: scroll (the page is held), then leave before it lands.
    const win1 = await mount()
    gate.only = ["page"]
    gate.hold = true
    void grow(win1, 10).catch(() => {}) // core aborts it on cleanup
    await waitFor(() => gate.queue.length >= 1) // a page is in flight
    await win1.cleanup() // the watch is released, its rows dropped
    await waitFor(() => messages.size === 0)
    gate.hold = false
    for (const deliver of gate.queue.splice(0)) deliver() // the late page
    await Promise.all(pages.splice(0))
    await sleep(50)
    // bugbash C8 (late-page half): nothing holds these rows, so none install.
    // The guard differs by version: 0.8.6's in-flight request is a cursor page
    // (loadMore's `o.signal?.aborted`), 0.9.2's are covered fetches
    // (fetchCovered's `acq.released`); reverting either fails its version.
    expect([...messages.keys()]).toEqual([])

    // C8 overlay: back on the list, the user posts a new item as m10 (it tops
    // the list; the id already exists on the DO, so the insert will be
    // rejected; `rejected` is held), then scrolls: the page carrying the
    // server's m10 lands under the overlay. (A draft sorting BELOW the loaded
    // rows would not do: 0.8.6 pages its index cursor from the optimistic row.)
    const win2 = await mount()
    expect(messages.get("m10")).toBeUndefined() // "11": below the window, not loaded
    gate.only = ["rejected"]
    gate.hold = true
    const tx = messages.insert({ id: "m10", body: "99-draft" })
    tx.isPersisted.promise.catch(() => {})
    void grow(win2, 11).catch(() => {}) // six visible (draft + 20..16), so the page reaches "11"
    await waitFor(() => paged.has("m10"))
    await sleep(20) // installPage ran
    // Hold later pages: on 0.9 the rollback (a source change) starts core's
    // full-source recovery fetch, which would re-deliver m10 and mask a
    // regression here with one extra round trip.
    gate.only = ["page"]
    for (const deliver of gate.queue.splice(0)) deliver() // rejected: the overlay rolls back
    await expect(tx.isPersisted.promise).rejects.toThrow()
    // bugbash C8 (overlay half): installPage writes insert-if-absent against
    // SYNCED state (`syncedHas`). Pre-fix it checked `collection.get`, which
    // sees the optimistic row, so the server row was skipped and was gone once
    // the overlay rolled back.
    await waitFor(() => (messages.get("m10") as { body?: string } | undefined)?.body === "11")
    gate.hold = false
    for (const deliver of gate.queue.splice(0)) deliver()
    // The window shows the server's m10 in place of the draft. Upstream-only
    // gate: 0.8.6 live queries miss a sync commit that lands with an optimistic
    // insert's rollback of the same key (reproduced without this adapter; its
    // collection is right, the query keeps the draft), so check 0.9 only.
    // Filling the 11th slot the rollback vacated is core's business too.
    if (db08) return t.close()
    await waitFor(() => bodies(win2).slice(0, 10).join() === "20,19,18,17,16,15,14,13,12,11")
    t.close()
  })
})
