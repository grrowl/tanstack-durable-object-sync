import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: `_sync_subs` (ADR-0019) is socket EPHEMERA, not data — the table must
// hold exactly the live subscriptions of currently-open sockets. ADR-0019 keeps
// that invariant with three seams: `unsub` DELETEs the row, `webSocketClose`/
// `webSocketError` DELETE the socket's rows, and `sweepOrphanSubs` (riding
// compaction) drops rows for sockets that died without a close. A downstream
// field report (agent-canvas, against 0.6.0) observed the table growing to 44
// rows over one interactive session and asked whether a facet still leaks on a
// LIVE socket — one that never closes, so neither close-cleanup nor the orphan
// sweep can catch it. The only way rows pile up on a live socket is if the
// client sends `sub` frames with fresh subIds and no matching `unsub`.
//
// These pins drive the REAL client paths (WebSocketTransport + doCollectionOptions
// sync lifecycle) against a REAL Durable Object and assert, via `_sync_subs`,
// that the durable row count keyed to live sockets always equals the number of
// genuinely live subscriptions — across the exact churn the field report named:
// eager mount/unmount/remount with fresh subIds, on-demand replaced where-shapes,
// and reconnect resubscribe.

const SOCKET_ID_TAG_PREFIX = "_tddc.sid:"
const SYNC_TAG = "_tddc"

interface SubsView {
  /** Total rows in `_sync_subs`. */
  total: number
  /** Rows whose socket_id belongs to a currently-live sync socket. */
  live: number
  /** Rows whose socket_id belongs to NO live socket (leaked/awaiting sweep). */
  orphan: number
  /** Number of live sync sockets. */
  sockets: number
}

type Stub = ReturnType<DurableObjectNamespace["get"]>

async function liveSids(stub: Stub): Promise<Array<string>> {
  return runInDurableObject(stub, (_i, s) => {
    const sids: Array<string> = []
    for (const ws of s.getWebSockets(SYNC_TAG)) {
      for (const tag of s.getTags(ws)) {
        if (tag.startsWith(SOCKET_ID_TAG_PREFIX)) sids.push(tag.slice(SOCKET_ID_TAG_PREFIX.length))
      }
    }
    return sids
  })
}

async function subsView(stub: Stub): Promise<SubsView> {
  return runInDurableObject(stub, (_i, s) => {
    const live = new Set<string>()
    for (const ws of s.getWebSockets(SYNC_TAG)) {
      for (const tag of s.getTags(ws)) {
        if (tag.startsWith(SOCKET_ID_TAG_PREFIX)) live.add(tag.slice(SOCKET_ID_TAG_PREFIX.length))
      }
    }
    const rows = Array.from(
      s.storage.sql.exec<{ socket_id: string }>("SELECT socket_id FROM _sync_subs"),
    )
    let liveCount = 0
    for (const r of rows) if (live.has(r.socket_id)) liveCount++
    return { total: rows.length, live: liveCount, orphan: rows.length - liveCount, sockets: live.size }
  })
}

async function subIdsForSid(stub: Stub, sid: string): Promise<Array<string>> {
  return runInDurableObject(stub, (_i, s) =>
    Array.from(
      s.storage.sql.exec<{ sub_id: string }>("SELECT sub_id FROM _sync_subs WHERE socket_id = ?", sid),
    ).map((r) => r.sub_id),
  )
}

async function waitFor(pred: () => boolean | Promise<boolean>, timeoutMs = 4000): Promise<void> {
  const start = Date.now()
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

/** A real transport whose socket is a real DO client socket (as the browser's
 *  would be). Returns the opened sockets so a test can assert reconnects. */
function realTransport(room: string, reconnectDelay = 20) {
  const sockets: Array<WebSocketLike & { close: () => void }> = []
  const transport = new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    reconnectDelay,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      const like = ws as unknown as WebSocketLike & { close: () => void }
      sockets.push(like)
      return like
    },
  })
  return { transport, sockets }
}

/** Like `realTransport`, but the FIRST socket open blocks until `release()` is
 *  called — lets a test churn subscribe/unsubscribe while the initial connect is
 *  still pending (`this.ws === null`), the exact window the field-report leak
 *  lived in. */
function gatedTransport(room: string) {
  const sockets: Array<WebSocketLike & { close: () => void }> = []
  let release!: () => void
  const gate = new Promise<void>((r) => (release = r))
  let gated = true
  const transport = new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    reconnectDelay: 20,
    open: async () => {
      if (gated) {
        gated = false
        await gate
      }
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      const like = ws as unknown as WebSocketLike & { close: () => void }
      sockets.push(like)
      return like
    },
  })
  return { transport, sockets, release }
}

/** Minimal SyncParams for an EAGER collection: a real syncedData Map (the
 *  reconcile pass reads `collection._state.syncedData`), begin/write/commit that
 *  keep it coherent, and no-op ready hooks. The collection lifecycle is what we
 *  test; the store is only here so sync() runs its real subscribe/cleanup path. */
function eagerControls(getKey: (r: { id: string }) => string) {
  const syncedData = new Map<string, unknown>()
  return {
    collection: { get: (k: string) => syncedData.get(k), _state: { syncedData } },
    begin: () => {},
    write: (m: { type: string; value?: unknown; key?: string }) => {
      if (m.type === "delete") syncedData.delete(m.key as string)
      else syncedData.set(getKey(m.value as { id: string }), m.value)
    },
    commit: () => true,
    markReady: () => {},
    markError: () => {},
    truncate: () => syncedData.clear(),
  }
}

/** Minimal SyncParams for an ON-DEMAND collection (subset subs never reconcile,
 *  so no syncedData is required). */
function onDemandControls() {
  return {
    collection: { get: () => undefined },
    begin: () => {},
    write: () => {},
    commit: () => true,
    markReady: () => {},
    markError: () => {},
    truncate: () => {},
  }
}

type EagerSync = (params: unknown) => () => void
type OnDemandSync = (params: unknown) => {
  loadSubset: (o: { where?: unknown }) => true | Promise<void>
  unloadSubset: (o: { where?: unknown }) => void
  cleanup: () => void
}
const eagerSyncOf = (cfg: unknown): EagerSync => (cfg as { sync: { sync: EagerSync } }).sync.sync
const onDemandSyncOf = (cfg: unknown): OnDemandSync => (cfg as { sync: { sync: OnDemandSync } }).sync.sync

const whereEq = (field: string, value: unknown): unknown => ({
  type: "func",
  name: "eq",
  args: [
    { type: "ref", path: [field] },
    { type: "val", value },
  ],
})

describe("_sync_subs row count == live subscriptions across churn (ADR-0019)", () => {
  // THE CRUX (exact field-report race, fixed by the ghost-sub guard in 227c2aa).
  // Before that guard, tagged 0.6.0's `subscribe()` sent its `sub` frame
  // UNCONDITIONALLY after `await connect()`. So a mount/unmount that both
  // happened while the initial socket was still opening left the handler
  // deleted (the `unsub` was dropped: `this.ws` was null) — yet the parked
  // `subscribe()` still fired its `sub` when the socket finally opened,
  // persisting a durable row for a subId with NO live handler. Repeated across a
  // burst of mounts during one pending connect, this is exactly how `_sync_subs`
  // grew to dozens of rows on a single never-closing socket. The guard
  // (`if (!this.handlers.has(subId)) return`) drops the send for an
  // already-unsubscribed sub, so no ghost row is ever created.
  it("does NOT create ghost rows for subs unsubscribed while the initial connect is pending", async () => {
    const room = "subs-live-ghost-race"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const { transport, sockets, release } = gatedTransport(room)

    // Churn many fresh-subId mounts+unmounts while the socket is still opening.
    const BURST = 12
    for (let i = 0; i < BURST; i++) {
      const cfg = doCollectionOptions<TestApi, "messages">({ transport, table: "messages", getKey: (r) => r.id })
      const cleanup = eagerSyncOf(cfg)(eagerControls((r) => r.id)) // subscribe() parks on connect()
      cleanup() // unsubscribe() while this.ws === null — the dropped-unsub window
    }
    // One mount left genuinely LIVE across the same pending connect.
    const liveCfg = doCollectionOptions<TestApi, "messages">({ transport, table: "messages", getKey: (r) => r.id })
    eagerSyncOf(liveCfg)(eagerControls((r) => r.id))

    // Open the socket; every parked subscribe() now resumes.
    release()
    await waitFor(() => sockets.length === 1)
    // The one live sub lands; the BURST ghosts must be dropped by the guard.
    await waitFor(async () => (await subsView(stub)).live === 1)
    // Settle any stragglers, then prove no ghost rows crept in.
    await new Promise((r) => setTimeout(r, 100))

    const v = await subsView(stub)
    expect(v.live).toBe(1) // only the genuinely-live sub
    expect(v.total).toBe(1) // NOT 1 + BURST ghosts
    expect(v.sockets).toBe(1)

    transport.close()
  })

  // The field-report shape: an eager collection recreated across mounts (no
  // useMemo / StrictMode double-mount / navigation churn) generates a FRESH
  // eagerSubId every mount (`${table}#${++subSeq}`). Each mount subscribes and
  // each unmount unsubscribes ON THE SAME live socket — so distinct subIds must
  // never pile up: the durable table returns to zero between mounts and holds
  // exactly one row while one collection is mounted.
  it("eager mount/unmount/remount with fresh subIds does not accumulate on a live socket", async () => {
    const room = "subs-live-eager-churn"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const { transport } = realTransport(room)
    await transport.connect()

    const seenSubIds = new Set<string>()
    // One socket exists after connect(); capture its id so we can read its rows.
    await waitFor(async () => (await liveSids(stub)).length === 1)
    const sid = (await liveSids(stub))[0]!

    const CYCLES = 8
    for (let i = 0; i < CYCLES; i++) {
      const cfg = doCollectionOptions<TestApi, "messages">({ transport, table: "messages", getKey: (r) => r.id })
      const cleanup = eagerSyncOf(cfg)(eagerControls((r) => r.id))
      // The sub is durable once the server has handled the `sub` frame.
      await waitFor(async () => (await subsView(stub)).live === 1)
      // Each mount is a FRESH eagerSubId (`${table}#${++subSeq}`) — the non-
      // memoized / StrictMode-remount shape the field report named.
      const [subId] = await subIdsForSid(stub, sid)
      expect(seenSubIds.has(subId!)).toBe(false)
      seenSubIds.add(subId!)
      cleanup()
      await waitFor(async () => (await subsView(stub)).live === 0)
    }

    // One final mount left live: exactly one row, still ONE socket the whole time.
    const cfg = doCollectionOptions<TestApi, "messages">({ transport, table: "messages", getKey: (r) => r.id })
    eagerSyncOf(cfg)(eagerControls((r) => r.id))
    await waitFor(async () => (await subsView(stub)).live === 1)

    const v = await subsView(stub)
    expect(v.live).toBe(1)
    expect(v.total).toBe(1) // no orphans, no accumulation
    expect(v.sockets).toBe(1) // all churn rode ONE socket — the live-socket case
    expect(seenSubIds.size).toBe(CYCLES) // genuinely distinct subIds churned, yet never piled up

    transport.close()
  })

  // Replaced where-shapes (on-demand): a live query whose filter changes makes
  // TanStack load the new subset and unload the old. Distinct `where`s are
  // distinct refcounted subs (`${table}#${JSON.stringify(where)}`); the replaced
  // one's row must be released. Churn many shapes and assert the table tracks
  // only the currently-loaded subsets.
  it("on-demand replaced where-shapes release the old subset's row", async () => {
    const room = "subs-live-ondemand-churn"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const { transport } = realTransport(room)
    const res = onDemandSyncOf(
      doCollectionOptions<TestApi, "messages">({
        transport,
        table: "messages",
        getKey: (r) => r.id,
        syncMode: "on-demand",
      }),
    )(onDemandControls())

    // Core releases each load with the options object it loaded (ADR-0023).
    let prev = { where: whereEq("body", "shape-0") }
    await res.loadSubset(prev)
    await waitFor(async () => (await subsView(stub)).live === 1)

    // Replace the where-shape repeatedly: load the next, unload the previous —
    // exactly one subset live at any time.
    for (let i = 1; i <= 8; i++) {
      const next = { where: whereEq("body", `shape-${i}`) }
      await res.loadSubset(next)
      res.unloadSubset(prev)
      prev = next
      await waitFor(async () => (await subsView(stub)).live === 1)
      const v = await subsView(stub)
      expect(v.total).toBe(1) // never two live shapes, never a leaked row
    }

    // Unload the last: the table empties while the socket stays open.
    res.unloadSubset(prev)
    await waitFor(async () => (await subsView(stub)).live === 0)
    expect((await subsView(stub)).total).toBe(0)
    expect((await subsView(stub)).sockets).toBe(1)

    transport.close()
  })

  // Reconnect resubscribe: a drop opens a NEW socket (fresh socket_id tag) and
  // `resubscribeAll` re-sends every live sub under the SAME subId. The invariant
  // that matters for accumulation is that the NEW socket carries exactly the live
  // set (two rows, the same subIds) — reconnect reuses subIds, so a reconnecting
  // client never multiplies its rows. (The OLD socket's rows are cleaned by
  // webSocketClose on a real drop / by the orphan sweep on a hard death — pinned
  // in ws-lifecycle and hibernation tests. This harness's abandoned client end
  // never completes the closing handshake, so we assert on the new socket, whose
  // row set is what a leak would inflate.)
  it("reconnect resubscribe reuses subIds — the new socket carries exactly the live set", async () => {
    const room = "subs-live-reconnect"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const { transport, sockets } = realTransport(room)
    await transport.connect()

    // Two distinct live subs on one socket (subIds shaped as doCollectionOptions
    // emits them). Driven through the transport directly so the reconnect path is
    // exercised without the collection lifecycle.
    const noop = {
      onSnap: () => {},
      onSnapEnd: () => {},
      onDelta: () => {},
      onUptodate: () => {},
      onReset: () => {},
    }
    await transport.subscribe("messages#a", "messages", noop)
    await transport.subscribe("messages#b", "messages", noop)
    await waitFor(async () => {
      const v = await subsView(stub)
      return v.live === 2 && v.sockets === 1
    })
    const [oldSid] = await liveSids(stub)
    const socketsBefore = sockets.length

    // Force a realistic drop (server closes the socket → client auto-reconnects).
    await runInDurableObject(stub, (_i, s) => {
      for (const ws of s.getWebSockets()) ws.close(1000, "drop")
    })

    // Wait for the reconnect on the CHEAP client-side signal — a new socket was
    // opened — so the poll does not contend with the DO accepting that socket.
    await waitFor(() => sockets.length > socketsBefore, 6000)

    // Wait for the resubscribe to land on the NEW socket, then assert that socket
    // carries exactly the two live subs under their ORIGINAL subIds — resubscribe
    // reused them, so the reconnect added no net rows for the live connection.
    await waitFor(async () => {
      const sids = await liveSids(stub)
      const fresh = sids.find((s) => s !== oldSid)
      return fresh !== undefined && (await subIdsForSid(stub, fresh)).length === 2
    }, 6000)
    const newSid = (await liveSids(stub)).find((s) => s !== oldSid)!
    const newRows = (await subIdsForSid(stub, newSid)).sort()
    expect(newRows).toEqual(["messages#a", "messages#b"]) // same subIds, no growth

    transport.close()
  }, 15000) // real reconnect + resubscribe + settle needs headroom over the 5s default
})
