import { describe, expect, it } from "vitest"
import { doCollectionOptions, type DoSyncMeta } from "../src/client/do-collection.ts"
import type { Transport } from "../src/client/transport.ts"

// WHY (ADR-0011 D3, adapter-level ordering): on-demand readiness must GATE on
// the transient hydration catch-up sub being SENT — loadSubset subs only fire
// after ready, so on the single ordered socket the catch-up's truncate/deltas
// always precede subset snapshots. A markReady racing ahead (the bug: its
// connect().then() was registered first) lets a subset snapshot land at a seq
// the catch-up then stomps over — or, below the floor, lets the catch-up's
// truncate WIPE an already-loaded subset. Also pins the syncMeta hook
// contract: export shape, import validation, where-fingerprint downgrade,
// min-merge.

interface Msg {
  id: string
  body: string
}

/** Structural schema Api the branded transport carries, so `doCollectionOptions`
 *  infers Row = Msg for `table: "messages"` (matches `RowOf`/`CollectionName`). */
type Api = { collections: { messages: { __row?: Msg } } }

type Hooked = {
  sync: {
    sync: (p: unknown) => unknown
    exportSyncMeta: () => DoSyncMeta
    importSyncMeta: (m: unknown) => void
    mergeSyncMeta: (a: unknown, b: unknown) => DoSyncMeta
  }
}

function spyTransport(calls: Array<string>): Transport<Api> {
  return {
    connect: async () => {
      calls.push("connect")
    },
    subscribe: async (subId, _collection, _handler, _where, _orderBy, _limit, since) => {
      calls.push(`sub:${subId}:since=${since ?? "none"}`)
    },
    unsubscribe: (subId: string) => {
      calls.push(`unsub:${subId}`)
    },
    sendMut: () => Promise.reject(new Error("unused")),
    fetch: () => Promise.reject(new Error("unused")),
    close: () => {},
    appliedCursor: "7",
    hasPosition: true,
    seedCursor: () => {
      calls.push("seed")
    },
  }
}

const controls = {
  collection: { get: () => undefined },
  begin: () => {},
  write: () => {},
  commit: () => {},
  truncate: () => {},
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0))

describe("hydrated on-demand start ordering", () => {
  it("ready waits for the catch-up sub to be SENT; the catch-up precedes any subset sub", async () => {
    const calls: Array<string> = []
    const opts = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    opts.sync.importSyncMeta({ v: 1, cursor: "5" })
    opts.sync.sync({ ...controls, markReady: () => calls.push("ready") })
    await flush()

    const catchup = calls.findIndex((c) => c.startsWith("sub:messages#hydrate#") && c.endsWith("since=5"))
    const ready = calls.indexOf("ready")
    expect(catchup).toBeGreaterThanOrEqual(0)
    expect(ready).toBeGreaterThan(catchup)
  })

  it("without hydration there is no catch-up sub and ready follows connect", async () => {
    const calls: Array<string> = []
    const opts = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    opts.sync.sync({ ...controls, markReady: () => calls.push("ready") })
    await flush()
    expect(calls.filter((c) => c.startsWith("sub:"))).toEqual([])
    expect(calls).toContain("ready")
  })

  it("no resume point ('0'): hydrated rows are truncated, not left to go stale", async () => {
    const calls: Array<string> = []
    const truncated: Array<string> = []
    const opts = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    opts.sync.importSyncMeta({ v: 1, cursor: "0" })
    opts.sync.sync({
      ...controls,
      truncate: () => truncated.push("truncate"),
      markReady: () => calls.push("ready"),
    })
    await flush()
    expect(truncated).toEqual(["truncate"])
    expect(calls.filter((c) => c.startsWith("sub:"))).toEqual([]) // no unfiltered full snapshot
    expect(calls).toContain("ready")
  })
})

describe("syncMeta hooks", () => {
  const eq = (field: string, value: unknown): unknown => ({
    type: "func",
    name: "eq",
    args: [
      { type: "ref", path: [field] },
      { type: "val", value },
    ],
  })

  function makeOpts(where?: unknown): Hooked {
    return doCollectionOptions({
      transport: spyTransport([]),
      table: "messages",
      getKey: (r) => r.id,
      where,
    }) as unknown as Hooked
  }

  it("export round-trips through import; the eager where is fingerprinted", () => {
    const a = makeOpts(eq("body", "keep"))
    const meta = a.sync.exportSyncMeta()
    expect(meta).toMatchObject({ v: 1, cursor: "7" })
    expect(typeof meta.where).toBe("string")
    a.sync.importSyncMeta(meta) // same fingerprint: accepted (no throw)
  })

  it("a DIFFERENT where downgrades the cursor to the snapshot-reconcile path", async () => {
    const calls: Array<string> = []
    const renderSide = makeOpts(eq("body", "keep"))
    const meta = renderSide.sync.exportSyncMeta()

    const clientSide = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      where: eq("body", "other"),
    }) as unknown as Hooked
    clientSide.sync.importSyncMeta(meta)
    expect(calls).not.toContain("seed") // an unsound cursor is never claimed
    clientSide.sync.sync({ ...controls, markReady: () => {} })
    await flush()
    // The eager sub must NOT resume from the foreign cursor.
    expect(calls.some((c) => c.startsWith("sub:") && c.endsWith("since=none"))).toBe(true)
  })

  it("rejects meta it does not understand — never resumes from garbage", () => {
    const o = makeOpts()
    expect(() => o.sync.importSyncMeta({ v: 2, cursor: "5" })).toThrow(/unrecognized sync meta/)
    expect(() => o.sync.importSyncMeta({ v: 1, cursor: "not-a-seq" })).toThrow()
    expect(() => o.sync.importSyncMeta(null)).toThrow(/unrecognized sync meta/)
  })

  it("unrecognized meta fails loud BUT safe: the rows already landed, so sync still reconciles", async () => {
    // Upstream applies the chunk's rows BEFORE importSyncMeta — a throw can't
    // veto them. If the throw also skipped our bookkeeping, sync would start
    // down the non-hydrated path and a server-deleted hydrated row would be
    // stale forever. The throw must leave the safe state behind: no resume
    // point ("0") → snapshot + reconcile.
    const calls: Array<string> = []
    const o = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
    }) as unknown as Hooked
    expect(() => o.sync.importSyncMeta({ v: 99, cursor: "5" })).toThrow(/unrecognized sync meta/)
    expect(calls).not.toContain("seed") // a cursor we can't read is never claimed
    o.sync.sync({ ...controls, markReady: () => {} })
    await flush()
    // Snapshot path (no since) — where the always-armed eager reconcile lives.
    expect(calls.some((c) => c.startsWith("sub:") && c.endsWith("since=none"))).toBe(true)
  })

  it("merge takes the EARLIER cursor — replay is idempotent, skipping is not", () => {
    const o = makeOpts()
    const merged = o.sync.mergeSyncMeta({ v: 1, cursor: "90" }, { v: 1, cursor: "100" })
    expect(merged.cursor).toBe("90")
    expect(o.sync.mergeSyncMeta({ v: 1, cursor: "100" }, { v: 1, cursor: "90" }).cursor).toBe("90")
  })

  it("a NEGATIVE cursor is rejected — and on-demand still lands on the safe truncate path", async () => {
    // codex finding: BigInt("-1") parses, seedCursor ignores it, but
    // `since:"-1"` on the wire makes the server answer a full snapshot the
    // catch-up handler discards — no terminal, the transient sub never tears
    // down, stale hydrated rows survive forever. Parse must refuse it; the
    // fail-loud-but-safe contract then routes sync start to "0" (truncate).
    const calls: Array<string> = []
    const truncated: Array<string> = []
    const o = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    expect(() => o.sync.importSyncMeta({ v: 1, cursor: "-1" })).toThrow(/negative/)
    expect(calls).not.toContain("seed")
    o.sync.sync({ ...controls, truncate: () => truncated.push("truncate"), markReady: () => {} })
    await flush()
    expect(truncated).toEqual(["truncate"]) // safe "0" route, not a since:"-1" catch-up
    expect(calls.filter((c) => c.startsWith("sub:"))).toEqual([])
  })

  it("merge with MISMATCHED fingerprints yields cursor '0' — no sound joint resume point", () => {
    // codex finding: MIN alone can return the side whose fingerprint matches
    // ours while the OTHER side's foreign-filter rows were already applied —
    // import would then accept a cursor whose catch-up never covers them.
    const o = makeOpts() // our fingerprint: undefined
    const merged = o.sync.mergeSyncMeta({ v: 1, cursor: "50", where: "AAA" }, { v: 1, cursor: "100", where: "BBB" })
    expect(merged.cursor).toBe("0")
    expect(merged.where).toBeUndefined() // OUR fingerprint, so import routes it to "0"
    // Same fingerprints (even a foreign one) still MIN — import does the
    // ours-vs-theirs check.
    expect(o.sync.mergeSyncMeta({ v: 1, cursor: "50", where: "AAA" }, { v: 1, cursor: "100", where: "AAA" }).cursor).toBe("50")
  })
})

describe("commit receipts (0.8.5 SyncAppliedReceipt)", () => {
  /** Spy transport that CAPTURES subscribe handlers so a test can drive them. */
  function capturingTransport(calls: Array<string>, handlers: Map<string, import("../src/client/transport.ts").SubHandler>): Transport<Api> {
    const t = spyTransport(calls)
    return {
      ...t,
      subscribe: async (subId, _c, handler) => {
        calls.push(`sub:${subId}`)
        handlers.set(subId, handler)
      },
    }
  }

  it("a REJECTED receipt fails the subset load — an aborted application is not success", async () => {
    const calls: Array<string> = []
    const handlers = new Map<string, import("../src/client/transport.ts").SubHandler>()
    const opts = doCollectionOptions({
      transport: capturingTransport(calls, handlers),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    const res = opts.sync.sync({
      ...controls,
      commit: () => Promise.reject(new Error("aborted application")),
      markReady: () => {},
      markError: () => {},
    } as never) as { loadSubset: (o: unknown) => true | Promise<void> }
    await flush()
    const load = res.loadSubset({}) as Promise<void>
    const settled = load.then(
      () => "resolved",
      (e: Error) => `rejected:${e.message}`,
    )
    await flush()
    const [h] = [...handlers.values()] // the subset's watch (fresh subId per watch, ADR-0023)
    h!.onSnap(undefined, { id: "a", body: "x" })
    h!.onSnapEnd() // flush → rejected receipt
    await expect(settled).resolves.toBe("rejected:aborted application")
  })

  it("exportSyncMeta never claims a boundary whose receipt is unsettled", async () => {
    // codex finding: the transport cursor advances at the boundary, but the
    // commit's application can settle later — a dehydrate in that window would
    // serialize pre-boundary rows under meta claiming the boundary.
    const calls: Array<string> = []
    const handlers = new Map<string, import("../src/client/transport.ts").SubHandler>()
    let settle!: () => void
    const receipt = new Promise<void>((r) => {
      settle = r
    })
    const opts = doCollectionOptions({
      transport: capturingTransport(calls, handlers),
      table: "messages",
      getKey: (r) => r.id,
    }) as unknown as Hooked
    expect(opts.sync.exportSyncMeta().cursor).toBe("7") // nothing pending: the live claim
    opts.sync.sync({
      ...controls,
      collection: { get: () => undefined, _state: { syncedData: new Map() } },
      commit: () => receipt,
      markReady: () => {},
      markError: () => {},
    } as never)
    await flush()
    const h = handlers.get([...handlers.keys()][0]!)!
    h.onSnap(undefined, { id: "a", body: "x" })
    h.onSnapEnd() // flush → pending receipt
    expect(opts.sync.exportSyncMeta().cursor).toBe("0") // under-claim, never the unproven "7"
    settle()
    await flush()
    expect(opts.sync.exportSyncMeta().cursor).toBe("7") // settled: live claim again
  })
})
