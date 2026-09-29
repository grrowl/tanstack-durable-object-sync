import { env, evictDurableObject, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"

// WHY: two ACCEPTED limitations of the single cursor, pinned as `it.fails` so a
// future fix has a target. Both end the same way: the client's cursor claims a
// position whose changes it never applied, and nothing ever repairs it.
//
// - L2 (ADR-0009 "Out of scope"): the DO is torn down while a delta sits in the
//   coalescer and the hibernatable socket survives. No reconnect fires, and the
//   next delivery advances the cursor past the lost write (no contiguity check).
// - L1 (ADR-0011 "Known limitations"): no incarnation epoch. After a storage
//   reset the new change log reuses seqs; a client cursor from before the reset
//   passes the retention floor check and catches up silently wrong.
//
// Each asserts convergence (client rows == DO rows), not a mechanism, so any
// fix (a delivered delta, a detected gap + resnapshot, an epoch reset) passes.

const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))
async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}
/** Poll up to `ms` for `pred`, without failing: the assertion that follows says what diverged. */
async function settle(pred: () => boolean, ms = 500): Promise<void> {
  await waitFor(pred, ms).catch(() => {})
}
async function serverWrite(r: string, sql: string, ...args: Array<unknown>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    s.storage.sql.exec(sql, ...args)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
async function serverIds(r: string): Promise<Array<string>> {
  return runInDurableObject(stubFor(r), (_i, s) =>
    Array.from(s.storage.sql.exec<{ id: string }>("SELECT id FROM messages ORDER BY id")).map((x) => x.id),
  )
}

/** A raw sub that keeps the rows it was told about. */
function rowSink(): { rows: Map<string, unknown>; terminals: Array<string>; handler: SubHandler } {
  const rows = new Map<string, unknown>()
  const terminals: Array<string> = []
  return {
    rows,
    terminals,
    handler: {
      onSnap: (k, row) => rows.set(String(k), row),
      onSnapEnd: () => terminals.push("snap-end"),
      onDelta: (op, k, cols) => (op === "delete" ? rows.delete(String(k)) : rows.set(String(k), cols)),
      onUptodate: (own) => terminals.push(own ? "uptodate(own)" : "uptodate"),
      onReset: () => {
        terminals.push("reset")
        rows.clear()
      },
    },
  }
}

/** Limitation pins that got as far as their convergence check (see the last test). */
const reached = new Set<string>()

describe("single-cursor known limitations", () => {
  // ADR-0009 "Out of scope": mid-tick eviction dropping pending deltas.
  it.fails("L2: a write buffered when the DO is torn down still reaches the connected client", async () => {
    const r = `ckl-l2-${crypto.randomUUID()}`
    let closed = false
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${r}`,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket!
        ws.accept()
        ws.addEventListener("close", () => (closed = true))
        return ws as unknown as WebSocketLike
      },
    })
    try {
      const sink = rowSink()
      await t.subscribe("s", "messages", sink.handler)
      await waitFor(() => sink.terminals.includes("snap-end"))

      // SIMULATION, not a production path: we cannot make workerd tear the DO
      // down inside the ≤tickMs window, so we stand in for "died before its
      // armed tick fired" by handing the coalescer a flush timer that never
      // fires (swallowed setTimeout), then evict. `evictDurableObject` drops
      // the instance (and the buffered delta with it) and, by default
      // (`webSockets: "hibernate"`), keeps the socket open. Whether production
      // ever evicts with an armed timer while keeping hibernatable sockets is
      // a platform question (bugbash report A, L2).
      await runInDurableObject(stubFor(r), (instance, s) => {
        s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('x','buffered')")
        const real = globalThis.setTimeout
        ;(globalThis as { setTimeout: unknown }).setTimeout = () => 0
        try {
          ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
        } finally {
          globalThis.setTimeout = real
        }
      })
      await evictDurableObject(stubFor(r))
      // Premise: x was still buffered when the instance died. A fix that removes
      // the buffering window altogether makes L2 moot and fails here: then
      // retire this pin rather than weaken it.
      expect(sink.rows.has("x")).toBe(false)
      expect(closed).toBe(false) // premise: the socket survived, so no reconnect will fire

      // Delivery resumes on the surviving socket: a later write reaches it.
      await serverWrite(r, "INSERT INTO messages(id,body) VALUES('y','later')")
      await waitFor(() => sink.rows.has("y"))
      const server = await serverIds(r)
      await settle(() => JSON.stringify([...sink.rows.keys()].sort()) === JSON.stringify(server))

      const client = [...sink.rows.keys()].sort()
      reached.add("L2")
      expect(client).toEqual(server)
    } finally {
      t.close()
    }
  })

  // ADR-0011 "Known limitations": no incarnation epoch.
  it.fails("L1: a reconnect after a storage reset does not keep the old incarnation's rows", async () => {
    const r = `ckl-l1-${crypto.randomUUID()}`
    let gate: Promise<void> = Promise.resolve()
    let release: () => void = () => {}
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${r}`,
      reconnectDelay: () => 10,
      open: async () => {
        await gate
        const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket!
        ws.accept()
        return ws as unknown as WebSocketLike
      },
    })
    try {
      const sink = rowSink()
      for (let i = 1; i <= 5; i++) await serverWrite(r, "INSERT INTO messages(id,body) VALUES(?, 'gen1')", `old${i}`)
      await t.subscribe("s", "messages", sink.handler)
      await waitFor(() => sink.terminals.includes("snap-end"))
      expect(sink.rows.size).toBe(5)
      const cursor = BigInt(t.appliedCursor)

      // Drop; hold the reconnect while storage is reset and repopulated.
      gate = new Promise<void>((res) => (release = res))
      await runInDurableObject(stubFor(r), (_i, s) => {
        for (const w of s.getWebSockets()) w.close(1000, "drop")
      })
      await waitFor(() => (t as unknown as { ws: unknown }).ws === null)
      await runInDurableObject(stubFor(r), async (_i, s) => {
        await s.storage.deleteAll() // an operator reset; a point-in-time restore has the same shape
      })
      await evictDurableObject(stubFor(r)) // the next access re-runs the constructor on empty storage
      for (let i = 1; i <= 7; i++) await serverWrite(r, "INSERT INTO messages(id,body) VALUES(?, 'gen2')", `new${i}`)
      const server = await serverIds(r)
      const maxSeq = await runInDurableObject(stubFor(r), (_i, s) =>
        BigInt(Array.from(s.storage.sql.exec<{ m: number }>("SELECT max(seq) AS m FROM _sync_changes"))[0]!.m),
      )
      expect(maxSeq).toBeGreaterThan(cursor) // premise: the new log reaches past the old cursor

      const mark = sink.terminals.length
      release()
      await waitFor(() => sink.terminals.slice(mark).some((e) => e === "uptodate(own)" || e === "snap-end"))
      await settle(() => JSON.stringify([...sink.rows.keys()].sort()) === JSON.stringify(server))

      const client = [...sink.rows.keys()].sort()
      reached.add("L1")
      expect(client).toEqual(server)
    } finally {
      t.close()
    }
  })

  it("harness: every limitation pin ran to its convergence check", () => {
    // A pin that threw before its convergence check passed `it.fails` for the
    // wrong reason; this is what catches it.
    expect([...reached].sort()).toEqual(["L1", "L2"])
  })
})
