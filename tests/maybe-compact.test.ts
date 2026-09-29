import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: maybeCompact is the opportunistic housekeeping path — it fires only every
// `compactionEvery` drained mutations and runs compaction + retention prune +
// orphan-sub sweep under `ctx.waitUntil` (the dedup sweep rides receipts
// instead; see dedup-sweep.test.ts). It was previously covered only by calling
// the underlying functions directly; the WIRING (does the threshold gate it?
// does it actually run compaction and the ADR-0009 prune?) was untested. These
// drive the real WS dispatch → drain → maybeCompact → waitUntil path on a DO
// with compactionEvery=3, polling for the deferred effect (the waitUntil work
// completes shortly after the mutation's receipt, so we poll rather than assume).

const codec = createFrameCodec()

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/maint/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))

function waitForFrame(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 2000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("waitForFrame timeout")), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      if (done(codec.decode(e.data as ArrayBuffer) as ServerFrame)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve()
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

/** Send an insert and await its `committed` receipt — guarantees the write
 *  drained (and thus maybeCompact was called) before we proceed. */
async function insert(ws: WebSocket, txId: string, key: string, body: string): Promise<void> {
  send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "insert", key, cols: { id: key, body } }] })
  await waitForFrame(ws, (f) => f.t === "committed" && f.txId === txId)
}

async function update(ws: WebSocket, txId: string, key: string, body: string): Promise<void> {
  send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "update", key, cols: { body } }] })
  await waitForFrame(ws, (f) => f.t === "committed" && f.txId === txId)
}

const maint = (room: string) => env.MAINT_DO.get(env.MAINT_DO.idFromName(room))

/** Rows currently in the change log, as `key@seq`. */
async function changeRows(room: string): Promise<Array<string>> {
  return runInDurableObject(maint(room), (_i, s) =>
    Array.from(
      s.storage.sql.exec<{ key: string; seq: number }>("SELECT key, seq FROM _sync_changes ORDER BY seq"),
    ).map((r) => `${r.key}@${r.seq}`),
  )
}

async function waitUntil(pred: () => Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitUntil(condition) timeout")
    await new Promise((r) => setTimeout(r, 10))
  }
}

describe("maybeCompact wiring: threshold gate + compaction + retention prune (compactionEvery=3)", () => {
  it("does NOT compact before the threshold, then collapses on the Nth drained write", async () => {
    const room = "mc-threshold"
    const ws = await openWs(room)
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await waitForFrame(ws, (f) => f.t === "snap-end")

    // 2 writes to the SAME key (< compactionEvery): maybeCompact returns without
    // scheduling, so both change rows persist. This is a STABLE assertion — no
    // waitUntil is pending, so there is nothing to race.
    await insert(ws, "t1", "x", "1") // seq1
    await update(ws, "t2", "x", "2") // seq2
    expect(await changeRows(room)).toEqual(["x@1", "x@2"]) // not collapsed yet

    // 3rd drained write hits the threshold → maybeCompact schedules compaction.
    // Poll for the deferred collapse to latest-op-per-key (seq1 superseded).
    await update(ws, "t3", "x", "3") // seq3
    await waitUntil(async () => {
      const rows = await changeRows(room)
      return rows.length === 1 && rows[0] === "x@3"
    })
    ws.close()
  })

  it("prunes changes older than the retention window when maybeCompact fires", async () => {
    const room = "mc-prune"
    const ws = await openWs(room)
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await waitForFrame(ws, (f) => f.t === "snap-end")

    // One write, then age its change row past the default 2-day retention.
    await insert(ws, "t1", "old", "1") // seq1
    await runInDurableObject(maint(room), (_i, s) => {
      s.storage.sql.exec("UPDATE _sync_changes SET ts = ts - ? WHERE key = 'old'", 3 * 86_400_000) // 3 days
    })

    // Two more writes reach compactionEvery=3 → maybeCompact runs compaction
    // (distinct keys, no collapse) AND pruneChanges(2 days). Poll until the aged
    // 'old' row is gone while the fresh ones remain — proving the prune is wired
    // into the housekeeping path, with the shipped default retention.
    await insert(ws, "t2", "a", "2") // seq2
    await insert(ws, "t3", "b", "3") // seq3
    await waitUntil(async () => {
      const keys = (await changeRows(room)).map((r) => r.split("@")[0])
      return !keys.includes("old") && keys.includes("a") && keys.includes("b")
    })
    ws.close()
  })

  // ADR-0019 D4: `_sync_subs` rows for a socket that died WITHOUT a
  // webSocketClose (hard termination) are orphans — nothing else deletes them.
  // Their hygiene rides this same housekeeping path, so it gets the same
  // drive-the-real-path treatment: seed an orphan row, cross the threshold,
  // poll for the deferred sweep. Precision matters as much as deletion: the
  // live socket's own row must survive the same sweep.
  it("sweeps orphaned _sync_subs rows while keeping live sockets' rows", async () => {
    const room = "mc-orphan-subs"
    const ws = await openWs(room)
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await waitForFrame(ws, (f) => f.t === "snap-end")

    // Seed the orphan: a socket_id no live socket bears.
    await runInDurableObject(maint(room), (_i, s) => {
      s.storage.sql.exec(
        "INSERT INTO _sync_subs(socket_id, sub_id, collection, where_ir) VALUES('dead-socket','s9','messages',NULL)",
      )
    })

    const subRows = (): Promise<Array<string>> =>
      runInDurableObject(maint(room), (_i, s) =>
        Array.from(s.storage.sql.exec<{ socket_id: string }>("SELECT socket_id FROM _sync_subs")).map(
          (r) => r.socket_id,
        ),
      )
    expect((await subRows()).includes("dead-socket")).toBe(true) // seeded
    expect((await subRows()).length).toBe(2) // orphan + the live sub's write-through row

    // Cross compactionEvery=3 → housekeeping (incl. the sweep) is scheduled.
    await insert(ws, "t1", "k1", "1")
    await insert(ws, "t2", "k2", "2")
    await insert(ws, "t3", "k3", "3")
    await waitUntil(async () => {
      const ids = await subRows()
      return !ids.includes("dead-socket") && ids.length === 1 // orphan gone, live row kept
    })
    ws.close()
  })
})
