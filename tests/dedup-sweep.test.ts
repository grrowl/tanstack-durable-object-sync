import { env, evictDurableObject, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (bugbash F7, ADR-0009 amendment 2026-09-29): every answered `mut`/`call`
// adds a `_sync_seen_tx` row, so the sweep must ride answered receipts, not
// drained writes. A DO that only rejects, replays, or runs write-free commands
// otherwise grows the table forever. The gate is time-based and held in memory:
// at most one sweep per `dedupRetentionMs`, and the first receipt after a
// construction or hibernation wake always sweeps. These tests drive real
// sockets and poll the table for the effect (ADR-0009's lesson), never calling
// `sweepDedup` directly.
//
// SYNC_DO keeps the shipped 1 h retention, so "the gate is closed" is a
// race-free assertion there. MAINT_DO overrides retention to 1 s so the gate
// can be seen to reopen with time alone.

const codec = createFrameCodec()
const HOUR = 3_600_000
const stubFor = (path: "sync" | "maint", room: string) => {
  const ns = path === "sync" ? env.SYNC_DO : env.MAINT_DO
  return ns.get(ns.idFromName(room))
}

async function openWs(path: "sync" | "maint", room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/${path}/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function sendForReceipt(ws: WebSocket, frame: Extract<ClientFrame, { t: "mut" | "call" }>): Promise<ServerFrame> {
  const receipt = new Promise<ServerFrame>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no receipt for ${frame.txId}`)), 2000)
    const onMsg = (event: MessageEvent): void => {
      const f = codec.decode(event.data as ArrayBuffer) as ServerFrame
      if ((f.t === "committed" || f.t === "rejected") && f.txId === frame.txId) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(f)
      }
    }
    ws.addEventListener("message", onMsg)
  })
  ws.send(codec.encode(frame))
  return receipt
}

// "FORBIDDEN" makes the messages:insert authorize throw, so the mut is rejected.
const insert = (ws: WebSocket, txId: string, body: string): Promise<ServerFrame> =>
  sendForReceipt(ws, {
    t: "mut", txId, collection: "messages", ops: [{ type: "insert", key: txId, cols: { id: txId, body } }],
  })

const seenIds = (stub: DurableObjectStub): Promise<string[]> =>
  runInDurableObject(stub, (_i, state) =>
    Array.from(state.storage.sql.exec<{ tx_id: string }>("SELECT tx_id FROM _sync_seen_tx")).map((r) => r.tx_id),
  )

/** Seed an already-expired receipt. Touches storage only, never the gate. */
const seedExpired = (stub: DurableObjectStub, txId: string): Promise<void> =>
  runInDurableObject(stub, (_i, state) => {
    state.storage.sql.exec(
      "INSERT INTO _sync_seen_tx(tx_id,ok,cursor,error,error_code,result,ts) VALUES(?,0,NULL,'old',NULL,NULL,?)",
      txId,
      Date.now() - 2 * HOUR,
    )
  })

async function waitForSweep(stub: DurableObjectStub, expiredId: string, freshId: string): Promise<void> {
  const deadline = Date.now() + 2000
  while (Date.now() < deadline) {
    const ids = await seenIds(stub)
    if (!ids.includes(expiredId) && ids.includes(freshId)) return
    await new Promise((r) => setTimeout(r, 10))
  }
  const ids = await seenIds(stub)
  expect(ids).not.toContain(expiredId)
  expect(ids).toContain(freshId)
}

describe("dedup sweep rides answered receipts (bugbash F7)", () => {
  it("a rejected mutation sweeps expired receipts", async () => {
    const room = `sweep-reject-${crypto.randomUUID()}`
    const stub = stubFor("sync", room)
    const ws = await openWs("sync", room)
    await seedExpired(stub, "old")
    expect((await insert(ws, "rej-1", "FORBIDDEN")).t).toBe("rejected")
    await waitForSweep(stub, "old", "rej-1")
    ws.close()
  })

  it("a successful write-free command sweeps expired receipts", async () => {
    const room = `sweep-echo-${crypto.randomUUID()}`
    const stub = stubFor("sync", room)
    const ws = await openWs("sync", room)
    await seedExpired(stub, "old")
    expect((await sendForReceipt(ws, { t: "call", txId: "echo-1", name: "echo", args: 1 })).t).toBe("committed")
    await waitForSweep(stub, "old", "echo-1")
    ws.close()
  })

  it("a drained write sweeps expired receipts, without waiting for compaction", async () => {
    // MAINT_DO compacts every 3 drains; one write proves the sweep no longer
    // rides compaction.
    const room = `sweep-write-${crypto.randomUUID()}`
    const stub = stubFor("maint", room)
    const ws = await openWs("maint", room)
    await seedExpired(stub, "old")
    expect((await insert(ws, "write-1", "fine")).t).toBe("committed")
    await waitForSweep(stub, "old", "write-1")
    ws.close()
  })

  it("a replayed receipt sweeps expired receipts", async () => {
    const room = `sweep-replay-${crypto.randomUUID()}`
    const stub = stubFor("sync", room)
    const ws = await openWs("sync", room)
    expect((await insert(ws, "old", "FORBIDDEN")).t).toBe("rejected")
    await runInDurableObject(stub, (_i, state) => {
      state.storage.sql.exec("UPDATE _sync_seen_tx SET ts = ts - ?", 2 * HOUR)
    })
    expect((await insert(ws, "fresh", "FORBIDDEN")).t).toBe("rejected")
    // The wake reopens the gate, so the replay is the receipt that sweeps. It
    // replays the stored outcome first, then deletes the now-expired row.
    await evictDurableObject(stub)
    expect((await insert(ws, "old", "fine")).t).toBe("rejected")
    await waitForSweep(stub, "old", "fresh")
    ws.close()
  })

  it("sweeps at most once per retention window, and again after a hibernation wake", async () => {
    const room = `sweep-wake-${crypto.randomUUID()}`
    const stub = stubFor("sync", room)
    const ws = await openWs("sync", room)
    expect((await insert(ws, "rej-1", "FORBIDDEN")).t).toBe("rejected") // sweeps; gate closes
    await seedExpired(stub, "old")
    expect((await insert(ws, "rej-2", "FORBIDDEN")).t).toBe("rejected")
    // Not yet: the gate is closed for an hour. The sweep would run synchronously
    // after this receipt, so the row surviving here is a race-free "no".
    expect(await seenIds(stub)).toContain("old")
    // A wake starts a fresh in-memory gate, so a DO that hibernates between
    // receipts still sweeps.
    await evictDurableObject(stub)
    expect((await insert(ws, "rej-3", "FORBIDDEN")).t).toBe("rejected")
    await waitForSweep(stub, "old", "rej-3")
    ws.close()
  })

  it("sweeps again once the retention window has passed, without a wake", async () => {
    const room = `sweep-reopen-${crypto.randomUUID()}`
    const stub = stubFor("maint", room)
    const ws = await openWs("maint", room)
    expect((await insert(ws, "rej-1", "FORBIDDEN")).t).toBe("rejected") // sweeps; gate closes
    await seedExpired(stub, "old")
    await new Promise((r) => setTimeout(r, 1100)) // MAINT_DO dedupRetentionMs = 1 s
    expect((await insert(ws, "rej-2", "FORBIDDEN")).t).toBe("rejected")
    await waitForSweep(stub, "old", "rej-2")
    ws.close()
  })
})
