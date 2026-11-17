import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: the orphan-subscription sweep (ADR-0019 D4) rides compaction under
// `ctx.waitUntil`. It used to bind one SQL parameter per live socket, and
// Durable Object SQLite allows 100 per statement, so with 101 live sockets the
// sweep threw in the background and orphaned `_sync_subs` rows were never
// removed. Driven through the real path (compactionEvery=3 on MaintTestDO),
// polling for the deferred effect.

const codec = createFrameCodec()
const maint = (room: string) => env.MAINT_DO.get(env.MAINT_DO.idFromName(room))

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
async function waitUntil(pred: () => Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitUntil(condition) timeout")
    await new Promise((r) => setTimeout(r, 10))
  }
}

describe("orphan-sub sweep with more live sockets than SQLite binds", () => {
  it("sweeps an orphaned row while 101 sockets are live, keeping the live rows", async () => {
    const room = `sweep-many-${crypto.randomUUID()}`
    const sockets = await Promise.all(Array.from({ length: 101 }, () => openWs(room)))
    const ws = sockets[0]!
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await waitForFrame(ws, (f) => f.t === "snap-end")
    await runInDurableObject(maint(room), (_i, s) => {
      s.storage.sql.exec(
        "INSERT INTO _sync_subs(socket_id, sub_id, collection, where_ir) VALUES('dead-socket','s9','messages',NULL)",
      )
    })
    const subRows = (): Promise<Array<string>> =>
      runInDurableObject(maint(room), (_i, s) =>
        Array.from(s.storage.sql.exec<{ socket_id: string }>("SELECT socket_id FROM _sync_subs")).map((r) => r.socket_id),
      )
    expect(await subRows()).toHaveLength(2) // the orphan + the live sub's row

    // Cross compactionEvery=3: housekeeping, the sweep included, is scheduled.
    for (const txId of ["t1", "t2", "t3"]) {
      send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "insert", key: txId, cols: { id: txId, body: txId } }] })
      await waitForFrame(ws, (f) => f.t === "committed" && f.txId === txId)
    }
    await waitUntil(async () => {
      const ids = await subRows()
      return !ids.includes("dead-socket") && ids.length === 1
    })
    for (const s of sockets) s.close()
  })
})
