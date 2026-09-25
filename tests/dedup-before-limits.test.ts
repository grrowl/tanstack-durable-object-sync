import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, MutOp, ServerFrame } from "../src/wire/frames.ts"

// WHY: a resent txId must get its STORED outcome (exactly-once, ADR-0002 C5), so
// every per-tx rejection runs after the dedup lookup (ADR-0025, amended). The
// sharp case is ADR-0021's hold-and-replay: the client resends an identical
// frame after a drop. If `maxOpsPerMutation` was lowered in between (a deploy
// over the same storage), a limit check that ran first would answer
// LIMIT_EXCEEDED, and the client would roll back a write that committed.

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 2000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

/** Send a mut and return its receipt (`committed` or `rejected`). */
async function mut(ws: WebSocket, txId: string, ops: Array<MutOp>): Promise<ServerFrame> {
  const frame: ClientFrame = { t: "mut", txId, collection: "messages", ops }
  ws.send(codec.encode(frame))
  return (await collectUntil(ws, (f) => f.t === "committed" || f.t === "rejected")).at(-1)!
}

const inserts = (...ids: Array<string>): Array<MutOp> =>
  ids.map((id) => ({ type: "insert", key: id, cols: { id, body: id } }))

async function rowIds(ns: DurableObjectNamespace, room: string): Promise<Array<string>> {
  return runInDurableObject(ns.get(ns.idFromName(room)), (_i, s) =>
    Array.from(s.storage.sql.exec("SELECT id FROM messages ORDER BY id")).map((r) => (r as { id: string }).id),
  )
}

describe("dedup lookup runs before the per-mutation op limit (ADR-0025 amendment)", () => {
  it("(a) committed T replayed with the SAME payload after the limit is lowered → stored committed", async () => {
    const room = "dbl-lowered"
    const ws = await openWs(`/sync/${room}`)
    const first = await mut(ws, "dbl-a1", inserts("a1", "a2"))
    expect(first.t).toBe("committed")

    // Stand-in for a deploy that lowers the limit over the same storage: the
    // limit is a construction-time field, and the dedup table lives in SQLite,
    // so setting the field on the live instance is the same state a new
    // instance of a redeployed class would see.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (i) => {
      ;(i as unknown as { maxOpsPerMutation: number }).maxOpsPerMutation = 1
    })

    const again = await mut(ws, "dbl-a1", inserts("a1", "a2"))
    expect(again).toMatchObject({ t: "committed", txId: "dbl-a1", seq: (first as { seq: string }).seq })
    expect(await rowIds(env.SYNC_DO, room)).toEqual(["a1", "a2"])

    // The lowered limit does apply to a NEW txId: it is a real limit, not a no-op.
    const fresh = await mut(ws, "dbl-a2", inserts("a3", "a4"))
    expect(fresh).toMatchObject({ t: "rejected", error: { code: "LIMIT_EXCEEDED" } })
    ws.close()
  })

  it("a LIMIT_EXCEEDED rejection is itself recorded: after the limit is raised, the same frame replays it", async () => {
    // LimitsTestDO: maxOpsPerMutation = 2.
    const room = "dbl-raised"
    const ws = await openWs(`/limits/${room}`)
    const first = await mut(ws, "dbl-x1", inserts("x1", "x2", "x3"))
    expect(first).toMatchObject({ t: "rejected", error: { code: "LIMIT_EXCEEDED" } })

    await runInDurableObject(env.LIMITS_DO.get(env.LIMITS_DO.idFromName(room)), (i) => {
      ;(i as unknown as { maxOpsPerMutation: number }).maxOpsPerMutation = 128
    })

    const again = await mut(ws, "dbl-x1", inserts("x1", "x2", "x3"))
    expect(again).toEqual(first)
    expect(await rowIds(env.LIMITS_DO, room)).toEqual([])
    ws.close()
  })

  // The next two resend a DIFFERENT payload under a used txId. Our client never
  // does that (hold-and-replay resends the identical frame); a non-conforming
  // client can, and the stored outcome must still win.
  it("(b) committed T resent with a different, over-limit payload → stored committed, nothing applied", async () => {
    // LimitsTestDO: maxOpsPerMutation = 2.
    const room = "dbl-bigger"
    const ws = await openWs(`/limits/${room}`)
    const first = await mut(ws, "dbl-b1", inserts("b1"))
    expect(first.t).toBe("committed")

    const again = await mut(ws, "dbl-b1", inserts("b2", "b3", "b4"))
    expect(again).toMatchObject({ t: "committed", txId: "dbl-b1", seq: (first as { seq: string }).seq })
    expect(await rowIds(env.LIMITS_DO, room)).toEqual(["b1"])
    ws.close()
  })

  it("a rejected T resent over the limit replays its stored rejection, not LIMIT_EXCEEDED", async () => {
    const room = "dbl-rejected"
    const ws = await openWs(`/limits/${room}`)
    // The test schema's insert authorize denies body "FORBIDDEN".
    const first = await mut(ws, "dbl-r1", [{ type: "insert", key: "r1", cols: { id: "r1", body: "FORBIDDEN" } }])
    expect(first).toMatchObject({ t: "rejected", error: { message: "forbidden body" } })

    const again = await mut(ws, "dbl-r1", inserts("r2", "r3", "r4"))
    expect(again).toEqual(first)
    expect(await rowIds(env.LIMITS_DO, room)).toEqual([])
    ws.close()
  })
})
