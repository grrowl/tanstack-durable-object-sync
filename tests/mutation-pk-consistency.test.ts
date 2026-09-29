// Bug bash 2026-09-29, Group C, item F2.
// WHAT: an insert op whose cols[pk] differs from its op `key` is accepted by the
// DO. The row is stored (and broadcast) under cols[pk], not under `key`, which
// breaks ADR-0001 D9 (optimistic id == confirmed id). Assertions encode the
// CORRECT behaviour (a VALIDATION rejection, nothing stored), so this test
// FAILS while the bug is present.
// RUN:  copy to tests/_bugbash_C_F2.test.ts in a 38ac092 checkout, then
//       npx vitest run tests/_bugbash_C_F2.test.ts
// OBSERVED on 38ac092: 2/2 FAIL.
//   differing pk: receipt {"t":"committed","txId":"f2-1","seq":"1"}; rows [{"id":"b","body":"x"}];
//     watcher delta key "b" (the op key was "a").
//   empty pk: receipt committed; rows [{"id":"","body":"x"}]; watcher delta key "".
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket!
  ws.accept()
  return ws
}
const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))
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

describe("F2: insert cols[pk] vs op key", () => {
  for (const [label, colsId] of [
    ["differing pk", "b"],
    ["empty pk", ""],
  ] as const) {
    it(`insert key 'a' with cols.id = ${JSON.stringify(colsId)} (${label}) is rejected, nothing stored`, async () => {
      const room = `bb-c-f2-${crypto.randomUUID()}`
      const watcher = await openWs(`/sync/${room}`)
      send(watcher, { t: "sub", subId: "w", collection: "messages" })
      await collectUntil(watcher, (f) => f.t === "snap-end")
      const watched = collectUntil(watcher, (f) => f.t === "d", 1000).catch(() => [] as Array<ServerFrame>)

      const ws = await openWs(`/sync/${room}`)
      send(ws, { t: "mut", txId: "f2-1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: colsId, body: "x" } }] })
      const receipt = (await collectUntil(ws, (f) => f.t === "committed" || f.t === "rejected")).at(-1)!
      const deltas = (await watched).filter((f) => f.t === "d")
      const rows = await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) =>
        Array.from(s.storage.sql.exec("SELECT id, body FROM messages")),
      )
      console.log(`[F2 ${label}] receipt=`, JSON.stringify(receipt), "rows=", JSON.stringify(rows), "deltaKeys=", JSON.stringify(deltas.map((d) => (d as { key: string }).key)))

      expect(receipt.t).toBe("rejected")
      expect((receipt as Extract<ServerFrame, { t: "rejected" }>).error.code).toBe("VALIDATION")
      expect(rows).toEqual([])
      expect(deltas).toEqual([])
      ws.close()
      watcher.close()
    })
  }
})
