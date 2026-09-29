import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, MutOp, ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()
const receipt = (frame: ServerFrame): boolean => frame.t === "committed" || frame.t === "rejected"

async function openWs(room: string): Promise<WebSocket> {
  const response = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = response.webSocket!
  ws.accept()
  return ws
}

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
}

function collectUntil(ws: WebSocket, done: (frame: ServerFrame) => boolean): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const frames: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${frames.map((frame) => frame.t).join(",")}]`)), 2000)
    const onMessage = (event: MessageEvent): void => {
      const frame = codec.decode(event.data as ArrayBuffer) as ServerFrame
      frames.push(frame)
      if (done(frame)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMessage)
        resolve(frames)
      }
    }
    ws.addEventListener("message", onMessage)
  })
}

async function state(room: string): Promise<{ rows: Array<{ id: string; body: string }>; changes: number }> {
  return runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_instance, storage) => ({
    rows: Array.from(storage.storage.sql.exec("SELECT id, body FROM messages ORDER BY id")) as Array<{ id: string; body: string }>,
    changes: (Array.from(storage.storage.sql.exec("SELECT COUNT(*) AS n FROM _sync_changes"))[0] as { n: number }).n,
  }))
}

async function mutWithWatcher(room: string, txId: string, ops: Array<MutOp>) {
  const watcher = await openWs(room)
  send(watcher, { t: "sub", subId: "w", collection: "messages" })
  await collectUntil(watcher, (frame) => frame.t === "snap-end")
  const writer = await openWs(room)
  const sentinel = `${txId}-sentinel`
  const watched = collectUntil(watcher, (frame) => frame.t === "d" && frame.key === sentinel)
  const before = await state(room)
  send(writer, { t: "mut", txId, collection: "messages", ops })
  const result = (await collectUntil(writer, receipt)).at(-1)!
  const after = await state(room)

  send(writer, { t: "mut", txId: sentinel, collection: "messages", ops: [{ type: "insert", key: sentinel, cols: { id: sentinel, body: "sentinel" } }] })
  expect((await collectUntil(writer, receipt)).at(-1)!.t).toBe("committed")
  const deltas = (await watched).filter((frame) => frame.t === "d" && frame.key !== sentinel)
  writer.close()
  watcher.close()
  return { result, before, after, deltas }
}

describe("mutation pk agrees with the op key (ADR-0025)", () => {
  for (const [label, id] of [["different", "b"], ["empty", ""], ["null", null], ["numeric", 7]] as const) {
    it(`rejects insert with ${label} cols.id without a write or delta`, async () => {
      const room = `pk-insert-${crypto.randomUUID()}`
      const { result, before, after, deltas } = await mutWithWatcher(room, `pk-${label}`, [
        { type: "insert", key: "a", cols: { id, body: "x" } },
      ])
      expect(result).toMatchObject({ t: "rejected", error: { code: "VALIDATION" } })
      expect(after).toEqual(before)
      expect(deltas).toEqual([])
    })
  }

  it("rejects before the author's authorization runs", async () => {
    const room = `pk-auth-${crypto.randomUUID()}`
    const writer = await openWs(room)
    send(writer, { t: "mut", txId: "pk-auth", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "b", body: "FORBIDDEN" } }] })
    expect((await collectUntil(writer, receipt)).at(-1)).toMatchObject({ t: "rejected", error: { code: "VALIDATION" } })
    writer.close()
  })

  it("rejects an update carrying a different pk, leaving the target untouched", async () => {
    const room = `pk-update-${crypto.randomUUID()}`
    const writer = await openWs(room)
    send(writer, { t: "mut", txId: "seed", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "seed" } }] })
    expect((await collectUntil(writer, receipt)).at(-1)!.t).toBe("committed")
    writer.close()

    const { result, before, after, deltas } = await mutWithWatcher(room, "pk-update", [
      { type: "update", key: "a", cols: { id: "b", body: "changed" } },
    ])
    expect(result).toMatchObject({ t: "rejected", error: { code: "VALIDATION" } })
    expect(after).toEqual(before)
    expect(deltas).toEqual([])
  })

  it("leaves an omitted insert pk to the author handler", async () => {
    const room = `pk-omitted-${crypto.randomUUID()}`
    const writer = await openWs(room)
    send(writer, { t: "mut", txId: "pk-omitted", collection: "messages", ops: [{ type: "insert", key: "a", cols: { body: "FORBIDDEN" } }] })
    expect((await collectUntil(writer, receipt)).at(-1)).toMatchObject({ t: "rejected", error: { message: "forbidden body" } })
    expect((await state(room)).rows).toEqual([])
    writer.close()
  })

  it("refuses the whole batch if a later insert conflicts", async () => {
    const room = `pk-batch-${crypto.randomUUID()}`
    const { result, before, after, deltas } = await mutWithWatcher(room, "pk-batch", [
      { type: "insert", key: "good", cols: { id: "good", body: "ok" } },
      { type: "insert", key: "a", cols: { id: "b", body: "bad" } },
    ])
    expect(result).toMatchObject({ t: "rejected", error: { code: "VALIDATION" } })
    expect(after).toEqual(before)
    expect(deltas).toEqual([])
  })

  it("replays a recorded commit before checking a duplicate's mismatched pk", async () => {
    const room = `pk-dedup-${crypto.randomUUID()}`
    const writer = await openWs(room)
    send(writer, { t: "mut", txId: "pk-dedup", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "seed" } }] })
    const first = (await collectUntil(writer, receipt)).at(-1)!
    expect(first.t).toBe("committed")
    send(writer, { t: "mut", txId: "pk-dedup", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "b", body: "changed" } }] })
    expect((await collectUntil(writer, receipt)).at(-1)).toMatchObject({ t: "committed", txId: "pk-dedup", seq: (first as { seq: string }).seq })
    expect((await state(room)).rows).toEqual([{ id: "a", body: "seed" }])
    writer.close()
  })

  it("replays a recorded pk rejection before checking a corrected duplicate", async () => {
    const room = `pk-rejected-dedup-${crypto.randomUUID()}`
    const writer = await openWs(room)
    send(writer, { t: "mut", txId: "pk-rejected", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "b", body: "bad" } }] })
    expect((await collectUntil(writer, receipt)).at(-1)).toMatchObject({ t: "rejected", error: { code: "VALIDATION" } })
    send(writer, { t: "mut", txId: "pk-rejected", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "good" } }] })
    expect((await collectUntil(writer, receipt)).at(-1)).toMatchObject({ t: "rejected", error: { code: "VALIDATION" } })
    expect((await state(room)).rows).toEqual([])
    writer.close()
  })
})
