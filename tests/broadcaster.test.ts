import { describe, expect, it } from "vitest"
import { Broadcaster } from "../src/server/broadcast.ts"
import type { ServerFrame } from "../src/wire/frames.ts"

// WHY: the coalescer is the single latency/throughput knob. It must collapse
// repeated writes to the same row (a streaming token field is the motivating
// case) to the latest state, keep distinct keys separate, and — critically for
// hibernation — hold no timer when idle. These are deterministic unit tests of
// that logic; the C1 ordering through the coalescer is covered by the wire
// tests (sync-write / transport), which now flush via flushOne before committed.

function setup() {
  const sent: Array<{ ws: unknown; frame: ServerFrame }> = []
  const b = new Broadcaster((ws, frame) => sent.push({ ws, frame }), () => 50)
  const ws = {} as unknown as WebSocket
  return { b, ws, sent }
}

const deltas = (sent: Array<{ frame: ServerFrame }>) => sent.filter((s) => s.frame.t === "d").map((s) => s.frame)

describe("Broadcaster (M4 egress coalescer)", () => {
  it("collapses repeated writes to one (sub,key) to the latest, with one boundary", () => {
    const { b, ws, sent } = setup()
    b.enqueue(ws, { subId: "s", key: "a", op: "insert", cols: { id: "a", body: "v1" } }, "1")
    b.enqueue(ws, { subId: "s", key: "a", op: "update", cols: { id: "a", body: "v2" } }, "2")
    b.flushOne(ws)
    const ds = deltas(sent)
    expect(ds.length).toBe(1)
    expect(ds[0]).toMatchObject({ op: "update", key: "a", cols: { body: "v2" }, seq: "2" })
    expect(sent.filter((s) => s.frame.t === "uptodate").length).toBe(1)
  })

  it("keeps distinct keys as separate deltas", () => {
    const { b, ws, sent } = setup()
    b.enqueue(ws, { subId: "s", key: "a", op: "insert", cols: { id: "a" } }, "1")
    b.enqueue(ws, { subId: "s", key: "b", op: "insert", cols: { id: "b" } }, "2")
    b.flushOne(ws)
    expect(deltas(sent).length).toBe(2)
  })

  it("emits a delete delta with no cols", () => {
    const { b, ws, sent } = setup()
    b.enqueue(ws, { subId: "s", key: "a", op: "delete" }, "3")
    b.flushOne(ws)
    const d = deltas(sent)[0] as Extract<ServerFrame, { t: "d" }>
    expect(d.op).toBe("delete")
    expect("cols" in d).toBe(false)
  })

  it("does not flush an empty buffer", () => {
    const { b, ws, sent } = setup()
    b.flushOne(ws)
    expect(sent.length).toBe(0)
  })

  it("arms a flush on enqueue and clears it on flush (hibernation-friendly)", () => {
    const { b, ws } = setup()
    b.start(() => [ws])
    expect(b.isFlushScheduled).toBe(false)
    b.enqueue(ws, { subId: "s", key: "a", op: "insert", cols: {} }, "1")
    expect(b.isFlushScheduled).toBe(true)
    b.flushAll([ws])
    expect(b.isFlushScheduled).toBe(false)
  })
})
