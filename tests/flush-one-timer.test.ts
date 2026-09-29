import { describe, expect, it } from "vitest"
import { Broadcaster } from "../src/server/broadcast.ts"
import type { ServerFrame } from "../src/wire/frames.ts"

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

describe("flushOne timer hygiene", () => {
  it("cancels the last pending socket's tick instead of running an empty flush", async () => {
    const sent: Array<ServerFrame> = []
    const ws = {} as WebSocket
    let tickFlushes = 0
    const broadcaster = new Broadcaster((_ws, frame) => sent.push(frame), 20)
    broadcaster.start(() => {
      tickFlushes++
      return [ws]
    })
    try {
      expect(broadcaster.isFlushScheduled).toBe(false)
      broadcaster.enqueue(ws, { subId: "s", key: "a", op: "insert", cols: { id: "a" } }, "1")
      expect(broadcaster.isFlushScheduled).toBe(true)
      broadcaster.flushOne(ws)
      const armedAfterFlush = broadcaster.isFlushScheduled
      const scansAfterFlush = tickFlushes
      expect(sent.map((frame) => frame.t)).toEqual(["d", "uptodate"])
      await new Promise((resolve) => setTimeout(resolve, 60))
      expect(armedAfterFlush).toBe(false)
      expect(tickFlushes).toBe(scansAfterFlush)
      expect(sent).toHaveLength(2)
    } finally {
      broadcaster.stop()
    }
  })

  it("keeps another socket's tick, then rearms after becoming idle", async () => {
    const sent: Array<{ ws: WebSocket; frame: ServerFrame }> = []
    const first = {} as WebSocket
    const second = {} as WebSocket
    const broadcaster = new Broadcaster((ws, frame) => sent.push({ ws, frame }), 20)
    broadcaster.start(() => [first, second])
    try {
      broadcaster.enqueue(first, { subId: "s", key: "a", op: "insert", cols: { id: "a" } }, "1")
      broadcaster.enqueue(second, { subId: "s", key: "b", op: "insert", cols: { id: "b" } }, "2")
      broadcaster.flushOne(first)
      expect(broadcaster.isFlushScheduled).toBe(true)
      expect(sent.some((entry) => entry.ws === second)).toBe(false)
      await waitFor(() => sent.some((entry) => entry.ws === second && entry.frame.t === "uptodate"))
      expect(broadcaster.isFlushScheduled).toBe(false)
      expect(sent.filter((entry) => entry.ws === second).map((entry) => entry.frame.t)).toEqual(["d", "uptodate"])

      broadcaster.enqueue(first, { subId: "s", key: "c", op: "delete" }, "3")
      expect(broadcaster.isFlushScheduled).toBe(true)
      await waitFor(() => sent.some((entry) => entry.frame.t === "d" && entry.frame.key === "c"))
      expect(broadcaster.isFlushScheduled).toBe(false)
    } finally {
      broadcaster.stop()
    }
  })

  it("ignores departed sockets when a live socket's flush decides whether to keep the tick", () => {
    const first = {} as WebSocket
    const departed = {} as WebSocket
    const live = new Set([first, departed])
    const broadcaster = new Broadcaster(() => {}, 20)
    broadcaster.start(() => live)
    try {
      broadcaster.enqueue(first, { subId: "s", key: "a", op: "delete" }, "1")
      broadcaster.enqueue(departed, { subId: "s", key: "b", op: "delete" }, "2")
      live.delete(departed)
      broadcaster.flushOne(first)
      expect(broadcaster.isFlushScheduled).toBe(false)
    } finally {
      broadcaster.stop()
    }
  })
})
