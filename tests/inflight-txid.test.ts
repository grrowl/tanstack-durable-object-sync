import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import type { GatedApi, GatedTestDO } from "./test-worker.ts"

// WHY: a txId runs its handler at most once, even when a second frame for it
// arrives while the first is still in flight (ADR-0025 amendment). `authorize`
// (and a command's `execute`) can await I/O, and while it does the DO's input
// gate is open, so a duplicate frame is dispatched and passes the dedup lookup,
// because nothing is recorded yet. ADR-0021's hold-and-replay produces exactly
// that duplicate: the socket drops mid-authorize and the client replays the txId
// on its new socket. Without the in-flight guard the replay runs the handler a
// second time: a mutation's replay fails (pk conflict → EXECUTE_FAILED) and the
// client rolls back a write that committed; a command's side effect runs twice.
//
// GatedTestDO parks `authorize` on a gate the test releases from a separate
// event, so "the first frame is still in flight" is a fact, not a timing guess.

const codec = createFrameCodec()

function stub(room: string): DurableObjectStub<GatedTestDO> {
  return env.GATED_DO.get(env.GATED_DO.idFromName(room)) as DurableObjectStub<GatedTestDO>
}

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/gated/${room}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
}

/** Resolve with the receipt for `txId` on `ws`. Listens from the call, so call
 *  it BEFORE the frame that triggers the receipt can be answered. */
function receiptFor(ws: WebSocket, txId: string, timeoutMs = 3000): Promise<ServerFrame> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no receipt for ${txId}`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      const f = codec.decode(e.data as ArrayBuffer) as ServerFrame
      if ((f.t === "committed" || f.t === "rejected") && f.txId === txId) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(f)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

async function waitFor(pred: () => Promise<boolean> | boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

const parked = (room: string, tag: string): Promise<number> =>
  runInDurableObject(stub(room), (i: GatedTestDO) => i.gateParked(tag))
const release = (room: string, tag: string): Promise<void> =>
  runInDurableObject(stub(room), (i: GatedTestDO) => i.gateRelease(tag))

/** Frames on one socket dispatch in order and each handler runs until its first
 *  await, so once an `echo` sent AFTER `frame` is answered, `frame` has been
 *  dispatched and is parked wherever it awaits. */
async function sendAndDispatch(ws: WebSocket, frame: ClientFrame, barrierId: string): Promise<void> {
  const barrier = receiptFor(ws, barrierId)
  send(ws, frame)
  send(ws, { t: "call", txId: barrierId, name: "echo", args: null })
  expect((await barrier).t).toBe("committed")
}

const gatedInsert = (txId: string, id: string, tag: string): ClientFrame => ({
  t: "mut",
  txId,
  collection: "gated",
  ops: [{ type: "insert", key: id, cols: { id, body: tag } }],
})

describe("an in-flight txId runs once; a duplicate replays its outcome (ADR-0025 amendment)", () => {
  it("mut: a duplicate on another socket while T's authorize is parked gets T's committed; T runs once", async () => {
    const room = "ift-mut"
    const ws1 = await openWs(room)
    const ws2 = await openWs(room)
    const r1 = receiptFor(ws1, "T")
    const r2 = receiptFor(ws2, "T")

    send(ws1, gatedInsert("T", "row", "g-mut"))
    await waitFor(async () => (await parked(room, "g-mut")) === 1)
    await sendAndDispatch(ws2, gatedInsert("T", "row", "g-mut"), "B1")

    // The duplicate is waiting on T, not running its own authorize.
    expect(await parked(room, "g-mut")).toBe(1)
    await release(room, "g-mut")

    const [a, b] = await Promise.all([r1, r2])
    expect(a).toMatchObject({ t: "committed", txId: "T" })
    expect(b).toMatchObject({ t: "committed", txId: "T", seq: (a as { seq: string }).seq })
    const rows = await runInDurableObject(stub(room), (_i, s) => Array.from(s.storage.sql.exec("SELECT id FROM gated")))
    expect(rows).toEqual([{ id: "row" }])
    ws1.close()
    ws2.close()
  })

  it("call: a duplicate while T is in flight gets T's result; the side effect runs once", async () => {
    const room = "ift-call"
    const ws1 = await openWs(room)
    const ws2 = await openWs(room)
    const r1 = receiptFor(ws1, "C")
    const r2 = receiptFor(ws2, "C")

    send(ws1, { t: "call", txId: "C", name: "bump", args: { tag: "g-call" } })
    await waitFor(async () => (await parked(room, "g-call")) === 1)
    await sendAndDispatch(ws2, { t: "call", txId: "C", name: "bump", args: { tag: "g-call" } }, "B2")

    expect(await parked(room, "g-call")).toBe(1)
    await release(room, "g-call")

    const [a, b] = await Promise.all([r1, r2])
    expect(a).toMatchObject({ t: "committed", txId: "C", result: { n: 1 } })
    expect(b).toMatchObject({ t: "committed", txId: "C", result: { n: 1 } })
    const n = await runInDurableObject(stub(room), (_i, s) => Array.from(s.storage.sql.exec("SELECT n FROM bumps"))[0])
    expect(n).toEqual({ n: 1 })
    ws1.close()
    ws2.close()
  })

  it("a rejected T: the duplicate gets the same rejection, and a later resend is not wedged", async () => {
    const room = "ift-reject"
    const ws1 = await openWs(room)
    const ws2 = await openWs(room)
    const r1 = receiptFor(ws1, "R")
    const r2 = receiptFor(ws2, "R")

    // T is rejected after its authorize parks: its execute hits an existing row.
    await runInDurableObject(stub(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO gated(id, body) VALUES ('dup', 'seed')")
    })
    send(ws1, gatedInsert("R", "dup", "g-rej"))
    await waitFor(async () => (await parked(room, "g-rej")) === 1)
    await sendAndDispatch(ws2, gatedInsert("R", "dup", "g-rej"), "B3")
    await release(room, "g-rej")

    const [a, b] = await Promise.all([r1, r2])
    expect(a).toMatchObject({ t: "rejected", txId: "R", error: { code: "EXECUTE_FAILED" } })
    expect(b).toEqual(a)

    // T has settled: a resend is answered from the dedup table, promptly.
    const r3 = receiptFor(ws1, "R", 1000)
    send(ws1, gatedInsert("R", "dup", "g-rej"))
    expect(await r3).toEqual(a)
    expect(await parked(room, "g-rej")).toBe(1)
    ws1.close()
    ws2.close()
  })

  it("the duplicate's socket closes before T settles: T still commits and the DO keeps serving", async () => {
    const room = "ift-closed"
    const ws1 = await openWs(room)
    const ws2 = await openWs(room)
    // ws2 is subscribed, so T's commit buffers a delta for it: the waiter's
    // replay then flushes (and sends) to a socket the server has seen close.
    const subReady = receiptOrFrame(ws2, (f) => f.t === "snap-end")
    send(ws2, { t: "sub", subId: "s2", collection: "gated" })
    await subReady
    const r1 = receiptFor(ws1, "K")

    send(ws1, gatedInsert("K", "k", "g-closed"))
    await waitFor(async () => (await parked(room, "g-closed")) === 1)
    await sendAndDispatch(ws2, gatedInsert("K", "k", "g-closed"), "B4")
    ws2.close()
    // Server-side barrier: only ws1 is still OPEN on the DO.
    await waitFor(() =>
      runInDurableObject(stub(room), (_i, state) => state.getWebSockets().filter((w) => w.readyState === WebSocket.OPEN).length === 1),
    )
    await release(room, "g-closed")

    expect(await r1).toMatchObject({ t: "committed", txId: "K" })
    const r2 = receiptFor(ws1, "E")
    send(ws1, { t: "call", txId: "E", name: "echo", args: 1 })
    expect(await r2).toMatchObject({ t: "committed", result: { echoed: 1 } })
    ws1.close()
  })

  it("C1: on a subscribed duplicate socket, T's delta arrives before T's replayed committed", async () => {
    const room = "ift-c1"
    const ws1 = await openWs(room)
    const ws2 = await openWs(room)
    const subReady = receiptOrFrame(ws2, (f) => f.t === "snap-end")
    send(ws2, { t: "sub", subId: "s2", collection: "gated" })
    await subReady

    // Record ws2's frames from here; stop at T's receipt.
    const frames = framesUntil(ws2, (f) => f.t === "committed" && f.txId === "T1")
    send(ws1, gatedInsert("T1", "c1", "g-c1"))
    await waitFor(async () => (await parked(room, "g-c1")) === 1)
    await sendAndDispatch(ws2, gatedInsert("T1", "c1", "g-c1"), "B5")
    await release(room, "g-c1")

    const seen = await frames
    const delta = seen.findIndex((f) => f.t === "d" && f.key === "c1")
    expect(delta).toBeGreaterThanOrEqual(0)
    expect(delta).toBeLessThan(seen.length - 1) // the delta precedes the committed
    ws1.close()
    ws2.close()
  })
})

/** Resolve once a frame matching `done` arrives on `ws`. */
function receiptOrFrame(ws: WebSocket, done: (f: ServerFrame) => boolean): Promise<void> {
  return framesUntil(ws, done).then(() => undefined)
}

/** Collect every frame on `ws` up to and including the first matching `done`. */
function framesUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 3000): Promise<Array<ServerFrame>> {
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

describe("a replayed committed flushes the socket's pending deltas first (ADR-0002 C1)", () => {
  // SlowTickDO: the coalescer tick is 30 s, so a delta for a socket that is not
  // the writer stays buffered unless something flushes it. A plain sequential
  // resend on that socket must not let `committed` (cursor-advancing) overtake it.
  it("a sequential replay on a subscribed socket delivers the buffered delta before committed", async () => {
    const room = "ift-slow"
    const open = async (): Promise<WebSocket> => {
      const res = await SELF.fetch(`https://example.com/slow/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket!
      ws.accept()
      return ws
    }
    const ws1 = await open()
    const ws2 = await open()
    const subReady = receiptOrFrame(ws2, (f) => f.t === "snap-end")
    send(ws2, { t: "sub", subId: "s2", collection: "messages" })
    await subReady

    const frames = framesUntil(ws2, (f) => f.t === "committed" && f.txId === "S1")
    const r1 = receiptFor(ws1, "S1")
    const m: ClientFrame = { t: "mut", txId: "S1", collection: "messages", ops: [{ type: "insert", key: "s", cols: { id: "s", body: "v" } }] }
    send(ws1, m)
    expect((await r1).t).toBe("committed")
    send(ws2, m) // resend on the subscribed socket: a dedup replay

    const seen = await frames
    const delta = seen.findIndex((f) => f.t === "d" && f.key === "s")
    expect(delta).toBeGreaterThanOrEqual(0)
    expect(delta).toBeLessThan(seen.length - 1)
    ws1.close()
    ws2.close()
  })
})

// --- Full stack: the real transport's hold-and-replay (ADR-0021) -----------

function noopHandler(): SubHandler {
  return { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} }
}

describe("hold-and-replay across a drop mid-authorize (ADR-0021 × ADR-0025 amendment)", () => {
  it("the replayed mut resolves committed, never a rollback of the write that committed", async () => {
    const room = "ift-client"
    const sockets: Array<{ sent: Array<ClientFrame> }> = []
    const t = new WebSocketTransport<GatedApi>({
      url: `https://example.com/gated/${room}`,
      reconnectDelay: 20,
      timeoutMs: 60_000,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/gated/${room}`, { headers: { Upgrade: "websocket" } })
        const real = res.webSocket
        if (!real) throw new Error("no webSocket")
        real.accept()
        const log = { sent: [] as Array<ClientFrame> }
        sockets.push(log)
        return {
          send: (d) => {
            log.sent.push(codec.decode(d as ArrayBuffer) as ClientFrame)
            real.send(d)
          },
          close: (code, reason) => real.close(code, reason),
          addEventListener: (type, l) => real.addEventListener(type, l as never),
          removeEventListener: (type, l) => real.removeEventListener(type, l as never),
        } satisfies WebSocketLike
      },
    })
    await t.connect()
    await t.subscribe("s1", "gated", noopHandler()) // hold-and-replay needs a live sub

    const outcome = t.sendMut(gatedInsert("TX", "row", "g-client") as Extract<ClientFrame, { t: "mut" }>).then(
      (v) => ({ ok: true as const, v }),
      (e: unknown) => ({ ok: false as const, e }),
    )
    await waitFor(async () => (await parked(room, "g-client")) === 1)

    // The socket drops while TX's authorize is parked. The transport holds TX,
    // reconnects, and replays it on the new socket.
    await runInDurableObject(stub(room), (_i, state) => {
      for (const s of state.getWebSockets()) s.close(1000, "drop")
    })
    await waitFor(() => sockets.length === 2 && sockets[1]!.sent.some((f) => f.t === "mut"))
    // Barrier: an echo sent after the replay is answered, so the replay is dispatched.
    await t.call.echo(null)

    await release(room, "g-client")
    const r = await outcome
    expect(r.ok ? "committed" : String(r.e)).toBe("committed")
    const rows = await runInDurableObject(stub(room), (_i, s) => Array.from(s.storage.sql.exec("SELECT id FROM gated")))
    expect(rows).toEqual([{ id: "row" }])
    t.close()
  })
})
