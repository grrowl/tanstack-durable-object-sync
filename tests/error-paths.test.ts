import { createCollection } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"
import { settled } from "./settled.ts"

// WHY: these tests pin three load-bearing invariants in the server's error paths:
//   1. Atomicity — a failed mutation leaves no partial state; nothing is applied
//      and no delta frames leak to subscribers.
//   2. Fail-loud — the response to a failed operation is always a `rejected` frame
//      with the cause, never a silent drop or a `committed`.
//   3. Exactly-once in failure — a retried rejected txId replays the rejection from
//      the dedup record without re-running the handler.
//
// A regression in any of these paths (rolled-back op leaving a row, a `d` frame
// arriving after rejection, a retry re-executing) would ship silently without these
// tests. They are also the verification baseline that plans 004 and 005 build on.

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

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
}

/** Subscribe and wait for the initial snap-end so later frames are post-sub. */
async function subscribe(ws: WebSocket, subId: string): Promise<void> {
  send(ws, { t: "sub", subId, collection: "messages" })
  await collectUntil(ws, (f) => f.t === "snap-end")
}

describe("mutation error paths (atomicity, fail-loud, exactly-once)", () => {
  it("execute throws → rejected, rolled back, no deltas", async () => {
    const ws = await openWs("/sync/err-exec-throw")
    await subscribe(ws, "s1")

    // Insert key "a" successfully.
    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "v1" } }] })
    await collectUntil(ws, (f) => f.t === "committed")

    // Insert the SAME key "a" — SQLite PRIMARY KEY throws inside transactionSync.
    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "v2" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")

    const last = frames[frames.length - 1]!
    expect(last.t).toBe("rejected")
    expect(frames.some((f) => f.t === "d")).toBe(false) // no delta for the failed write

    // Via server-side inspection: original row is intact.
    const room = "err-exec-throw"
    const rows = await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      return Array.from(s.storage.sql.exec("SELECT body FROM messages WHERE id='a'"))
    })
    expect(rows).toHaveLength(1)
    expect((rows[0] as { body: string }).body).toBe("v1")
    ws.close()
  })

  it("execute throw rolls back the WHOLE multi-op batch", async () => {
    const subWs = await openWs("/sync/err-multi-rollback")
    await subscribe(subWs, "s1")

    const ws = await openWs("/sync/err-multi-rollback")
    // Two ops: op 2 duplicates the PK of op 1 — the whole batch must roll back.
    send(ws, {
      t: "mut",
      txId: "t1",
      collection: "messages",
      ops: [
        { type: "insert", key: "x", cols: { id: "x", body: "first" } },
        { type: "insert", key: "x", cols: { id: "x", body: "dup" } },
      ],
    })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    expect(frames[frames.length - 1]!.t).toBe("rejected")

    // op 1 must also be rolled back (count = 0).
    const room = "err-multi-rollback"
    const rows = await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      return Array.from(s.storage.sql.exec("SELECT COUNT(*) as cnt FROM messages WHERE id='x'"))
    })
    expect((rows[0] as { cnt: number }).cnt).toBe(0)

    // No delta frames arrived on the subscribed socket either.
    // We collect with a small timeout (no terminal frame to wait for).
    let deltaArrived = false
    subWs.addEventListener("message", (e) => {
      const f = codec.decode(e.data as ArrayBuffer) as ServerFrame
      if (f.t === "d") deltaArrived = true
    })
    await new Promise((r) => setTimeout(r, 100))
    expect(deltaArrived).toBe(false)
    ws.close()
    subWs.close()
  })

  it("authorize failure on op 2 rejects whole batch before any write", async () => {
    const ws = await openWs("/sync/err-authz-op2")
    await subscribe(ws, "s1")

    send(ws, {
      t: "mut",
      txId: "t1",
      collection: "messages",
      ops: [
        { type: "insert", key: "ok1", cols: { id: "ok1", body: "fine" } },
        { type: "insert", key: "bad", cols: { id: "bad", body: "FORBIDDEN" } },
      ],
    })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(last.t).toBe("rejected")
    expect(last.error.message).toMatch(/forbidden/)
    expect(frames.some((f) => f.t === "d")).toBe(false)

    // "ok1" must not exist server-side (authorize runs before any execute).
    const room = "err-authz-op2"
    const rows = await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      return Array.from(s.storage.sql.exec("SELECT COUNT(*) as cnt FROM messages WHERE id='ok1'"))
    })
    expect((rows[0] as { cnt: number }).cnt).toBe(0)
    ws.close()
  })

  it("unknown mutation collection → rejected", async () => {
    const ws = await openWs("/sync/err-unknown-mut")
    send(ws, { t: "mut", txId: "u1", collection: "nonexistent", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "x" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(last.t).toBe("rejected")
    expect(last.error.message).toMatch(/no mutation handler/)
    ws.close()
  })

  it("a retried REJECTED txId replays the rejection without re-running", async () => {
    const ws = await openWs("/sync/err-retry-rejected")
    const frame: ClientFrame = { t: "mut", txId: "u1", collection: "nonexistent", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "x" } }] }

    send(ws, frame)
    const first = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const r1 = first[first.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(r1.t).toBe("rejected")

    // Retry the exact same frame — must replay the rejection from the dedup record.
    send(ws, frame)
    const second = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const r2 = second[second.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(r2.t).toBe("rejected")
    expect(r2.error.message).toBe(r1.error.message)
    // A code-less rejection must replay code-less — the replayed frame is shaped
    // identically to the original, so a client can't tell replay from first send.
    expect(r1.error.code).toBeUndefined()
    expect(r2.error.code).toBeUndefined()
    ws.close()
  })

  it("a retried CODED rejection replays with the identical code (issue #21)", async () => {
    // WHY: the machine-readable `code` exists so clients can branch on the cause
    // (VALIDATION vs EXECUTE_FAILED, ...) — and the dedup replay path is exactly
    // where a retrying client re-reads the outcome. If the code is dropped from
    // the persisted record, code-based handling breaks precisely on the retry it
    // was built for, while the first response looks fine in every test.
    const ws = await openWs("/sync/err-retry-coded")
    // Empty `body` fails the `validated` collection's schema → VALIDATION code.
    const frame: ClientFrame = { t: "mut", txId: "v1", collection: "validated", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "" } }] }

    send(ws, frame)
    const first = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const r1 = first[first.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(r1.t).toBe("rejected")
    expect(r1.error.code).toBe("VALIDATION")

    // Retry the exact same frame — the replayed rejection must carry the same code.
    send(ws, frame)
    const second = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const r2 = second[second.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(r2.t).toBe("rejected")
    expect(r2.error.code).toBe("VALIDATION")
    expect(r2.error.message).toBe(r1.error.message)
    ws.close()
  })
})

describe("no mutation handler, at client altitude", () => {
  // WHY: the wire-level sibling above ("unknown mutation collection →
  // rejected") pins that the server answers /no mutation handler/ with a
  // `rejected` frame. This pins the CLIENT altitude of the SAME server path:
  // the rejection surfaces through a real @tanstack/db collection as a
  // MutationRejectedError and the optimistic overlay rolls back promptly —
  // a regression in either half (transport rejection plumbing, TanStack
  // rollback wiring) ships silently past the wire-level pin alone.
  //
  // `transformed` declares ONLY an insert mutation — update/delete are the
  // "no mutations block" case per-op. A collection with NO mutations at all
  // takes the identical code path (mutations.get returns undefined).
  it("client update on an op with no handler: MutationRejectedError + optimistic rollback", async () => {
    const room = "fl-readonly"
    const t = new WebSocketTransport<TestApi>({
      url: `https://example.com/sync/${room}`,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        return ws as unknown as WebSocketLike
      },
    })
    await t.connect()

    const coll = createCollection(
      doCollectionOptions({
        transport: t,
        table: "transformed",
        getKey: (r) => r.id,
      }),
    )
    await coll.preload()

    // Seed via the existing insert mutation (allowed).
    await settled(coll.insert({ id: "r1", body: "orig" }))
    expect(coll.get("r1")).toMatchObject({ body: "orig" })

    // update has NO handler on `transformed`.
    const tx = coll.update("r1", (d) => {
      d.body = "hacked"
    })
    // Optimistic overlay applies immediately...
    expect(coll.get("r1")).toMatchObject({ body: "hacked" })

    let err: unknown
    try {
      await settled(tx)
    } catch (e) {
      err = e
    }
    expect(err).toBeDefined()
    // TanStack may wrap the mutationFn error; the root cause is the transport's
    // MutationRejectedError carrying the server's message.
    const msg =
      err instanceof Error ? `${err.message} ${String((err as { cause?: unknown }).cause ?? "")}` : String(err)
    expect(msg).toMatch(/no mutation handler for 'transformed:update'/)

    // Optimistic overlay rolled back to the confirmed row.
    expect(coll.get("r1")).toMatchObject({ body: "orig" })

    // Server row untouched.
    const rows = await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) =>
      Array.from(s.storage.sql.exec("SELECT body FROM transformed WHERE id='r1'")),
    )
    expect((rows[0] as { body: string }).body).toBe("orig")
    t.close()
  })
})

describe("command error paths", () => {
  it("command execute throws → rejected with generic message, detail not leaked", async () => {
    const ws = await openWs("/sync/err-cmd-boom")
    send(ws, { t: "call", txId: "c1", name: "boom", args: {} })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(last.t).toBe("rejected")
    // execute errors are sanitized — internal detail must NOT reach the client (ADR-0012)
    expect(last.error.message).toBe("command failed")
    expect(last.error.code).toBe("EXECUTE_FAILED")
    expect(last.error.message).not.toMatch(/boom/)
    ws.close()
  })

  it("unknown command → rejected with UNKNOWN_COMMAND code", async () => {
    const ws = await openWs("/sync/err-cmd-unknown")
    send(ws, { t: "call", txId: "c2", name: "nope", args: {} })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(last.t).toBe("rejected")
    expect(last.error.code).toBe("UNKNOWN_COMMAND")
    ws.close()
  })
})

describe("retention floor reconnect reset", () => {
  it("a since below the retention floor gets reset + fresh snapshot", async () => {
    const room = "err-floor-reset"
    const ws = await openWs(`/sync/${room}`)

    // Insert three rows. We'll prune the first two change log rows so floor=3,
    // which makes since="1" strictly below floor-1=2, triggering a reset.
    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "alpha" } }] })
    await collectUntil(ws, (f) => f.t === "committed")

    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "insert", key: "b", cols: { id: "b", body: "beta" } }] })
    const c2frames = await collectUntil(ws, (f) => f.t === "committed")
    const secondSeq = Number((c2frames.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>).seq)

    send(ws, { t: "mut", txId: "t3", collection: "messages", ops: [{ type: "insert", key: "c", cols: { id: "c", body: "gamma" } }] })
    await collectUntil(ws, (f) => f.t === "committed")
    ws.close()

    // Simulate retention pruning: delete the first two change rows, leaving
    // floor=3. The client's pretend cursor is since="1" < floor-1=2 → reset.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("DELETE FROM _sync_changes WHERE seq <= ?", secondSeq)
    })

    // Open a new socket and subscribe with since="1" — strictly below floor-1.
    const ws2 = await openWs(`/sync/${room}`)
    send(ws2, { t: "sub", subId: "s1", collection: "messages", since: "1" })
    // Expect: reset for s1, then snap frames for both rows, then snap-end.
    const frames = await collectUntil(ws2, (f) => f.t === "snap-end")

    const resetIdx = frames.findIndex((f) => f.t === "reset")
    expect(resetIdx).toBeGreaterThanOrEqual(0)
    const resetFrame = frames[resetIdx] as Extract<ServerFrame, { t: "reset" }>
    expect(resetFrame.sub).toBe("s1")

    const snapFrames = frames.filter((f) => f.t === "snap")
    expect(snapFrames.length).toBe(3) // all three rows in the fresh snapshot

    const snapEnd = frames[frames.length - 1]! as Extract<ServerFrame, { t: "snap-end" }>
    expect(snapEnd.t).toBe("snap-end")
    // reset must come before snap frames.
    const firstSnapIdx = frames.findIndex((f) => f.t === "snap")
    expect(resetIdx).toBeLessThan(firstSnapIdx)
    ws2.close()
  })
})
