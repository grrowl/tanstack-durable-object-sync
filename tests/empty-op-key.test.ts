import type { SqlStorage } from "@cloudflare/workers-types"
import { createCollection } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { MutationRejectedError, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, MutOp, ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: a `mut` op's key is the row's pk, and the pk is client-supplied TEXT
// (ADR-0001 D9 / 0007). `""` is never a real row identity, so the server refuses
// it before anything is written (ADR-0025, amending ADR-0012 D1 for op keys):
//   1. The refusal is a `rejected` VALIDATION reply, not a silent drop. The
//      client learns at once and rolls back, instead of waiting out a
//      confirmation timeout that looks like a network fault.
//   2. Nothing is applied: no row, no change-log entry, no delta to any
//      subscriber, and a mixed batch is refused whole.
//   3. The dedup lookup runs first: a resent txId gets its STORED outcome, even
//      if the resent frame carries an empty key.

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
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

const receipt = (f: ServerFrame): boolean => f.t === "committed" || f.t === "rejected"

async function subscribe(ws: WebSocket, subId: string): Promise<void> {
  send(ws, { t: "sub", subId, collection: "messages" })
  await collectUntil(ws, (f) => f.t === "snap-end")
}

function stub(room: string) {
  return env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
}

/** The messages table and the change-log length, read inside the DO. */
async function snapshot(room: string): Promise<{ rows: Array<{ id: string; body: string }>; changes: number }> {
  return runInDurableObject(stub(room), (_i, s) => ({
    rows: Array.from(s.storage.sql.exec("SELECT id, body FROM messages ORDER BY id")) as Array<{ id: string; body: string }>,
    changes: (Array.from(s.storage.sql.exec("SELECT COUNT(*) AS n FROM _sync_changes"))[0] as { n: number }).n,
  }))
}

/** Send one mut on a fresh socket while a second socket is subscribed. Returns
 *  the receipt, the DO state right after it, and every delta the subscriber got
 *  up to a valid SENTINEL write sent afterwards. Deltas reach a subscriber in
 *  commit order, so anything the mut broadcast arrives before the sentinel's
 *  delta: no timing window to lose a late frame in. */
async function mutWithWatcher(room: string, txId: string, ops: Array<MutOp>) {
  const watcher = await openWs(`/sync/${room}`)
  await subscribe(watcher, "w1")
  const ws = await openWs(`/sync/${room}`)
  // Listen BEFORE the mut is sent, so an early broadcast can't slip past.
  const sentinel = `${txId}-sentinel`
  const watched = collectUntil(watcher, (f) => f.t === "d" && f.key === sentinel)
  send(ws, { t: "mut", txId, collection: "messages", ops })
  const last = (await collectUntil(ws, receipt)).at(-1)!
  const after = await snapshot(room)

  send(ws, { t: "mut", txId: sentinel, collection: "messages", ops: [{ type: "insert", key: sentinel, cols: { id: sentinel, body: "s" } }] })
  expect((await collectUntil(ws, receipt)).at(-1)!.t).toBe("committed")
  const deltas = (await watched).filter((f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && f.key !== sentinel)
  ws.close()
  watcher.close()
  return { last, after, deltas }
}

function expectKeyRejected(last: ServerFrame): void {
  expect(last.t).toBe("rejected")
  const r = last as Extract<ServerFrame, { t: "rejected" }>
  expect(r.error.code).toBe("VALIDATION")
  expect(r.error.message).toMatch(/non-empty/)
}

describe("empty op key (ADR-0025)", () => {
  it("insert with key '' → rejected VALIDATION; no row, no change, no delta", async () => {
    const room = "eok-insert"
    const before = await snapshot(room)

    // "FORBIDDEN" makes the author's authorize throw: getting the key error
    // instead proves the check runs BEFORE authorize.
    const { last, after, deltas } = await mutWithWatcher(room, "eok-i1", [
      { type: "insert", key: "", cols: { id: "", body: "FORBIDDEN" } },
    ])

    expectKeyRejected(last)
    expect(deltas).toEqual([])
    expect(after).toEqual(before)
  })

  // update/delete: a row with id '' is seeded server-side (legacy data an older
  // server let through), so an applied op WOULD change something observable.
  // Seeded via runSyncedWrite so its own delta is drained before the watcher
  // subscribes (ADR-0006: a raw write is captured but not broadcast).
  for (const type of ["update", "delete"] as const) {
    it(`${type} with key '' → rejected VALIDATION; seeded row untouched, no change, no delta`, async () => {
      const room = `eok-${type}`
      await runInDurableObject(stub(room), (i) => {
        ;(i as unknown as { runSyncedWrite: (fn: (sql: SqlStorage) => void) => void }).runSyncedWrite((sql) => {
          sql.exec("INSERT INTO messages(id, body) VALUES ('', 'seed')")
        })
      })
      const before = await snapshot(room)
      expect(before.rows).toEqual([{ id: "", body: "seed" }])

      const op: MutOp = type === "update" ? { type, key: "", cols: { body: "changed" } } : { type, key: "" }
      const { last, after, deltas } = await mutWithWatcher(room, `eok-${type}1`, [op])

      expectKeyRejected(last)
      expect(deltas).toEqual([])
      expect(after).toEqual(before)
    })
  }

  it("a batch with one empty key is refused whole: its valid op is not applied", async () => {
    const room = "eok-mixed"
    const before = await snapshot(room)

    const { last, after, deltas } = await mutWithWatcher(room, "eok-m1", [
      { type: "insert", key: "k1", cols: { id: "k1", body: "good" } },
      { type: "delete", key: "" },
    ])

    expectKeyRejected(last)
    expect(deltas).toEqual([])
    expect(after).toEqual(before)
  })

  // Also the positive control for mutWithWatcher: the watcher DOES see a
  // committed write's delta, so the empty `deltas` above are meaningful.
  it("non-empty keys are unaffected, including whitespace ' ' (a valid TEXT pk)", async () => {
    const room = "eok-control"
    for (const [txId, key] of [
      ["eok-c1", "k"],
      ["eok-c2", " "],
    ] as const) {
      const { last, after, deltas } = await mutWithWatcher(room, txId, [{ type: "insert", key, cols: { id: key, body: "ok" } }])
      expect(last.t).toBe("committed")
      expect(after.rows).toContainEqual({ id: key, body: "ok" })
      expect(deltas.map((d) => d.key)).toEqual([key])
    }
  })

  it("dedup runs first: a committed txId resent with key '' replays its committed receipt", async () => {
    const room = "eok-dedup"
    const ws = await openWs(`/sync/${room}`)
    send(ws, { t: "mut", txId: "eok-d1", collection: "messages", ops: [{ type: "insert", key: "k1", cols: { id: "k1", body: "v" } }] })
    const first = (await collectUntil(ws, receipt)).at(-1)!
    expect(first.t).toBe("committed")

    // Same txId, now with an empty key: the stored outcome wins (exactly-once).
    send(ws, { t: "mut", txId: "eok-d1", collection: "messages", ops: [{ type: "delete", key: "" }] })
    const again = (await collectUntil(ws, receipt)).at(-1)!
    expect(again).toMatchObject({ t: "committed", txId: "eok-d1", seq: (first as { seq: string }).seq })
    expect((await snapshot(room)).rows).toEqual([{ id: "k1", body: "v" }])
    ws.close()
  })

  it("the rejection is recorded: the same txId resent with a valid op replays VALIDATION, applies nothing", async () => {
    const room = "eok-dedup-rej"
    const ws = await openWs(`/sync/${room}`)
    send(ws, { t: "mut", txId: "eok-r1", collection: "messages", ops: [{ type: "delete", key: "" }] })
    expectKeyRejected((await collectUntil(ws, receipt)).at(-1)!)

    send(ws, { t: "mut", txId: "eok-r1", collection: "messages", ops: [{ type: "insert", key: "k1", cols: { id: "k1", body: "v" } }] })
    expectKeyRejected((await collectUntil(ws, receipt)).at(-1)!)
    expect((await snapshot(room)).rows).toEqual([])
    ws.close()
  })
})

describe("empty op key, at client altitude (ADR-0025)", () => {
  // WHY: the point of replying instead of dropping. A collection insert with pk
  // '' must fail promptly as a MutationRejectedError (VALIDATION) and roll the
  // optimistic row back — not sit until the transport's confirmation timeout.
  it("coll.insert({ id: '' }) → prompt MutationRejectedError VALIDATION + rollback", async () => {
    const room = "eok-client"
    const t = new WebSocketTransport<TestApi>({
      url: `https://example.com/sync/${room}`,
      // Long enough that a dropped frame could not be mistaken for a prompt reply.
      timeoutMs: 30_000,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        return ws as unknown as WebSocketLike
      },
    })
    await t.connect()

    const coll = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }))
    await coll.preload()

    const tx = coll.insert({ id: "", body: "empty pk" })
    // The optimistic overlay applies immediately...
    expect(coll.get("")).toMatchObject({ body: "empty pk" })

    const outcome = await Promise.race([
      tx.isPersisted.promise.then(
        () => ({ settled: "resolved" as const, err: undefined as unknown }),
        (err: unknown) => ({ settled: "rejected" as const, err }),
      ),
      new Promise<{ settled: "pending"; err: unknown }>((r) => setTimeout(() => r({ settled: "pending", err: undefined }), 2000)),
    ])
    expect(outcome.settled).toBe("rejected")

    // TanStack may wrap the mutationFn error; find the transport's error in the cause chain.
    let cause: unknown = outcome.err
    while (cause != null && !(cause instanceof MutationRejectedError)) cause = (cause as { cause?: unknown }).cause
    expect(cause).toBeInstanceOf(MutationRejectedError)
    expect((cause as MutationRejectedError).code).toBe("VALIDATION")

    // ...and is rolled back; the server holds nothing.
    expect(coll.get("")).toBeUndefined()
    expect((await snapshot(room)).rows).toEqual([])
    t.close()
  })
})
