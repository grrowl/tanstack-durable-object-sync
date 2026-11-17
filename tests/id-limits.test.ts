import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { encodeResult } from "../src/server/dedup.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (ADR-0012 amendment 2026-11-17, id lengths): the only bound on a client's
// ids was the frame size. A sub id is echoed in every frame of its sub and
// stored in `_sync_subs`, so a 600 000 character sub id made every delta for it
// that long; a txId is stored in `_sync_seen_tx`. An id (`subId`, `fetchId`,
// `txId`) may be at most 256 UTF-8 bytes, and a collection or command name at
// most 128. A frame over a limit gets its own frame type's refusal, which
// echoes the id once, and nothing is stored or registered: `reset` for a sub,
// an empty `page` for a fetch, `rejected` with LIMIT_EXCEEDED for a mut or a
// call. An unsub over the limit is ignored. The unit is the UTF-8 byte, the form
// in which an id is stored and sent, so a non-ASCII id is counted by its bytes.

const codec = createFrameCodec()
type Frame = ServerFrame & { sub?: string; fetchId?: string; txId?: string }

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

/** Records every frame from the start, so a test can look at all of them. */
function recorder(ws: WebSocket): { frames: Array<Frame>; until: (done: (f: Frame) => boolean) => Promise<void> } {
  const frames: Array<Frame> = []
  const waiters: Array<{ done: (f: Frame) => boolean; resolve: () => void }> = []
  ws.addEventListener("message", (e: MessageEvent) => {
    const f = codec.decode(e.data as ArrayBuffer) as Frame
    frames.push(f)
    for (const w of [...waiters]) if (w.done(f)) waiters.splice(waiters.indexOf(w), 1), w.resolve()
  })
  const until = (done: (f: Frame) => boolean): Promise<void> => {
    if (frames.some(done)) return Promise.resolve()
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout; got [${frames.map((f) => f.t).join(",")}]`)), 2000)
      waiters.push({ done, resolve: () => (clearTimeout(timer), resolve()) })
    })
  }
  return { frames, until }
}

const send = (ws: WebSocket, f: Record<string, unknown>): void => ws.send(codec.encode(f as ClientFrame))
const insert = (txId: string, id: string, collection = "messages") => ({
  t: "mut",
  txId,
  collection,
  ops: [{ type: "insert", key: id, cols: { id, body: id } }],
})

const query = <T,>(room: string, sql: string): Promise<Array<T>> =>
  runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) =>
    Array.from(s.storage.sql.exec(sql)) as Array<T>,
  )

/** Ids at 256 UTF-8 bytes and one byte over, in three widths of character. */
const widths: Array<[string, string, string]> = [
  ["ASCII", "i".repeat(256), "i".repeat(257)],
  ["two-byte", "é".repeat(128), "é".repeat(128) + "i"],
  ["four-byte", "😀".repeat(64), "😀".repeat(64) + "i"],
]

describe("ids over 256 UTF-8 bytes are refused with their own reply (ADR-0012 amendment 2026-11-17)", () => {
  for (const [width, at, over] of widths) {
    it(`sub (${width}): at the limit is served; over it gets one reset, nothing stored, no later delta`, async () => {
      const room = `idlim-sub-${width}-${crypto.randomUUID()}`
      const ws = await openWs(room)
      const r = recorder(ws)
      send(ws, { t: "sub", subId: over, collection: "messages" })
      send(ws, { t: "sub", subId: at, collection: "messages" })
      await r.until((f) => f.t === "snap-end" && f.sub === at)
      send(ws, insert("t1", "a"))
      await r.until((f) => f.t === "committed")

      expect(r.frames.filter((f) => f.sub === over)).toEqual([{ t: "reset", sub: over }])
      expect(r.frames.some((f) => f.t === "d" && f.sub === at && f.key === "a")).toBe(true)
      const stored = await query<{ sub_id: string }>(room, "SELECT sub_id FROM _sync_subs")
      expect(stored.map((s) => s.sub_id)).toEqual([at])
      ws.close()
    })

    it(`fetch (${width}): at the limit gets rows; over it gets an empty page`, async () => {
      const room = `idlim-fetch-${width}-${crypto.randomUUID()}`
      const ws = await openWs(room)
      const r = recorder(ws)
      send(ws, insert("t1", "a"))
      await r.until((f) => f.t === "committed")
      send(ws, { t: "fetch", fetchId: over, collection: "messages" })
      send(ws, { t: "fetch", fetchId: at, collection: "messages" })
      await r.until((f) => f.t === "page" && f.fetchId === at)

      const pages = r.frames.filter((f): f is Extract<ServerFrame, { t: "page" }> => f.t === "page")
      expect(pages.map((p) => [p.fetchId, p.rows.length])).toEqual([
        [over, 0],
        [at, 1],
      ])
      ws.close()
    })

    it(`mut (${width}): at the limit commits; over it is rejected, not written, not recorded, also when resent`, async () => {
      const room = `idlim-mut-${width}-${crypto.randomUUID()}`
      const ws = await openWs(room)
      const r = recorder(ws)
      send(ws, insert(over, "a"))
      send(ws, insert(over, "a"))
      send(ws, insert(at, "b"))
      await r.until((f) => f.t === "committed" && f.txId === at)

      const refused = r.frames.filter((f) => f.txId === over)
      expect(refused).toHaveLength(2)
      for (const f of refused) {
        expect(f).toMatchObject({ t: "rejected", error: { code: "LIMIT_EXCEEDED", message: "txId is over 256 bytes" } })
      }
      expect(await query(room, "SELECT id FROM messages")).toEqual([{ id: "b" }])
      expect(await query(room, "SELECT tx_id FROM _sync_seen_tx")).toEqual([{ tx_id: at }])
      ws.close()
    })

    it(`call (${width}): at the limit runs; over it is rejected, not run, not recorded`, async () => {
      const room = `idlim-call-${width}-${crypto.randomUUID()}`
      const ws = await openWs(room)
      const r = recorder(ws)
      send(ws, insert("t1", "a"))
      await r.until((f) => f.t === "committed")
      send(ws, { t: "call", txId: over, name: "clearMessages", args: null })
      send(ws, { t: "call", txId: at, name: "echo", args: 1 })
      await r.until((f) => f.t === "committed" && f.txId === at)

      expect(r.frames.filter((f) => f.txId === over)).toEqual([
        { t: "rejected", txId: over, error: { code: "LIMIT_EXCEEDED", message: "txId is over 256 bytes" } },
      ])
      expect(await query(room, "SELECT id FROM messages")).toEqual([{ id: "a" }]) // clearMessages never ran
      const seen = await query<{ tx_id: string }>(room, "SELECT tx_id FROM _sync_seen_tx ORDER BY tx_id")
      expect(seen.map((s) => s.tx_id).sort()).toEqual([at, "t1"].sort())
      ws.close()
    })
  }

  it("a receipt stored under a long txId before the limit still replays, for a mut and a call (ADR-0025)", async () => {
    // The dedup lookup runs before every rejection, the length check included,
    // so a receipt recorded before this limit existed is still answered.
    const room = `idlim-replay-${crypto.randomUUID()}`
    const ws = await openWs(room)
    const r = recorder(ws)
    const mutTx = "m".repeat(300)
    const callTx = "c".repeat(300)
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      const row = "INSERT INTO _sync_seen_tx(tx_id, ok, cursor, error, error_code, result, ts) VALUES (?, 1, ?, NULL, NULL, ?, ?)"
      s.storage.sql.exec(row, mutTx, "7", null, Date.now())
      s.storage.sql.exec(row, callTx, "7", encodeResult({ ok: true }), Date.now())
    })
    send(ws, insert(mutTx, "a"))
    send(ws, { t: "call", txId: callTx, name: "clearMessages", args: null })
    await r.until((f) => f.txId === callTx)
    const mutFrames = r.frames.filter((f) => f.txId === mutTx)
    expect(mutFrames).toHaveLength(1)
    expect(mutFrames[0]).toMatchObject({ t: "committed", txId: mutTx, seq: "7" })
    expect(r.frames.find((f) => f.txId === callTx)).toEqual({ t: "committed", txId: callTx, seq: "7", result: { ok: true } })
    expect(await query(room, "SELECT id FROM messages")).toEqual([]) // replayed, not run
    ws.close()
  })

  it("unsub over the limit is ignored: no reply, and the socket's subs keep their deltas", async () => {
    const room = `idlim-unsub-${crypto.randomUUID()}`
    const ws = await openWs(room)
    const r = recorder(ws)
    send(ws, { t: "sub", subId: "ok", collection: "messages" })
    await r.until((f) => f.t === "snap-end")
    send(ws, { t: "unsub", subId: "i".repeat(257) })
    send(ws, insert("t1", "a"))
    await r.until((f) => f.t === "committed")
    expect(r.frames.some((f) => f.t === "d" && f.sub === "ok" && f.key === "a")).toBe(true)
    expect(r.frames.filter((f) => f.t === "reset" || f.t === "rejected" || (f.sub ?? "ok") !== "ok")).toEqual([])
    ws.close()
  })

  it("a mut whose txId and collection are both over is answered once, for the txId", async () => {
    const room = `idlim-both-${crypto.randomUUID()}`
    const ws = await openWs(room)
    const r = recorder(ws)
    const over = "i".repeat(257)
    send(ws, insert(over, "a", "m".repeat(129)))
    send(ws, insert("t1", "b"))
    await r.until((f) => f.t === "committed")
    expect(r.frames.filter((f) => f.t === "rejected")).toEqual([
      { t: "rejected", txId: over, error: { code: "LIMIT_EXCEEDED", message: "txId is over 256 bytes" } },
    ])
    expect(await query(room, "SELECT tx_id FROM _sync_seen_tx")).toEqual([{ tx_id: "t1" }])
    ws.close()
  })
})

describe("names over 128 UTF-8 bytes (ADR-0012 amendment 2026-11-17)", () => {
  it("a mut on a collection name over the limit is rejected with LIMIT_EXCEEDED, without quoting it", async () => {
    const room = `nmlim-mut-${crypto.randomUUID()}`
    const ws = await openWs(room)
    const r = recorder(ws)
    send(ws, insert("t-over", "a", "é".repeat(64) + "m")) // 129 bytes
    send(ws, insert("t-at", "b", "é".repeat(64))) // 128 bytes: an unknown collection, as before
    await r.until((f) => f.t === "rejected" && f.txId === "t-at")

    const byTx = (txId: string) => r.frames.find((f) => f.txId === txId) as Extract<ServerFrame, { t: "rejected" }>
    expect(byTx("t-over").error).toEqual({ code: "LIMIT_EXCEEDED", message: "collection name is over 128 bytes" })
    expect(byTx("t-at").error.code).toBeUndefined()
    expect(byTx("t-at").error.message).toMatch(/no mutation handler/)
    // A valid txId is recorded as any rejection is (ADR-0025), with the short message.
    const seen = await query<{ tx_id: string; error: string }>(room, "SELECT tx_id, error FROM _sync_seen_tx WHERE tx_id = 't-over'")
    expect(seen).toEqual([{ tx_id: "t-over", error: "collection name is over 128 bytes" }])
    ws.close()
  })

  it("a call on a command name over the limit is rejected with LIMIT_EXCEEDED; at the limit it is unknown", async () => {
    const room = `nmlim-call-${crypto.randomUUID()}`
    const ws = await openWs(room)
    const r = recorder(ws)
    send(ws, { t: "call", txId: "c-over", name: "c".repeat(129), args: null })
    send(ws, { t: "call", txId: "c-at", name: "c".repeat(128), args: null })
    await r.until((f) => f.t === "rejected" && f.txId === "c-at")
    const byTx = (txId: string) => r.frames.find((f) => f.txId === txId) as Extract<ServerFrame, { t: "rejected" }>
    expect(byTx("c-over").error).toEqual({ code: "LIMIT_EXCEEDED", message: "command name is over 128 bytes" })
    expect(byTx("c-at").error.code).toBe("UNKNOWN_COMMAND")
    ws.close()
  })

  it("a sub or fetch on a collection name over the limit is refused as unknown, and nothing is stored", async () => {
    const room = `nmlim-sub-${crypto.randomUUID()}`
    const ws = await openWs(room)
    const r = recorder(ws)
    send(ws, { t: "sub", subId: "s", collection: "m".repeat(129) })
    send(ws, { t: "fetch", fetchId: "f", collection: "m".repeat(129) })
    await r.until((f) => f.t === "page")
    expect(r.frames).toEqual([
      { t: "reset", sub: "s" },
      { t: "page", fetchId: "f", rows: [], seq: "0" },
    ])
    expect(await query(room, "SELECT sub_id FROM _sync_subs")).toEqual([])
    ws.close()
  })
})
