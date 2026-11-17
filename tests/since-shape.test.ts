import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (ADR-0012 amendment 2026-11-17, `since`): the server read `since` with
// `Number()`, so `"1e400"` became Infinity and was answered `uptodate` at the
// current position: the client was told it was up to date without being sent
// anything. `"abc"` became NaN and got a snapshot. A position is a SQLite
// INTEGER, so `since` must be its plain decimal form; any other `since` is a
// malformed frame (D1: dropped, logged, no reply). A well formed `since` past
// the current position cannot be a position this object gave out, so it gets
// `reset` and a snapshot, never `uptodate`.

const codec = createFrameCodec()
type Frame = ServerFrame & { sub?: string }

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

/** Everything received until `done`, which must arrive within the timeout. */
function collectUntil(ws: WebSocket, done: (f: Frame) => boolean, timeoutMs = 2000): Promise<Array<Frame>> {
  return new Promise((resolve, reject) => {
    const out: Array<Frame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as Frame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

const send = (ws: WebSocket, f: Record<string, unknown>): void => ws.send(codec.encode(f as ClientFrame))

/** A socket on a fresh object whose change log holds one row, so a `since`
 *  reaches the catch-up branch (an empty log always snapshots). Returns the
 *  current position. */
async function seeded(room: string): Promise<{ ws: WebSocket; seq: string }> {
  const ws = await openWs(room)
  send(ws, { t: "mut", txId: `${room}-t1`, collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "1" } }] })
  const frames = await collectUntil(ws, (f) => f.t === "committed")
  return { ws, seq: (frames.at(-1) as Extract<ServerFrame, { t: "committed" }>).seq }
}

const storedSubIds = (room: string): Promise<Array<string>> =>
  runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) =>
    Array.from(s.storage.sql.exec<{ sub_id: string }>("SELECT sub_id FROM _sync_subs"), (r) => r.sub_id),
  )

describe("`since` must be a plain decimal position (ADR-0012 amendment 2026-11-17)", () => {
  const malformed: Array<[string, string]> = [
    ["an exponent that overflows to Infinity", "1e400"],
    ["not a number", "abc"],
    ["negative", "-1"],
    ["a fraction", "1.5"],
    ["leading space", " 1"],
    ["empty", ""],
    ["a leading zero", "01"],
    ["hex", "0x10"],
    ["twenty digits, longer than any SQLite INTEGER", "1" + "0".repeat(19)],
    ["a very long digit string", "9".repeat(10_000)],
  ]

  for (const [name, since] of malformed) {
    it(`a sub whose since is ${name} is dropped: no reply, nothing stored`, async () => {
      const room = `since-bad-${name.replace(/\W+/g, "-")}`
      const { ws } = await seeded(room)
      send(ws, { t: "sub", subId: "bad", collection: "messages", since })
      // Frames on one socket are handled in order: anything sent for `bad`
      // arrives before `ok`'s snapshot ends.
      send(ws, { t: "sub", subId: "ok", collection: "messages" })
      const frames = await collectUntil(ws, (f) => f.t === "snap-end" && f.sub === "ok")
      expect(frames.filter((f) => f.sub === "bad")).toEqual([])
      expect(frames.some((f) => f.t === "uptodate")).toBe(false)
      expect(await storedSubIds(room)).toEqual(["ok"])
      ws.close()
    })
  }

  it("a since past the current position gets reset and a snapshot, not uptodate", async () => {
    const { ws, seq } = await seeded("since-past-end")
    send(ws, { t: "sub", subId: "s", collection: "messages", since: String(BigInt(seq) + 1n) })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end" && f.sub === "s")
    expect(frames.map((f) => f.t)).toEqual(["reset", "snap", "snap-end"])
    ws.close()
  })

  it("the largest well formed since (19 digits) is answered, with reset and a snapshot", async () => {
    const { ws } = await seeded("since-19-digits")
    send(ws, { t: "sub", subId: "s", collection: "messages", since: "9".repeat(19) })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end" && f.sub === "s")
    expect(frames.map((f) => f.t)).toEqual(["reset", "snap", "snap-end"])
    ws.close()
  })

  it("a since at the current position still catches up (control)", async () => {
    const { ws, seq } = await seeded("since-at-end")
    send(ws, { t: "sub", subId: "s", collection: "messages", since: seq })
    const frames = await collectUntil(ws, (f) => f.t === "uptodate")
    expect(frames.map((f) => f.t)).toEqual(["uptodate"])
    ws.close()
  })
})
