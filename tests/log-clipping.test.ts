import { SELF } from "cloudflare:test"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import { whereJustUnderStoredCap } from "./row-over-cap.ts"

// WHY: the server's log lines quote strings a client chose (sub ids, fetch ids,
// collection names, whole malformed frames). Unclipped, any client can make the
// Durable Object write a log line as long as a frame allows: a 600 000
// character sub id once stalled a CI runner for minutes on one line. Each quoted
// client string is clipped (ADR-0012 amendment 2026-11-17); a normal id stays
// whole.

const codec = createFrameCodec()
const CAP = 2000 // far above a clipped line, far below an unclipped one
async function openWs(room: string, route = "sync"): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/${route}/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}
const send = (ws: WebSocket, f: unknown): void => ws.send(codec.encode(f as ClientFrame))
function next(ws: WebSocket, done: (f: ServerFrame) => boolean): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timeout")), 3000)
    ws.addEventListener("message", function onMsg(e) {
      if (done(codec.decode((e as MessageEvent).data as ArrayBuffer) as ServerFrame)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve()
      }
    })
  })
}
const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })

describe("log lines clip the strings a client chose", () => {
  let lines: Array<string>
  beforeEach(() => {
    lines = []
    const capture = (...args: Array<unknown>): void => void lines.push(args.map(String).join(" "))
    vi.spyOn(console, "error").mockImplementation(capture)
    vi.spyOn(console, "warn").mockImplementation(capture)
  })
  afterEach(() => vi.restoreAllMocks())

  const long = "s".repeat(600_000)
  const normal = "messages#3"
  // The longest id the server accepts (ADR-0012 amendment 2026-11-17), still
  // longer than a log line keeps. The refusals below come after the id check,
  // so only an id within the limit reaches them.
  const longest = "s".repeat(256)

  it("a refused sub with a long id or a long column name", async () => {
    const ws = await openWs(`clip-sub-${crypto.randomUUID()}`)
    send(ws, { t: "sub", subId: long, collection: "messages", where: fn("ne", ref("body"), val("x")) })
    await next(ws, (f) => f.t === "reset")
    send(ws, { t: "sub", subId: "s2", collection: "messages", where: fn("eq", ref(`bad-${long}`), val("x")) })
    await next(ws, (f) => f.t === "reset")
    send(ws, { t: "sub", subId: normal, collection: "messages", where: fn("ne", ref("body"), val("x")) })
    await next(ws, (f) => f.t === "reset")
    ws.close()
    expect(lines.length).toBeGreaterThanOrEqual(3)
    expect(lines.filter((l) => l.length > CAP).map((l) => l.length)).toEqual([])
    expect(lines.some((l) => l.includes(`clipped, ${long.length} chars`))).toBe(true)
    expect(lines.some((l) => l.includes(`'${normal}'`)), "a normal id stays whole").toBe(true)
  })

  it("a refused fetch with a long id, and a malformed frame", async () => {
    const ws = await openWs(`clip-fetch-${crypto.randomUUID()}`)
    send(ws, { t: "fetch", fetchId: long, collection: "messages", cursor: { whereFrom: null } })
    await next(ws, (f) => f.t === "page")
    // A sub whose subId is not a string: dropped as malformed, logged, no reply.
    send(ws, { t: "sub", subId: 1, collection: long })
    send(ws, { t: "fetch", fetchId: "probe", collection: "messages" })
    await next(ws, (f) => f.t === "page")
    ws.close()
    expect(lines.length).toBeGreaterThanOrEqual(2)
    expect(lines.filter((l) => l.length > CAP).map((l) => l.length)).toEqual([])
  })

  /** The one line logged for `marker`: short, and noting what was clipped. */
  const expectClipped = (marker: string): void => {
    const hits = lines.filter((l) => l.includes(marker))
    expect(hits, `a line containing "${marker}"`).toHaveLength(1)
    expect(hits[0]!.length).toBeLessThanOrEqual(CAP)
    expect(hits[0]).toMatch(/clipped, \d+ chars/)
  }
  const dates = (n: number): Array<Date> => Array.from({ length: n }, (_, i) => new Date(1_700_000_000_000 + i))

  // The line that stalled CI: the backstop's refusal, reached when the
  // `_sync_subs` row is over SQLite's value cap. A 600 000 character sub id did
  // that then; now the id is limited, so the predicate sits just under the cap.
  it("a sub refused by the SQLite-limit backstop", async () => {
    const ws = await openWs(`clip-backstop-${crypto.randomUUID()}`)
    send(ws, { t: "sub", subId: longest, collection: "messages", where: whereJustUnderStoredCap() })
    await next(ws, (f) => f.t === "reset")
    ws.close()
    expectClipped("refused at a SQLite limit")
  })

  it("a fetch refused for its predicate", async () => {
    const ws = await openWs(`clip-fetch-refused-${crypto.randomUUID()}`)
    send(ws, { t: "fetch", fetchId: longest, collection: "messages", where: fn("ne", ref("body"), val("x")) })
    await next(ws, (f) => f.t === "page")
    ws.close()
    expectClipped("operator 'ne' is not supported")
  })

  it("a sub whose predicate is too large to store", async () => {
    const ws = await openWs(`clip-store-${crypto.randomUUID()}`)
    send(ws, { t: "sub", subId: longest, collection: "messages", where: fn("in", ref("id"), val(dates(60_000))) })
    await next(ws, (f) => f.t === "reset")
    ws.close()
    expectClipped("predicate too large to store")
  })

  it("a sub refused at the per-socket cap", async () => {
    // LimitsTestDO allows 2 subs per socket.
    const ws = await openWs(`clip-cap-${crypto.randomUUID()}`, "limits")
    for (const subId of ["a", "b"]) {
      send(ws, { t: "sub", subId, collection: "messages" })
      await next(ws, (f) => f.t === "snap-end")
    }
    send(ws, { t: "sub", subId: longest, collection: "messages" })
    await next(ws, (f) => f.t === "reset")
    ws.close()
    expectClipped("maxSubsPerSocket")
  })
})
