import { SELF } from "cloudflare:test"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: the server's log lines quote strings a client chose (sub ids, fetch ids,
// collection names, whole malformed frames). Unclipped, any client can make the
// Durable Object write a log line as long as a frame allows: a 600 000
// character sub id once stalled a CI runner for minutes on one line. Each quoted
// client string is clipped (ADR-0012 amendment 2026-11-17); a normal id stays
// whole.

const codec = createFrameCodec()
const CAP = 2000 // far above a clipped line, far below an unclipped one
async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
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
})
