import { env, runInDurableObject, SELF } from "cloudflare:test"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { UnsupportedPredicateError } from "../src/server/sql-compiler.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (ADR-0013 amendment 2026-11-17, column references): the SQL that builds a
// snapshot and the evaluator that decides the deltas must read the same column.
// The compiler read a ref's `path[0]` (in `where`) or its last element (in
// `orderBy`) and ignored every other field. The evaluator reads a ref by its whole
// shape: on @tanstack/db 1.0 it drops the first path element when it equals
// `sourceAlias`, and it walks a path of any length. So a ref the compiler read as
// `body` could be read by the evaluator as the whole row, or as `row.nope.body`.
// The library's own client sends exactly `{ type: "ref", path: [name] }` on 0.8.6,
// 0.9.2 and 1.0.0-rc.1.5 (measured through the adapter, in `where`, `orderBy` and
// `cursor`), so a ref of any other shape is refused like a query off the floor.

const codec = createFrameCodec()
const ref = (col: string) => ({ type: "ref", path: [col] })
const val = (value: unknown) => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>) => ({ type: "func", name, args })
const by = (expression: unknown, direction = "asc") => [{ expression, compareOptions: { direction, nulls: "first" } }]

type Sent = Record<string, unknown>
type Frame = ServerFrame & { sub?: string; fetchId?: string; rows?: Array<{ id: string }>; key?: string }

function stubFor(room: string): DurableObjectStub {
  return env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
}

async function seeded(room: string): Promise<void> {
  await runInDurableObject(stubFor(room), (inst, s) => {
    for (let i = 1; i <= 3; i++) s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", `m${i}`, `b${i}`)
    ;(inst as unknown as { sync: { drainAndBroadcast(): void } }).sync.drainAndBroadcast()
  })
}

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

/** Send `f` and collect every frame until `done`. */
function exchange(ws: WebSocket, f: Sent, done: (x: Frame) => boolean): Promise<Array<Frame>> {
  return new Promise((resolve, reject) => {
    const out: Array<Frame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((x) => x.t).join(",")}]`)), 3000)
    const onMsg = (e: MessageEvent): void => {
      const x = codec.decode(e.data as ArrayBuffer) as Frame
      out.push(x)
      if (done(x)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
    ws.send(codec.encode(f as ClientFrame))
  })
}

async function sub(ws: WebSocket, body: Sent): Promise<{ keys: Array<string>; end: string }> {
  const frames = await exchange(ws, { t: "sub", subId: "s1", collection: "messages", ...body }, (x) =>
    x.sub === "s1" && (x.t === "snap-end" || x.t === "reset" || x.t === "uptodate"),
  )
  const mine = frames.filter((x) => x.sub === "s1")
  return { keys: mine.filter((x) => x.t === "snap" || x.t === "d").map((x) => x.key!), end: mine.at(-1)!.t }
}

async function fetchPage(ws: WebSocket, body: Sent): Promise<Array<string>> {
  const frames = await exchange(ws, { t: "fetch", fetchId: "f1", collection: "messages", ...body }, (x) => x.fetchId === "f1")
  return frames.at(-1)!.rows!.map((r) => r.id)
}

type SnapshotReq = { collection: string; where?: unknown; orderBy?: unknown; limit?: number }
type Snapshot = { rows: Array<{ id: string }> }
function readSyncSnapshot(room: string, req: SnapshotReq): Promise<Snapshot | { error: unknown }> {
  return runInDurableObject(stubFor(room), (inst) =>
    (inst as unknown as { readSyncSnapshot: (r: SnapshotReq, request: Request) => Promise<Snapshot> })
      .readSyncSnapshot(req, new Request("https://example.com/ssr"))
      .catch((error: unknown) => ({ error })),
  )
}

// Each shape names `body`, a column `messages` has, in a form the evaluator does
// not read as `row.body`. `cause` is what the refusal must say. Before the fix
// each one was accepted and read as `body`.
const aliased = { type: "ref", path: ["body"], sourceAlias: "body" }
const SHAPES: Array<{ name: string; cause: RegExp; where?: unknown; orderBy?: unknown; limit?: number }> = [
  // The evaluator (1.0) drops `path[0]` when it equals `sourceAlias` and compares
  // the whole row with "b1": the snapshot sent m1, the deltas never match.
  { name: "where, a ref with sourceAlias", cause: /sourceAlias/, where: fn("eq", aliased, val("b1")) },
  // When `sourceAlias` differs the evaluator reads `path` whole, but no field
  // beside `type` and `path` is read the same way on every version, so any is refused.
  { name: "where, a ref with another field", cause: /bindingId/, where: fn("eq", { ...ref("body"), bindingId: "x" }, val("b1")) },
  { name: "where, a path that is not an array", cause: /path/, where: fn("eq", { type: "ref", path: { 0: "body", length: 1 } }, val("b1")) },
  { name: "where, a path element that is not a string", cause: /column name/, where: fn("eq", { type: "ref", path: [["body"]] }, val("b1")) },
  { name: "orderBy, a ref with sourceAlias", cause: /sourceAlias/, orderBy: by(aliased, "desc"), limit: 2 },
  // `orderBy` took the last element of a path; `where` requires exactly one. The
  // client's comparator reads `row.nope.body` (undefined, so key order: m1, m2)
  // where SQL sorted by `body` (m3, m2).
  { name: "orderBy, a two element path", cause: /nested/, orderBy: by({ type: "ref", path: ["nope", "body"] }, "desc"), limit: 2 },
  { name: "orderBy, a nested path the client sends for a nested field", cause: /nested/, orderBy: by({ type: "ref", path: ["body", "x"] }), limit: 2 },
]

// Frames that threw from the compiler instead of being refused, so a `sub` or a
// `fetch` got no reply at all and stayed loading. Not column refs, but the same
// parse: each is now refused with its cause, like the shapes above.
const MALFORMED: typeof SHAPES = [
  { name: "an orderBy term that is null", cause: /orderBy clause/, orderBy: [null], limit: 2 },
  { name: "a function with no args", cause: /args/, where: { type: "func", name: "eq" } },
  // `name in COMPARATORS` followed the prototype chain and wrote broken SQL.
  { name: "a function named after an Object.prototype member", cause: /not supported/, where: fn("toString", ref("body"), val("b1")) },
  // The compiler read `args[0]` and `args[1]`; the evaluator calls `args.map` and throws.
  { name: "a function whose args is not an array", cause: /args/, where: { type: "func", name: "eq", args: { 0: ref("body"), 1: val("b1"), length: 2 } } },
]

describe("a column reference the evaluator reads differently from SQL is refused", () => {
  let lines: Array<string>
  beforeEach(() => {
    lines = []
    vi.spyOn(console, "error").mockImplementation((...args: Array<unknown>) => void lines.push(args.map(String).join(" ")))
  })
  afterEach(() => vi.restoreAllMocks())

  for (const shape of [...SHAPES, ...MALFORMED]) {
    const { name: _n, cause, ...query } = shape
    it(`sub, ${shape.name}: reset, nothing registered, logged`, async () => {
      const room = `ref-sub-${crypto.randomUUID()}`
      await seeded(room)
      const ws = await openWs(room)
      expect(await sub(ws, query)).toEqual({ keys: [], end: "reset" })
      expect(lines.some((l) => cause.test(l))).toBe(true)
      const persisted = await runInDurableObject(stubFor(room), (_i, s) =>
        Array.from(s.storage.sql.exec("SELECT sub_id FROM _sync_subs")),
      )
      expect(persisted).toEqual([])
      ws.close()
    })

    it(`fetch, ${shape.name}: an empty page, logged`, async () => {
      const room = `ref-fetch-${crypto.randomUUID()}`
      await seeded(room)
      const ws = await openWs(room)
      expect(await fetchPage(ws, query)).toEqual([])
      expect(lines.some((l) => cause.test(l))).toBe(true)
      ws.close()
    })

    it(`readSyncSnapshot, ${shape.name}: throws`, async () => {
      const room = `ref-snap-${crypto.randomUUID()}`
      await seeded(room)
      const { error } = (await readSyncSnapshot(room, { collection: "messages", ...query })) as { error: Error }
      expect(error).toBeInstanceOf(UnsupportedPredicateError)
      expect(error.message).toMatch(cause)
    })
  }

  it("a cursor fetch whose cursor alone carries an aliased ref is refused", async () => {
    const room = `ref-cursor-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    const cursor = { whereCurrent: fn("eq", aliased, val("b1")), whereFrom: fn("gt", ref("body"), val("b1")) }
    expect(await fetchPage(ws, { orderBy: by(ref("body")), limit: 2, cursor })).toEqual([])
    expect(lines.some((l) => /sourceAlias/.test(l))).toBe(true)
    ws.close()
  })
})

describe("the ref the library's own client sends still works", () => {
  it("in a where, an orderBy and a cursor", async () => {
    const room = `ref-ok-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    const q = { where: fn("gt", ref("body"), val("b1")), orderBy: by(ref("body"), "desc"), limit: 1 }
    expect(await sub(ws, q)).toEqual({ keys: ["m3"], end: "snap-end" })
    expect(await fetchPage(ws, q)).toEqual(["m3"])
    expect(((await readSyncSnapshot(room, { collection: "messages", ...q })) as Snapshot).rows.map((r) => r.id)).toEqual(["m3"])
    const cursor = { whereCurrent: fn("eq", ref("body"), val("b3")), whereFrom: fn("lt", ref("body"), val("b3")) }
    expect(await fetchPage(ws, { orderBy: by(ref("body"), "desc"), limit: 1, cursor })).toEqual(["m3", "m2"])
    ws.close()
  })
})
