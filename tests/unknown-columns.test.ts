import { env, evictDurableObject, runInDurableObject, SELF } from "cloudflare:test"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { UnsupportedPredicateError } from "../src/server/sql-compiler.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (ADR-0013 amendment 2026-11-17, unknown columns): the compiler writes a
// column as a double quoted name, and SQLite reads a double quoted name that
// matches no column as a string literal, in WHERE and in ORDER BY alike. The
// evaluator that decides deltas reads that column as undefined. So a typo in a
// column name gave a snapshot of every row (or none) and deltas that disagreed
// with it, with no refusal and no log line. SQLite also resolves a name in any
// case (`BODY` finds `body`) and resolves `rowid`, neither of which a row from
// `SELECT *` carries. A query that names a column `SELECT *` does not return is
// refused, like any query off the floor.

const codec = createFrameCodec()
const ref = (col: string) => ({ type: "ref", path: [col] })
const val = (value: unknown) => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>) => ({ type: "func", name, args })
const by = (col: string, direction = "asc") => [{ expression: ref(col), compareOptions: { direction, nulls: "first" } }]

type Sent = Record<string, unknown>
type Frame = ServerFrame & { sub?: string; fetchId?: string; rows?: Array<{ id: string }>; key?: string }

function stubFor(room: string): DurableObjectStub {
  return env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
}

async function seeded(room: string, setup = ""): Promise<void> {
  await runInDurableObject(stubFor(room), (inst, s) => {
    if (setup) s.storage.sql.exec(setup)
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

/** The rows a sub's snapshot sent, and the frame that ended it. */
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
/** `readSyncSnapshot` on the instance, settled inside it: an error thrown across
 *  the RPC boundary is also reported as an unhandled rejection by the test pool. */
function readSyncSnapshot(room: string, req: SnapshotReq): Promise<Snapshot | { error: unknown }> {
  return runInDurableObject(stubFor(room), (inst) =>
    (inst as unknown as { readSyncSnapshot: (r: SnapshotReq, request: Request) => Promise<Snapshot> })
      .readSyncSnapshot(req, new Request("https://example.com/ssr"))
      .catch((error: unknown) => ({ error })),
  )
}
const snapshotIds = async (room: string, req: SnapshotReq): Promise<Array<string>> =>
  ((await readSyncSnapshot(room, req)) as Snapshot).rows.map((r) => r.id)

// Each shape names `nope`, which `messages (id, body)` does not have; `column`
// is the name its log line must carry. Before the fix, those marked "every row"
// snapshot all 3 rows and those marked "none" snapshot none.
const SHAPES: Array<{ name: string; column: string; where?: unknown; orderBy?: unknown; limit?: number }> = [
  { name: "eq (every row)", column: "nope", where: fn("eq", ref("nope"), val("nope")) },
  { name: "gt (every row)", column: "nope", where: fn("gt", ref("nope"), val(5)) },
  { name: "in (every row)", column: "nope", where: fn("in", ref("nope"), val(["nope", "x"])) },
  { name: "like (every row)", column: "nope", where: fn("like", ref("nope"), val("no%")) },
  { name: "not isNull (every row)", column: "nope", where: fn("not", fn("isNull", ref("nope"))) },
  { name: "eq (none)", column: "nope", where: fn("eq", ref("nope"), val("other")) },
  { name: "isNull (none)", column: "nope", where: fn("isNull", ref("nope")) },
  { name: "isUndefined (none; the evaluator says every row)", column: "nope", where: fn("isUndefined", ref("nope")) },
  { name: "inside an or", column: "nope", where: fn("or", fn("eq", ref("body"), val("b1")), fn("eq", ref("nope"), val("x"))) },
  { name: "orderBy with a limit", column: "nope", orderBy: by("nope"), limit: 2 },
  { name: "orderBy after a valid where", column: "nope", where: fn("gt", ref("id"), val("")), orderBy: by("nope") },
  // SQLite resolves these; a row from `SELECT *` has no such key.
  { name: "a column in another case", column: "BODY", where: fn("eq", ref("BODY"), val("b1")) },
  { name: "rowid", column: "rowid", where: fn("gt", ref("rowid"), val(0)) },
  { name: "orderBy rowid", column: "rowid", orderBy: by("rowid", "desc"), limit: 2 },
]

describe("a query naming a column the table does not have is refused", () => {
  let lines: Array<string>
  beforeEach(() => {
    lines = []
    vi.spyOn(console, "error").mockImplementation((...args: Array<unknown>) => void lines.push(args.map(String).join(" ")))
  })
  afterEach(() => vi.restoreAllMocks())

  for (const shape of SHAPES) {
    const { name: _n, column, ...query } = shape
    it(`sub, ${shape.name}: reset, nothing registered, logged`, async () => {
      const room = `unk-sub-${crypto.randomUUID()}`
      await seeded(room)
      const ws = await openWs(room)
      expect(await sub(ws, query)).toEqual({ keys: [], end: "reset" })
      expect(lines.some((l) => l.includes(`'${column}'`))).toBe(true)
      // Nothing registered: a later insert sends this sub no delta (deltas for this
      // socket flush before `committed`, ADR-0002 C1), and nothing was persisted.
      const after = await exchange(
        ws,
        { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "m4", cols: { id: "m4", body: "nope" } }] },
        (x) => x.t === "committed",
      )
      expect(after.filter((x) => x.sub === "s1")).toEqual([])
      const persisted = await runInDurableObject(stubFor(room), (_i, s) =>
        Array.from(s.storage.sql.exec("SELECT sub_id FROM _sync_subs")),
      )
      expect(persisted).toEqual([])
      ws.close()
    })

    it(`fetch, ${shape.name}: an empty page, logged`, async () => {
      const room = `unk-fetch-${crypto.randomUUID()}`
      await seeded(room)
      const ws = await openWs(room)
      expect(await fetchPage(ws, query)).toEqual([])
      expect(lines.some((l) => l.includes(`'${column}'`))).toBe(true)
      ws.close()
    })

    it(`readSyncSnapshot, ${shape.name}: throws`, async () => {
      const room = `unk-snap-${crypto.randomUUID()}`
      await seeded(room)
      const { error } = (await readSyncSnapshot(room, { collection: "messages", ...query })) as { error: Error }
      expect(error).toBeInstanceOf(UnsupportedPredicateError)
      expect(error.message).toContain(`'${column}'`)
    })
  }

  it("a reconnecting sub (catch-up path, which runs no SQL) is refused too", async () => {
    const room = `unk-since-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    expect(await sub(ws, { where: fn("eq", ref("nope"), val("nope")), since: "1" })).toEqual({ keys: [], end: "reset" })
    ws.close()
  })

  it("a fetch whose cursor names an unknown column is refused", async () => {
    const room = `unk-cursor-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    const cursor = { whereCurrent: fn("eq", ref("nope"), val("b1")), whereFrom: fn("gt", ref("nope"), val("b1")) }
    expect(await fetchPage(ws, { orderBy: by("body"), limit: 2, cursor })).toEqual([])
    expect(lines.some((l) => l.includes("'nope'"))).toBe(true)
    ws.close()
  })

  it("the log line clips a long column name", async () => {
    const room = `unk-clip-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    const long = "n".repeat(5_000) // under the statement length cap, over the clip
    expect(await sub(ws, { where: fn("eq", ref(long), val("x")) })).toEqual({ keys: [], end: "reset" })
    expect(await fetchPage(ws, { orderBy: by(long) })).toEqual([])
    ws.close()
    expect(lines.length).toBeGreaterThanOrEqual(2)
    expect(lines.filter((l) => l.length > 2000).map((l) => l.length)).toEqual([])
  })
})

describe("columns the table does have still work", () => {
  it("the pk, in a where and an orderBy", async () => {
    const room = `unk-pk-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    const q = { where: fn("gt", ref("id"), val("m1")), orderBy: by("id", "desc"), limit: 1 }
    expect(await sub(ws, q)).toEqual({ keys: ["m3"], end: "snap-end" })
    expect(await fetchPage(ws, q)).toEqual(["m3"])
    expect(await snapshotIds(room, { collection: "messages", ...q })).toEqual(["m3"])
    ws.close()
  })

  it("a column added after the first query, plain or generated, is accepted at once", async () => {
    const room = `unk-added-${crypto.randomUUID()}`
    await seeded(room)
    const ws = await openWs(room)
    // A first query on this object, so any list of columns it kept would now be stale.
    expect(await fetchPage(ws, { where: fn("eq", ref("body"), val("b1")) })).toEqual(["m1"])

    // A migration while the object is alive, with no registerSync after it.
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("ALTER TABLE messages ADD COLUMN extra TEXT")
      s.storage.sql.exec("ALTER TABLE messages ADD COLUMN loud TEXT GENERATED ALWAYS AS (upper(body)) VIRTUAL")
      s.storage.sql.exec("UPDATE messages SET extra = 'e' WHERE id = 'm2'")
    })
    const extra = { where: fn("eq", ref("extra"), val("e")), orderBy: by("extra") }
    expect(await fetchPage(ws, extra)).toEqual(["m2"])
    expect(await sub(ws, extra)).toEqual({ keys: ["m2"], end: "snap-end" })
    expect(await snapshotIds(room, { collection: "messages", ...extra })).toEqual(["m2"])
    expect(await fetchPage(ws, { where: fn("eq", ref("loud"), val("B3")) })).toEqual(["m3"])
    ws.close()
  })
})

// ADR-0019: a sub persisted on a hibernated socket is restored on wake without its
// `sub` frame, so the frame's checks do not run. A restored predicate naming a
// column the table no longer has is handled like one that no longer compiles:
// the socket closes with 1011 and its rows go, so the client reconnects and its
// re-sent `sub` is refused with `reset` above.
describe("restore after a column was dropped", () => {
  it("closes the socket (1011) and drops its persisted subs", async () => {
    const room = `unk-restore-${crypto.randomUUID()}`
    const stub = stubFor(room)
    await seeded(room, "ALTER TABLE messages ADD COLUMN extra TEXT")
    const ws = await openWs(room)
    const closed = new Promise<number>((resolve) => ws.addEventListener("close", (e) => resolve((e as CloseEvent).code)))
    expect(await sub(ws, { where: fn("eq", ref("extra"), val("x")) })).toEqual({ keys: [], end: "snap-end" })

    // The migration drops the column, then the object hibernates and wakes.
    await runInDurableObject(stub, (_i, s) => void s.storage.sql.exec("ALTER TABLE messages DROP COLUMN extra"))
    await evictDurableObject(stub)
    const waker = await openWs(room)

    expect(await closed).toBe(1011)
    const persisted = await runInDurableObject(stub, (_i, s) => Array.from(s.storage.sql.exec("SELECT sub_id FROM _sync_subs")))
    expect(persisted).toEqual([])
    waker.close()
  })
})
