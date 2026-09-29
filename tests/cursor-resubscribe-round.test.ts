import { createCollection, createLiveQueryCollection, gte, lt } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: after a reconnect, every bootstrapped sub resumes from the ONE shared
// cursor, and the DO answers each with its own catch-up ending in a sub-scoped
// `uptodate` at the CURRENT seq (which covers every table). Any cursor advance
// in the middle of that resubscribe round claims positions that the subs still
// waiting for their catch-up never applied. If the socket drops right there,
// the next round resumes those subs past their offline changes, and the loss is
// permanent and silent (bugbash F1; ADR-0002's single cursor, ADR-0016's
// resubscribe, ADR-0023 D7 covers only subs that never bootstrapped).
//
// The suite is the target for the fix, not a design for it: one transport
// carries four subs (eager `messages`, eager `files`, and two on-demand watches
// on `messages` with disjoint predicates). While the socket is down the DO
// applies a change set that only some subs see. The reconnect round is then cut
// at one boundary (before any frame, mid a sub's catch-up, after a sub's
// terminal, after a live broadcast boundary), and after one clean reconnect
// every collection must equal the DO's rows. Scenarios the single cursor gets
// wrong today are `it.fails` (bugbash F1). A correct fix turns them all green;
// then drop the `isRed` switch and run every scenario as a plain `it`.

const codec = createFrameCodec()
const room = (): string => `crr-${crypto.randomUUID()}`
const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))
async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}
/** A server-originated synced write: capture, then drain + broadcast (ADR-0006). */
async function serverExec(r: string, statements: Array<string>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    for (const sql of statements) s.storage.sql.exec(sql)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}

// --- A socket we can cut at a chosen frame ---------------------------------

/** What one opened socket did: the subs it sent (in order, which is the order
 *  the DO serves their catch-ups) and the frames it delivered. */
interface Sock {
  subs: Array<{ subId: string; collection: string }>
  frames: Array<ServerFrame>
  dropped: boolean
}
/** Where to cut a socket. `at` sees each inbound frame before delivery: "after"
 *  delivers it and then drops, "before" drops without delivering it. A drop
 *  loses a SUFFIX of what the client sent (the DO only ever processes a
 *  prefix), so `muteSubsFrom` loses every outbound frame from that `sub` on. */
interface Cut {
  at: (f: ServerFrame, s: Sock) => "before" | "after" | false
  muteSubsFrom?: number
}

function wrap(ws: WebSocket, s: Sock, cut: Cut | undefined): WebSocketLike {
  const onMessage: Array<(ev: { data?: unknown }) => void> = []
  const onClose: Array<(ev: { data?: unknown }) => void> = []
  let muted = false
  const drop = (): void => {
    if (s.dropped) return
    s.dropped = true
    try {
      ws.close(1000, "drop")
    } catch {
      /* already closed */
    }
    for (const l of onClose) l({ code: 1006 } as { data?: unknown })
  }
  ws.addEventListener("message", (e) => {
    if (s.dropped) return
    const data = (e as MessageEvent).data
    const f = codec.decode(data as ArrayBuffer) as ServerFrame
    const where = cut?.at(f, s) ?? false
    if (where === "before") return drop()
    s.frames.push(f)
    for (const l of onMessage) l({ data })
    if (where === "after") drop()
  })
  ws.addEventListener("close", () => drop())
  return {
    send: (d) => {
      if (s.dropped || muted) return
      const f = codec.decode(d as ArrayBuffer) as ClientFrame
      if (f.t === "sub") {
        s.subs.push({ subId: f.subId, collection: f.collection })
        if (cut?.muteSubsFrom !== undefined && s.subs.length > cut.muteSubsFrom) {
          muted = true
          return
        }
      }
      ws.send(d as ArrayBuffer)
    },
    close: () => drop(),
    addEventListener: (type, l) => {
      if (type === "message") onMessage.push(l)
      else if (type === "close") onClose.push(l)
    },
    removeEventListener: () => {},
  }
}

// --- Topology, change sets, boundaries -------------------------------------

/** The resubscribe round, in the order the transport re-sends the subs. */
const ROUND = ["eager messages", "eager files", "on-demand a*", "on-demand c*"] as const
const TABLE_OF = ["messages", "files", "messages", "messages"] as const
/** The two on-demand watches: `lt(body, "b")` (a*) and `gte(body, "c")` (c*); b* is in neither. */
const inA = (body: string): boolean => body < "b"
const inC = (body: string): boolean => body >= "c"

const SEED = [
  "INSERT INTO messages(id,body) VALUES('m1','a1'),('m2','a2'),('m3','b3'),('m4','b4'),('m5','c5'),('m6','c6')",
  "INSERT INTO files(id,name) VALUES('f1','one'),('f2','two'),('f3','three')",
]

interface ChangeSet {
  sql: Array<string>
  /** ROUND indexes whose rows the change set changes (the subs that must see it). */
  affects: Array<number>
}
const CHANGE_SETS: Record<string, ChangeSet> = {
  // Every sub sees something: insert, update, delete, and moves in and out of
  // both on-demand subsets (a→c, b→a, c→b); files insert/update/delete.
  mixed: {
    sql: [
      "INSERT INTO messages(id,body) VALUES('m7','a7')",
      "UPDATE messages SET body='a1x' WHERE id='m1'",
      "DELETE FROM messages WHERE id='m5'",
      "UPDATE messages SET body='c2' WHERE id='m2'",
      "UPDATE messages SET body='a3' WHERE id='m3'",
      "UPDATE messages SET body='b6' WHERE id='m6'",
      "INSERT INTO files(id,name) VALUES('f4','four')",
      "UPDATE files SET name='one!' WHERE id='f1'",
      "DELETE FROM files WHERE id='f2'",
    ],
    affects: [0, 1, 2, 3],
  },
  // Only the second sub sees anything; the others' catch-ups are empty.
  "files only": {
    sql: [
      "INSERT INTO files(id,name) VALUES('f4','four')",
      "UPDATE files SET name='one!' WHERE id='f1'",
      "DELETE FROM files WHERE id='f2'",
    ],
    affects: [1],
  },
  // Only moves between the on-demand subsets (plus the eager mirror of them).
  "on-demand moves": {
    sql: [
      "UPDATE messages SET body='c2' WHERE id='m2'", // out of a*, into c*
      "UPDATE messages SET body='b6' WHERE id='m6'", // out of c*
      "UPDATE messages SET body='a3' WHERE id='m3'", // into a*
    ],
    affects: [0, 2, 3],
  },
}
/** A live write that lands mid-round (only the first sub is registered by then). */
const LIVE_WRITE = ["INSERT INTO messages(id,body) VALUES('m8','a8')"]

type Boundary =
  | { kind: "none" }
  | { kind: "before-any" }
  | { kind: "mid"; i: number }
  | { kind: "after"; i: number }
  | { kind: "broadcast" }

const own = (f: ServerFrame, subId: string | undefined): boolean =>
  subId !== undefined && (f.t === "uptodate" || f.t === "snap-end") && f.sub === subId

function cutFor(b: Boundary): Cut | undefined {
  switch (b.kind) {
    case "none":
      return undefined
    case "before-any":
      return { at: () => "before" }
    case "mid":
      // after the FIRST delta of sub i's catch-up (each applicable one has >1)
      return { at: (f, s) => (f.t === "d" && f.sub === s.subs[b.i]?.subId ? "after" : false) }
    case "after":
      return { at: (f, s) => (own(f, s.subs[b.i]?.subId) ? "after" : false) }
    case "broadcast":
      // Sub 0's `sub` reached the DO, the rest were still in flight when the
      // socket died. A live write then reaches sub 0 as a coalescer tick
      // (`d` + broadcast `uptodate`), and the socket drops after that boundary.
      return { at: (f) => (f.t === "uptodate" && f.sub === undefined ? "after" : false), muteSubsFrom: 1 }
  }
}

/** Does the single cursor lose changes here today? A cut loses a sub's
 *  catch-up only once something earlier in the round advanced the cursor. */
function isRed(b: Boundary, set: ChangeSet): boolean {
  switch (b.kind) {
    case "none":
    case "before-any":
      return false
    case "mid":
      return b.i > 0 && set.affects.some((j) => j >= b.i)
    case "after":
      return set.affects.some((j) => j > b.i)
    case "broadcast":
      return set.affects.some((j) => j >= 1)
  }
}

function label(b: Boundary): string {
  switch (b.kind) {
    case "none":
      return "no mid-round drop (control)"
    case "before-any":
      return "drop before any catch-up frame"
    case "mid":
      return `drop mid ${ROUND[b.i]}'s catch-up`
    case "after":
      return `drop after ${ROUND[b.i]}'s catch-up terminal`
    case "broadcast":
      return `drop after a live broadcast boundary, mid-round`
  }
}

// --- Scenario ---------------------------------------------------------------

type Row = { id: string; v: string }
const sorted = (rows: Array<Row>): Array<Row> => [...rows].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))

/** Red scenarios that got as far as their convergence check. A red scenario
 *  that fails EARLIER (a broken harness, a boundary that never fired) would
 *  otherwise be indistinguishable from the bug under `it.fails`. */
const reached = new Set<string>()
const redNames: Array<string> = []

async function scenario(name: string, setName: string, b: Boundary): Promise<void> {
  const set = CHANGE_SETS[setName]!
  const r = room()
  const sockets: Array<Sock> = []
  const cuts = new Map<number, Cut>()
  let gate: Promise<void> = Promise.resolve()
  let release: () => void = () => {}
  const t = new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 5,
    open: async () => {
      await gate
      const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket!
      ws.accept()
      const s: Sock = { subs: [], frames: [], dropped: false }
      const cut = cuts.get(sockets.length)
      sockets.push(s)
      return wrap(ws, s, cut)
    },
  })
  try {
    await serverExec(r, SEED)
    const em = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    const ef = createCollection(doCollectionOptions({ transport: t, table: "files", getKey: (f) => f.id }))
    const od = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    const qa = createLiveQueryCollection((q) => q.from({ m: od }).where(({ m }) => lt(m.body, "b")))
    const qc = createLiveQueryCollection((q) => q.from({ m: od }).where(({ m }) => gte(m.body, "c")))
    // Sequential, so the transport's handler order (the round's order) is ROUND.
    await em.preload()
    await ef.preload()
    await qa.preload()
    await qc.preload()

    const client = (): Record<string, Array<Row>> => ({
      [ROUND[0]]: sorted(em.toArray.map((m) => ({ id: m.id, v: m.body }))),
      [ROUND[1]]: sorted(ef.toArray.map((f) => ({ id: f.id, v: f.name }))),
      [ROUND[2]]: sorted(qa.toArray.map((m) => ({ id: m.id, v: m.body }))),
      [ROUND[3]]: sorted(qc.toArray.map((m) => ({ id: m.id, v: m.body }))),
      "on-demand held rows": sorted(od.toArray.map((m) => ({ id: m.id, v: m.body }))),
    })
    const server = async (): Promise<Record<string, Array<Row>>> => {
      const [msgs, files] = await runInDurableObject(stubFor(r), (_i, s) => [
        Array.from(s.storage.sql.exec<{ id: string; body: string }>("SELECT id, body FROM messages")).map((x) => ({
          id: x.id,
          v: x.body,
        })),
        Array.from(s.storage.sql.exec<{ id: string; name: string }>("SELECT id, name FROM files")).map((x) => ({
          id: x.id,
          v: x.name,
        })),
      ])
      return {
        [ROUND[0]]: sorted(msgs),
        [ROUND[1]]: sorted(files),
        [ROUND[2]]: sorted(msgs.filter((m) => inA(m.v))),
        [ROUND[3]]: sorted(msgs.filter((m) => inC(m.v))),
        "on-demand held rows": sorted(msgs.filter((m) => inA(m.v) || inC(m.v))),
      }
    }
    const converged = async (): Promise<boolean> => JSON.stringify(client()) === JSON.stringify(await server())
    await waitFor(() => sockets[0]!.subs.length === 4)
    expect(sockets[0]!.subs.map((s) => s.collection)).toEqual([...TABLE_OF])
    expect(client()).toEqual(await server()) // bootstrapped, converged
    const c0 = BigInt(t.appliedCursor)
    expect(c0).toBeGreaterThan(0n)

    // Drop; hold the reconnect until the offline changes are in.
    gate = new Promise<void>((res) => (release = res))
    await runInDurableObject(stubFor(r), (_i, s) => {
      for (const w of s.getWebSockets()) w.close(1000, "drop")
    })
    await waitFor(() => sockets[0]!.dropped)
    await serverExec(r, set.sql)
    const cut = cutFor(b)
    if (cut) cuts.set(1, cut)
    release()

    // The round on socket 1, cut at the boundary (or not).
    await waitFor(() => sockets.length >= 2)
    const round = sockets[1]!
    if (b.kind === "broadcast") {
      await waitFor(() => round.frames.some((f) => own(f, round.subs[0]?.subId)))
      await serverExec(r, LIVE_WRITE)
    }
    if (b.kind === "none") {
      await waitFor(() => round.subs.length === 4 && round.subs.every((s) => round.frames.some((f) => own(f, s.subId))))
    } else {
      await waitFor(() => round.dropped)
    }
    // The cut landed where the scenario says: a boundary that advanced the
    // cursor did so, and one before any terminal did not.
    const cursorAtCut = BigInt(t.appliedCursor)
    if (b.kind === "before-any" || (b.kind === "mid" && b.i === 0)) expect(cursorAtCut).toBe(c0)
    else expect(cursorAtCut).toBeGreaterThan(c0)

    // One clean reconnect: wait for every sub's own terminal on it.
    if (b.kind !== "none") await waitFor(() => sockets.length >= 3)
    const final = sockets[b.kind === "none" ? 1 : 2]!
    await waitFor(() => final.subs.length === 4 && final.subs.every((s) => final.frames.some((f) => own(f, s.subId))))
    // Convergence is eventual (live queries recompute after the commit); bound it.
    const deadline = Date.now() + 250
    while (!(await converged()) && Date.now() < deadline) await new Promise((res) => setTimeout(res, 10))
    expect(final.dropped).toBe(false)
    expect(sockets.length).toBe(b.kind === "none" ? 2 : 3)

    reached.add(name)
    expect(client()).toEqual(await server())
  } finally {
    t.close()
  }
}

describe("single cursor across a resubscribe round cut by a drop (bugbash F1)", () => {
  for (const [setName, set] of Object.entries(CHANGE_SETS)) {
    const boundaries: Array<Boundary> = [
      { kind: "none" },
      { kind: "before-any" },
      ...ROUND.map((_, i) => ({ kind: "after" as const, i })),
      // A mid-catch-up cut needs a catch-up with more than one delta: only
      // subs on a table the change set touches have one.
      ...ROUND.map((_, i) => ({ kind: "mid" as const, i })).filter((m) =>
        set.sql.some((s) => s.includes(` ${TABLE_OF[m.i]}`)),
      ),
      { kind: "broadcast" },
    ]
    describe(`offline change set: ${setName}`, () => {
      for (const b of boundaries) {
        const name = `${setName}: ${label(b)}`
        if (isRed(b, set)) {
          redNames.push(name)
          // bugbash F1: a cursor advance mid-round skips the unfinished subs' catch-up.
          it.fails(`${label(b)}: every collection converges to the DO`, () => scenario(name, setName, b))
        } else {
          it(`${label(b)}: every collection converges to the DO`, () => scenario(name, setName, b))
        }
      }
    })
  }

  it("harness: every red scenario ran to its convergence check", () => {
    // A red scenario that threw before its convergence check passed `it.fails`
    // for the wrong reason; this is what catches it.
    expect(redNames.filter((n) => !reached.has(n))).toEqual([])
  })
})
