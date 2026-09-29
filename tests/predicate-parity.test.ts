// WHY: a filtered subscription's membership must not depend on which evaluator
// decided it — the SQL snapshot path and the JS delta path must agree on every
// row, or two clients diverge by connection timing. Specifically:
//   - The snapshot path filters rows in SQLite via sql-compiler.ts (lowered SQL).
//   - The delta path filters rows in JS via @tanstack/db's compileSingleRowExpression
//     + toBooleanPredicate in subscriptions.ts.
// SQL and JS disagree at the edges (NULL three-valued logic, LIKE case-folding,
// IN with NULLs), so the operator floor is defined as exactly the set on which the
// two agree row-for-row, and the DO forces SQLite LIKE case-sensitive to match
// @tanstack/db's case-sensitive `like` (ADR-0013). These cases pin that agreement
// and fail loudly if it ever regresses.
//
// History: plan 003 first landed these as `it.fails` characterizations of two real
// divergences — `ne` (which @tanstack/db cannot compile, crashing the delta path
// and hanging the client) and `like` (SQLite case-insensitive vs JS case-sensitive).
// ADR-0013 resolved both: `ne` dropped from the floor (→ reset), LIKE made
// case-sensitive. The cases below now assert the fixed behavior.

import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 3000): Promise<Array<ServerFrame>> {
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

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))

// IR helpers — plain JSON matching the wire shape (type/name/args).
const ref = (col: string): unknown => ({ type: "ref", path: [col] })
const val = (value: unknown): unknown => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })

// Seed rows:
//   n1 — body IS NULL  (tests NULL semantics in every case)
//   lo — body = "hello" (lowercase)
//   up — body = "HELLO" (uppercase — tests LIKE case-sensitivity)
//   x  — body = "x"    (value used in eq/in tests)
type Seed = { id: string; body: string | null }
const SEEDS: Array<Seed> = [
  { id: "n1", body: null },
  { id: "lo", body: "hello" },
  { id: "up", body: "HELLO" },
  { id: "x", body: "x" },
]

/**
 * Snapshot membership for a `where` IR: insert seed rows server-side via
 * runInDurableObject (raw SQL — so body can be NULL), then sub from a fresh
 * socket and collect keys from `snap` frames until `snap-end`. Returns the
 * sorted set of keys the SQL snapshot matched.
 */
async function snapshotMembers(room: string, where: unknown): Promise<Array<string>> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
  await runInDurableObject(stub, (_i, s) => {
    for (const seed of SEEDS) {
      if (seed.body === null) s.storage.sql.exec("INSERT INTO messages(id) VALUES (?)", seed.id)
      else s.storage.sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", seed.id, seed.body)
    }
  })
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  const frames = await collectUntil(ws, (f) => f.t === "snap-end")
  ws.close()
  return frames
    .filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap")
    .map((f) => f.key as string)
    .sort()
}

/**
 * Delta membership for a `where` IR: sub first (empty snapshot), then insert seed
 * rows via `mut` frames so they drain synchronously. Each inserted key arrives as
 * a `d`/insert (member) or `d`/delete (non-member, the always-emit rule). Returns
 * the sorted set of keys that arrived as non-delete deltas.
 *
 * The NULL row is sent with `body` omitted (undefined → NULL in SQLite, the normal
 * client path); `explicitNull` instead sends `body: null` to prove both spellings
 * produce identical membership (no codec/binding divergence).
 */
async function deltaMembers(room: string, where: unknown, explicitNull = false): Promise<Array<string>> {
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  await collectUntil(ws, (f) => f.t === "snap-end")

  const members = new Set<string>()
  let txCounter = 0
  for (const seed of SEEDS) {
    const txId = `tx-${++txCounter}`
    const cols: Record<string, unknown> =
      seed.body === null ? (explicitNull ? { id: seed.id, body: null } : { id: seed.id }) : { id: seed.id, body: seed.body }
    send(ws, { t: "mut", txId, collection: "messages", ops: [{ type: "insert", key: seed.id, cols }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed" && f.txId === txId)
    const delta = frames.find((f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && (f.key as string) === seed.id)
    if (delta && delta.op !== "delete") members.add(seed.id)
  }
  ws.close()
  return Array.from(members).sort()
}

/** Subscribe and return the type of the first terminal frame: "reset" or "snap-end". */
async function subTerminal(room: string, where: unknown): Promise<string> {
  const ws = await openWs(room)
  send(ws, { t: "sub", subId: "s1", collection: "messages", where })
  const frames = await collectUntil(ws, (f) => f.t === "reset" || f.t === "snap-end")
  ws.close()
  return frames[frames.length - 1]!.t
}

describe("SQL/JS predicate parity (ADR-0013)", () => {
  // ne: an off-floor operator @tanstack/db cannot compile. The SQL floor now
  // rejects it too (it was removed from COMPARATORS), so BOTH paths answer with a
  // `reset` — fail loud, never the old uncaught-throw hang. The supported
  // not-equal is not(eq(...)), covered below and in subscriptions.test.ts.
  it("ne: rejected with reset, never a hang (the old crash, fixed)", async () => {
    const where = fn("ne", ref("body"), val("x"))
    expect(await subTerminal("pp-ne-snap", where), "ne sub must be rejected with reset").toBe("reset")
  })

  // eq: exact match, no NULL edge. SQL `=` and JS `eq` both case-sensitive → only "lo".
  it("eq: exact match agrees across paths", async () => {
    const where = fn("eq", ref("body"), val("hello"))
    const snap = await snapshotMembers("pp-eq-snap", where)
    const delta = await deltaMembers("pp-eq-delta", where)
    expect(snap, `eq mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(snap).toEqual(["lo"])
  })

  // like: the headline fix. With PRAGMA case_sensitive_like=ON, SQLite LIKE matches
  // @tanstack/db's case-sensitive `like`: "HELLO" does NOT match "hello%" on either
  // path. Both → ["lo"]. (Before ADR-0013: snap=["lo","up"], delta=["lo"] — divergent.)
  it("like: case-sensitive on both paths (HELLO excluded)", async () => {
    const where = fn("like", ref("body"), val("hello%"))
    const snap = await snapshotMembers("pp-like-snap", where)
    const delta = await deltaMembers("pp-like-delta", where)
    expect(snap, `like mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(snap).toEqual(["lo"])
  })

  // gt: two edges, both agreeing across paths. (1) NULL: SQL NULL > 'a' is NULL →
  // n1 excluded; JS toBooleanPredicate(null) → false → excluded. (2) Case ordering:
  // both SQLite BINARY collation and JS string comparison are byte/code-unit based,
  // so "HELLO" (H=0x48) < "a" (0x61) → up excluded, while "hello"/"x" > "a" → in.
  // Members are ["lo","x"] on BOTH paths — another case-sensitivity agreement.
  it("gt: NULL excluded (three-valued) and uppercase sorts below 'a' on both paths", async () => {
    const where = fn("gt", ref("body"), val("a"))
    const snap = await snapshotMembers("pp-gt-snap", where)
    const delta = await deltaMembers("pp-gt-delta", where)
    const deltaExplicit = await deltaMembers("pp-gt-delta-explicit", where, true)
    expect(snap, `gt mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `gt null-spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(deltaExplicit)
    expect(snap).toEqual(["lo", "x"])
  })

  // not(eq): the supported not-equal. SQL: NOT (NULL = 'x') → NULL → n1 excluded. JS: same.
  it("not(eq): NULL body excluded under three-valued NOT", async () => {
    const where = fn("not", fn("eq", ref("body"), val("x")))
    const snap = await snapshotMembers("pp-not-snap", where)
    const delta = await deltaMembers("pp-not-delta", where)
    const deltaExplicit = await deltaMembers("pp-not-delta-explicit", where, true)
    expect(snap, `not(eq) mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `not(eq) null-spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(deltaExplicit)
    expect(snap).toEqual(["lo", "up"])
  })

  // isNull (bugbash F4-cursor): a nulls-last cursor's whereFrom is
  // `gt(v) OR isNull(col) OR isUndefined(col)`. SQL IS NULL and JS `=== null` agree
  // because the delta path evaluates rows hydrated from SQLite, where a NULL column
  // is `null` whether the write omitted it or sent it explicitly.
  it("isNull: only the NULL row matches on both paths", async () => {
    const where = fn("isNull", ref("body"))
    const snap = await snapshotMembers("pp-isnull-snap", where)
    const delta = await deltaMembers("pp-isnull-delta", where)
    const deltaExplicit = await deltaMembers("pp-isnull-delta-explicit", where, true)
    expect(snap, `isNull mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `isNull null-spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(deltaExplicit)
    expect(snap).toEqual(["n1"])
  })

  // isUndefined: SQLite has no undefined and hydration yields null, so it matches
  // nothing on either path — including the NULL row.
  it("isUndefined: matches no stored row on either path, NULL included", async () => {
    const where = fn("isUndefined", ref("body"))
    const snap = await snapshotMembers("pp-isundef-snap", where)
    const delta = await deltaMembers("pp-isundef-delta", where)
    const deltaExplicit = await deltaMembers("pp-isundef-delta-explicit", where, true)
    expect(snap, `isUndefined mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `isUndefined null-spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(deltaExplicit)
    expect(snap).toEqual([])
  })

  // The nulls-last cursor shape TanStack 0.9 emits, composed as a whole.
  it("gt OR isNull OR isUndefined: the cursor whereFrom agrees across paths", async () => {
    const where = fn("or", fn("gt", ref("body"), val("a")), fn("isNull", ref("body")), fn("isUndefined", ref("body")))
    const snap = await snapshotMembers("pp-cursor-snap", where)
    const delta = await deltaMembers("pp-cursor-delta", where)
    expect(snap, `cursor mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(snap).toEqual(["lo", "n1", "x"])
  })

  // in: SQL NULL IN (...) → NULL → n1 excluded; JS .includes(null) → false. "HELLO" not in list.
  it("in: NULL body excluded, exact match only (no case folding)", async () => {
    const where = fn("in", ref("body"), val(["hello", "x"]))
    const snap = await snapshotMembers("pp-in-snap", where)
    const delta = await deltaMembers("pp-in-delta", where)
    const deltaExplicit = await deltaMembers("pp-in-delta-explicit", where, true)
    expect(snap, `in mismatch — snap=${snap} delta=${delta}`).toEqual(delta)
    expect(delta, `in null-spelling mismatch — omit=${delta} explicit=${deltaExplicit}`).toEqual(deltaExplicit)
    expect(snap).toEqual(["lo", "x"])
  })
})
