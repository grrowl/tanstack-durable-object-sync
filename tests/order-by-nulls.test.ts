import * as db from "@tanstack/db"
import { BTreeIndex, createCollection, createLiveQueryCollection, localOnlyCollectionOptions } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

const version = "isWhereSubset" in db ? "0.8.x" : "0.9.x"
const room = (tag: string): string => `bbD-${tag}-${crypto.randomUUID()}`
const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 3000
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for snapshot")
    await sleep(10)
  }
}

type Row = { id: string; body: string | null }

async function seed(r: string, rows: Array<Row>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    for (const row of rows) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", row.id, row.body)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}

function transportFor(r: string, boundedSnapshots: Array<Array<Row>>): WebSocketTransport<TestApi> {
  const t = new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 0,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
  const sub = t.subscribe.bind(t)
  t.subscribe = (subId, coll, handler, where, orderBy, limit, since) => {
    if (limit !== K) return sub(subId, coll, handler, where, orderBy, limit, since)
    const snapshot: Array<Row> = []
    return sub(subId, coll, {
      ...handler,
      onSnap(key, row) {
        snapshot.push(row as Row)
        handler.onSnap(key, row)
      },
      onSnapEnd() {
        boundedSnapshots.push([...snapshot])
        handler.onSnapEnd()
      },
      onRestart() {
        snapshot.length = 0
        handler.onRestart?.()
      },
    }, where, orderBy, limit, since)
  }
  return t
}

// NULLs + strings whose locale and lexical orders AGREE ("01".."10"): isolates `nulls`.
const nullRows: Array<Row> = [
  ...Array.from({ length: 10 }, (_, i) => ({ id: `m${i}`, body: String(i + 1).padStart(2, "0") })),
  { id: "n1", body: null },
  { id: "n2", body: null },
]
// Mixed case, no NULLs: isolates collation. Byte order: B D F a c e; locale: a B c D e F.
const caseRows: Array<Row> = ["apple", "Banana", "cherry", "Date", "eel", "Fig"].map((b, i) => ({ id: `c${i}`, body: b }))

type Opts = { direction: "asc" | "desc"; nulls?: "first" | "last"; stringSort?: "lexical" | "locale" }

const cases: Array<{ name: string; rows: Array<Row>; opts: Opts }> = []
for (const stringSort of ["lexical", undefined] as const) {
  for (const direction of ["asc", "desc"] as const) {
    for (const nulls of [undefined, "first", "last"] as const) {
      cases.push({ name: `nulls ${direction} nulls=${nulls ?? "default"} sort=${stringSort ?? "default(locale)"}`, rows: nullRows, opts: { direction, nulls, stringSort } })
    }
  }
}
for (const stringSort of ["locale", undefined, "lexical"] as const) {
  for (const direction of ["asc", "desc"] as const) {
    cases.push({ name: `case ${direction} sort=${stringSort ?? "default(locale)"}`, rows: caseRows, opts: { direction, stringSort } })
  }
}

const K = 3
const bodies = (q: { toArray: Array<{ body: string | null }> }) => q.toArray.map((m) => m.body)
const strip = (o: Opts): Record<string, unknown> => Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined))

describe(`F4 bounded snapshot vs TanStack comparator (@tanstack/db ${version})`, () => {
  for (const indexed of [false, true]) {
    for (const c of cases) {
      // bugbash F4-locale: SQLite BINARY cannot reproduce JS localeCompare.
      // 0.9.x masks the bad bounded SQL result by subsequently fetching the full subset.
      const locale = c.rows === caseRows && c.opts.stringSort !== "lexical"
      let bounded: Array<string | null> | undefined
      let expected: Array<string | null> | undefined
      it(`${c.name} index=${indexed}`, async () => {
        const opts = strip(c.opts) as Opts
        // TanStack's own answer over all rows.
        const local = createCollection(localOnlyCollectionOptions<Row>({ id: `l-${crypto.randomUUID()}`, getKey: (r) => r.id, initialData: c.rows }))
        const want = createLiveQueryCollection((q) => q.from({ m: local }).orderBy(({ m }) => m.body, opts).limit(K))
        await want.preload()

        const r = room("f4")
        await seed(r, c.rows)
        const boundedSnapshots: Array<Array<Row>> = []
        const t = transportFor(r, boundedSnapshots)
        const coll = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m: Row) => m.id, syncMode: "on-demand" }))
        if (indexed) {
          coll.createIndex((m) => m.body, { indexType: BTreeIndex, options: {
            compareOptions: { direction: opts.direction, nulls: opts.nulls ?? "first", stringSort: opts.stringSort ?? "locale" },
          } })
        }
        const got = createLiveQueryCollection((q) => q.from({ m: coll }).orderBy(({ m }) => m.body, opts).limit(K))
        try {
          await got.preload()
          await waitFor(() => got.size === K && boundedSnapshots.length > 0)
          const w = bodies(want)
          expect(w.length).toBe(K)
          bounded = boundedSnapshots[0]!.map((row) => row.body)
          expected = w
          if (!locale) expect(bounded).toEqual(w)
          if (version === "0.9.x") {
            await waitFor(() => JSON.stringify(bodies(got)) === JSON.stringify(w))
          }
          if (!locale || version === "0.9.x") expect(bodies(got)).toEqual(w)
        } finally {
          t.close()
        }
      })
      if (locale) {
        // bugbash F4-locale: the SQL snapshot alone is wrong, even when 0.9.x later recovers.
        it.fails(`${c.name} index=${indexed} bounded locale parity`, () => {
          expect(bounded).toEqual(expected)
        })
      }
    }
  }

  const tiedRows: Array<Row> = ["z", "a", "m", "b"].map((id) => ({ id, body: null }))
  let tiedSnapshot: Array<string> | undefined
  let tiedExpected: Array<string> | undefined
  it("captures a bounded NULL tie snapshot from the real DO", async () => {
    const local = createCollection(localOnlyCollectionOptions<Row>({
      id: `ties-${crypto.randomUUID()}`, getKey: (row) => row.id, initialData: tiedRows,
    }))
    const want = createLiveQueryCollection((query) => query.from({ row: local }).orderBy(({ row }) => row.body, {
      direction: "desc", nulls: "first", stringSort: "lexical",
    }).limit(K))
    await want.preload()
    tiedExpected = want.toArray.map((row) => row.id)

    const r = room("ties")
    await seed(r, tiedRows)
    const snapshots: Array<Array<Row>> = []
    const transport = transportFor(r, snapshots)
    try {
      await transport.subscribe("ties", "messages", {
        onSnap() {}, onSnapEnd() {}, onDelta() {}, onUptodate() {}, onReset() {},
      }, undefined, [{ expression: { type: "ref", path: ["body"] }, compareOptions: {
        direction: "desc", nulls: "first", stringSort: "lexical",
      } }], K)
      await waitFor(() => snapshots.length > 0)
      tiedSnapshot = snapshots[0]!.map((row) => row.id)
      expect(tiedExpected).toHaveLength(K)
      expect(tiedSnapshot).toHaveLength(K)
    } finally {
      transport.close()
    }
  })

  // bugbash F4-ties: SQLite's bounded top-k does not use TanStack's row-key tie-break.
  it.fails("selects the same three NULL rows as TanStack when ties cross the limit", () => {
    expect(tiedSnapshot).toEqual(tiedExpected)
  })

  // 0.9.x scrolls an indexed, cursor-expressible window with a cursor fetch.
  // For nulls: "last" its `whereFrom` is `gt(v) OR isNull OR isUndefined`, and
  // isNull is outside the server's predicate floor (ADR-0013): the fetch is
  // refused as an empty page and the window never grows. nulls: "first" is the
  // control: the same path, with a cursor the floor can compile. 0.8.x never
  // scrolls on setWindow (see on-demand-contracts), so there is nothing to pin.
  // The scroll and its cursor fetch are asserted in a plain `it`, so the
  // expected failure below can only be the window's contents.
  for (const nulls of ["first", "last"] as const) {
    const run = version === "0.8.x" ? it.skip : it
    let grown: Array<string | null> | undefined
    let expected: Array<string | null> | undefined
    run(`scrolls an indexed asc nulls=${nulls} window with a cursor fetch`, async () => {
      const opts = { direction: "asc", nulls, stringSort: "lexical" } as const
      const all = nullRows.length
      const local = createCollection(localOnlyCollectionOptions<Row>({ id: `s-${crypto.randomUUID()}`, getKey: (row) => row.id, initialData: nullRows }))
      const want = createLiveQueryCollection((q) => q.from({ m: local }).orderBy(({ m }) => m.body, opts).limit(all))
      await want.preload()

      const r = room("scroll")
      await seed(r, nullRows)
      const t = transportFor(r, [])
      let cursorPages = 0
      const fetch = t.fetch.bind(t)
      t.fetch = async (frame) => {
        const page = await fetch(frame)
        if (frame.cursor) cursorPages++
        return page
      }
      const coll = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m: Row) => m.id, syncMode: "on-demand" }))
      coll.createIndex((m) => m.body, { indexType: BTreeIndex, options: { compareOptions: opts } })
      const got = createLiveQueryCollection((q) => q.from({ m: coll }).orderBy(({ m }) => m.body, opts).limit(K))
      try {
        await got.preload()
        await waitFor(() => got.size === K)
        const scrolled = (got as unknown as { utils: { setWindow(w: { offset: number; limit: number }): true | Promise<void> } }).utils.setWindow({ offset: 0, limit: all })
        expect(scrolled).not.toBe(true)
        await scrolled
        expect(cursorPages).toBeGreaterThan(0)
        grown = bodies(got)
        expected = bodies(want)
        expect(expected).toHaveLength(all)
      } finally {
        t.close()
      }
    })
    // bugbash F4-cursor: nulls: "last" only.
    const parity = version === "0.8.x" ? it.skip : nulls === "last" ? it.fails : it
    parity(`the scrolled asc nulls=${nulls} window matches TanStack's order`, () => {
      expect(grown).toEqual(expected)
    })
  }
})
