import { and, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { settled } from "./settled.ts"
import { LONG_COMMAND, LONG_TABLE, type longSchema } from "./test-worker.ts"

// WHY (ADR-0012 amendment 2026-11-17, id lengths): the server refuses an id over
// 256 UTF-8 bytes. A schema may register a table or command name of at most 128
// bytes, so every id the library's client builds (a table name plus `#`,
// `#fetch#` or `#hydrate#` and a counter, or a UUID) stays under the limit. This
// drives the real client at the bound: sub, fetch, mutation and command all
// work. And a sub refused for its id gets its `reset` on the watch keyed by that
// id, which settles instead of hanging.

interface Msg {
  id: string
  body: string
}

type Api = typeof longSchema
const utf8Bytes = (s: string): number => new TextEncoder().encode(s).byteLength

function transport(room: string): WebSocketTransport<Api> {
  return new WebSocketTransport<Api>({
    url: `https://example.com/long/${room}`,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/long/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
}

/** Records the id of every sub and fetch the client sends. */
function spyIds(t: WebSocketTransport<Api>): { subs: Array<string>; fetches: Array<string> } {
  const ids = { subs: [] as Array<string>, fetches: [] as Array<string> }
  const subscribe = t.subscribe.bind(t)
  const fetch = t.fetch.bind(t)
  t.subscribe = (subId, ...rest) => (ids.subs.push(subId), subscribe(subId, ...rest))
  t.fetch = (frame) => (ids.fetches.push(frame.fetchId), fetch(frame))
  return ids
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("the library's client at the name bound (ADR-0012 amendment 2026-11-17)", () => {
  it("a 128 byte table and command work end to end: sub, fetch, mutation and command ids all fit", async () => {
    const t = transport(`at-bound-${crypto.randomUUID()}`)
    const ids = spyIds(t)
    const opts = (syncMode: "eager" | "on-demand") =>
      doCollectionOptions({ transport: t, table: LONG_TABLE, getKey: (m: Msg) => m.id, syncMode } as never)

    const eager = createCollection(opts("eager")) as unknown as {
      preload(): Promise<void>
      insert(m: Msg): never
      get(k: string): Msg | undefined
    }
    await eager.preload()
    await settled(eager.insert({ id: "a", body: "keep" })) // a mutation: its txId is a UUID
    expect(eager.get("a")).toMatchObject({ id: "a", body: "keep" })

    // On demand: a watch for body = keep, then a query it covers, answered by a fetch.
    const onDemand = createCollection(opts("on-demand")) as never
    const kept = createLiveQueryCollection((q) =>
      q.from({ m: onDemand }).where(({ m }: { m: Msg }) => eq(m.body, "keep")),
    )
    await kept.preload()
    await waitFor(() => kept.get("a") !== undefined)
    const one = createLiveQueryCollection((q) =>
      q.from({ m: onDemand }).where(({ m }: { m: Msg }) => and(eq(m.body, "keep"), eq(m.id, "a"))),
    )
    await one.preload()
    await waitFor(() => one.get("a") !== undefined)

    expect(await t.sendCall(LONG_COMMAND as never, null as never)).toEqual({ ok: true })

    expect(ids.subs.length).toBeGreaterThanOrEqual(2)
    expect(ids.fetches.length).toBeGreaterThanOrEqual(1)
    for (const id of [...ids.subs, ...ids.fetches]) expect(utf8Bytes(id)).toBeLessThanOrEqual(256)
    t.close()
  })

  it("a sub refused for its id: the reset reaches the watch with that id, which settles empty", async () => {
    const t = transport(`refused-${crypto.randomUUID()}`)
    const ids = spyIds(t)
    const eager = createCollection(
      doCollectionOptions({ transport: t, table: LONG_TABLE, getKey: (m: Msg) => m.id } as never),
    ) as unknown as { preload(): Promise<void>; insert(m: Msg): never; get(k: string): Msg | undefined }
    await eager.preload()

    // A table name only this client knows, long enough that its watch id
    // (`<table>#<n>`) is over 256 bytes. No schema could register it.
    const farTable = "z".repeat(255)
    const far = createCollection(
      doCollectionOptions({ transport: t, table: farTable, getKey: (m: Msg) => m.id, syncMode: "on-demand" } as never),
    ) as never
    const q = createLiveQueryCollection((qb) => qb.from({ m: far }).where(({ m }: { m: Msg }) => eq(m.body, "keep")))
    await q.preload() // settles: the watch took the reset as its refusal
    expect(q.size).toBe(0)
    const farId = ids.subs.find((id) => id.startsWith(farTable))!
    expect(utf8Bytes(farId)).toBeGreaterThan(256)

    // The other collection on the same socket is untouched.
    await settled(eager.insert({ id: "b", body: "x" }))
    expect(eager.get("b")).toMatchObject({ id: "b", body: "x" })
    t.close()
  })
})
