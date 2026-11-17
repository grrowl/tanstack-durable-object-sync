import { CollectionImpl, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import { settled } from "./settled.ts"
import type { TestApi } from "./test-worker.ts"

// WHY (ADR-0028): a collection can be started over while it holds rows or is
// still loading. @tanstack/db 1.0 added `collection.reload()` for it: a new
// sync run that KEEPS the rows the previous run wrote. Core never deletes a
// kept row on its own, so a row the server deleted in between is the adapter's
// to remove. An app reloads exactly when something looked wrong, typically
// around a dropped connection, which is also when a delete is most likely to
// have been missed: the delete here is broadcast while the client has no
// socket. Without a reload the transport's reconnect catch-up delivers it. A
// reload replaces the subscriptions with fresh ones, which start from a
// snapshot and are never told about it.

// `reload()` does not exist before 1.0. There the same restart is `cleanup()`
// then `preload()`, which drops every row first.
const hasReload = "reload" in CollectionImpl.prototype

const room = (tag: string): string => `reload-${tag}-${crypto.randomUUID()}`
const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await sleep(5)
  }
}
async function serverExec(r: string, sql: string): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    s.storage.sql.exec(sql)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
const dropSocket = (r: string) =>
  runInDurableObject(stubFor(r), (_i, state) => {
    for (const sock of state.getWebSockets()) sock.close(1000, "drop")
  })

interface Link {
  /** The dial fails while true. */
  down: boolean
  attempts: number
  /** Hold inbound frames (in order) while true; `only` narrows it to these types. */
  hold: boolean
  only?: ReadonlyArray<string>
  held: Array<{ t: string; deliver: () => void }>
}
const link = (over: Partial<Link> = {}): Link => ({ down: false, attempts: 0, hold: false, held: [], ...over })
const codec = createFrameCodec()
/** A real transport to the DO, with a dial that can fail and inbound frames that can be held. */
function transportFor(r: string, l: Link): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 5,
    open: async () => {
      l.attempts++
      if (l.down) throw new Error("dial failed: server unreachable")
      const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return {
        send: (d) => ws.send(d as never),
        close: (c, reason) => ws.close(c, reason),
        addEventListener: (type, fn) => {
          if (type !== "message") return ws.addEventListener(type as never, fn as never)
          ws.addEventListener("message", (ev) => {
            const t = codec.decode(ev.data as ArrayBuffer).t
            if (l.hold && (!l.only || l.only.includes(t))) l.held.push({ t, deliver: () => fn(ev as never) })
            else fn(ev as never)
          })
        },
        removeEventListener: () => {},
      } satisfies WebSocketLike
    },
  })
}
/** Drop the socket while the server is unreachable, and wait until the client has noticed. */
async function disconnect(r: string, l: Link): Promise<void> {
  l.down = true
  const dials = l.attempts
  await dropSocket(r)
  await waitFor(() => l.attempts > dials)
}
const reload = (c: object): Promise<void> => (c as { reload: () => Promise<void> }).reload()
const keysOf = (c: { keys: () => Iterable<unknown> }): Array<unknown> => [...c.keys()].sort()

describe.runIf(hasReload)("collection.reload() (@tanstack/db 1.0)", () => {
  for (const syncMode of ["eager", "on-demand"] as const) {
    it(`${syncMode}: a row deleted while disconnected is gone after a reload, and the rest stay live`, async () => {
      const r = room(syncMode)
      const l = link()
      const t = transportFor(r, l)
      await t.connect()
      await serverExec(r, "INSERT INTO messages(id,body) VALUES('a','hi'),('b','hi'),('c','hi')")

      const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode }))
      await messages.preload()
      const hi = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "hi")))
      await hi.preload()
      expect(keysOf(hi)).toEqual(["a", "b", "c"])

      // The socket drops; the server deletes `b` and tells nobody.
      await disconnect(r, l)
      await serverExec(r, "DELETE FROM messages WHERE id='b'")
      l.down = false

      await reload(messages)
      await waitFor(() => !messages.has("b"))
      await waitFor(() => keysOf(hi).length === 2)
      expect(keysOf(messages)).toEqual(["a", "c"])
      expect(keysOf(hi)).toEqual(["a", "c"])

      // The new run's subscriptions are live.
      await serverExec(r, "INSERT INTO messages(id,body) VALUES('d','hi')")
      await waitFor(() => hi.has("d"))
      await serverExec(r, "DELETE FROM messages WHERE id='a'")
      await waitFor(() => !hi.has("a"))
      expect(keysOf(messages)).toEqual(["c", "d"])
      t.close()
    })
  }

  it("on-demand: rows still queued behind a persisting write are not kept by a reload", async () => {
    // Core holds a sync commit back while a user write is persisting. The rows
    // of the replaced run are then not in the synced store yet when the new run
    // starts, and they land later. The new run must not take "no rows" for
    // "nothing to drop".
    const r = room("queued")
    const l = link({ only: ["committed"] })
    const t = transportFor(r, l)
    await t.connect()
    await serverExec(r, "INSERT INTO messages(id,body) VALUES('a','hi'),('b','hi'),('c','hi')")
    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()

    l.hold = true
    const write = settled(messages.insert({ id: "w", body: "w" })).catch(() => {}) // persisting: its receipt is held
    const hi = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "hi")))
    void hi.preload().catch(() => {})
    await waitFor(() => t.appliedCursor !== "0") // the subset's snapshot arrived...
    expect(messages.has("a")).toBe(false) // ...and its commit is queued behind the write

    await disconnect(r, l)
    await serverExec(r, "DELETE FROM messages WHERE id='b'")
    l.down = false

    await reload(messages)
    l.hold = false
    for (const f of l.held.splice(0)) f.deliver()
    await write
    await waitFor(() => messages.has("a") && messages.has("c"))
    expect(messages.has("b")).toBe(false)
    expect(keysOf(hi)).toEqual(["a", "c"])
    t.close()
  })
})

describe("restarting an on-demand collection", () => {
  it("a ready callback of the app that throws is reported uncaught, not as a failed load", async () => {
    // A restarted run starts with a truncate, and applying it makes the
    // collection ready in core, which runs the app's ready callbacks. One that
    // throws is the app's error: the collection is ready and stays ready, as
    // it does when the callback throws from the first run's markReady().
    const r = room("observer")
    const t = transportFor(r, link())
    await t.connect()
    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()
    await messages.cleanup()

    // The test runner fails a run on any uncaught error, so take its place
    // for the one this test expects.
    type Listener = (reason: unknown) => void
    const { process } = globalThis as unknown as {
      process: { listeners: (e: string) => Array<Listener>; removeAllListeners: (e: string) => void; on: (e: string, l: Listener) => void }
    }
    const uncaught: Array<unknown> = []
    const runners = process.listeners("unhandledRejection")
    process.removeAllListeners("unhandledRejection")
    process.on("unhandledRejection", (e) => void uncaught.push(e))
    try {
      messages.onFirstReady(() => {
        throw new Error("the app's ready callback threw")
      })
      void messages.preload().catch(() => {})
      await waitFor(() => uncaught.length > 0)
    } finally {
      process.removeAllListeners("unhandledRejection")
      for (const l of runners) process.on("unhandledRejection", l)
    }
    expect(String(uncaught[0])).toContain("the app's ready callback threw")
    expect(messages.status).toBe("ready")
    expect((messages as { health?: { hasErrors: boolean } }).health?.hasErrors ?? false).toBe(false)
    t.close()
  })
})

describe("restarting an eager collection while its first snapshot is arriving", () => {
  /** Start a collection over: `reload()` from 1.0, `cleanup()` then `preload()` before it. */
  async function restart(c: { preload: () => Promise<void>; cleanup: () => Promise<void> }): Promise<void> {
    if (hasReload) return void reload(c).catch(() => {})
    await c.cleanup()
    void c.preload().catch(() => {})
  }

  it("the old subscription's remaining frames are not taken for the new run's snapshot", async () => {
    // The replaced run has consumed part of its snapshot. If its remaining
    // frames reach the new run, their snap-end looks like the end of a whole
    // snapshot: the transport marks the new subscription bootstrapped, so after
    // a drop it resumes from the cursor and the rows of the consumed part are
    // never sent again.
    const r = room("tail")
    const l = link()
    const t = transportFor(r, l)
    await t.connect()
    await serverExec(r, "INSERT INTO messages(id,body) VALUES('a','hi'),('b','hi'),('c','hi')")

    l.hold = true
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    void messages.preload().catch(() => {})
    const ends = (): number => l.held.filter((f) => f.t === "snap-end").length
    await waitFor(() => ends() === 1)
    l.held.shift()!.deliver() // the first run consumes one row of its snapshot

    await restart(messages)
    await waitFor(() => ends() === 2) // the new run's snapshot is on its way behind the old tail

    // The old tail arrives, then the socket drops before the new snapshot does.
    const tail = l.held.splice(0, l.held.findIndex((f) => f.t === "snap-end") + 1)
    for (const f of tail) f.deliver()
    l.held.length = 0
    l.hold = false
    await dropSocket(r)

    await waitFor(() => messages.size === 3)
    expect(keysOf(messages)).toEqual(["a", "b", "c"])
    t.close()
  })
})
