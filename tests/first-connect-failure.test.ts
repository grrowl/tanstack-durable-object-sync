import { CollectionImpl, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

// WHY (ADR-0028): a collection whose first connect fails must say so. Its
// `preload()` rejects with the cause instead of waiting forever on a server
// that is not there. The adapter reports this through the sync controls, which
// @tanstack/db renamed in 1.0 (`markError` became `fail`). Calling only the old
// name does nothing on 1.0, and the collection stays `loading`.
//
// A failed collection must also be recoverable once the server is reachable,
// through the call upstream gives an app for it: `reload()` from 1.0,
// `cleanup()` then `preload()` before it.

const hasReload = "reload" in CollectionImpl.prototype

const room = (tag: string): string => `fcf-${tag}-${crypto.randomUUID()}`
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await sleep(5)
  }
}

interface Dial {
  down: boolean
}
/** A real transport to the DO whose dial fails while `dial.down`. */
function transportFor(r: string, dial: Dial): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 5,
    open: async () => {
      if (dial.down) throw new Error("dial failed: server unreachable")
      const res = await SELF.fetch(`https://example.com/sync/${r}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
}
/** A room whose DO has its schema and one row. */
async function seededRoom(tag: string): Promise<string> {
  const r = room(tag)
  const t = transportFor(r, { down: false })
  await t.connect() // the upgrade creates the schema
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(r)), (_i, s) => {
    s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('seed','hi')")
  })
  t.close()
  return r
}
/** Reject if `p` has not settled in time: a hang must fail the assertion that
 *  awaits it, not time the whole test out. */
function within<T>(p: Promise<T>, ms = 1500): Promise<T> {
  return Promise.race([p, sleep(ms).then(() => Promise.reject(new Error(`still pending after ${ms} ms`)))])
}
/** Start a failed collection over, as upstream tells an app to. */
async function restart(c: { preload: () => Promise<void>; cleanup: () => Promise<void> }): Promise<void> {
  const reload = (c as { reload?: () => Promise<void> }).reload
  if (reload) return reload.call(c)
  await c.cleanup()
  await c.preload()
}

describe("a failed first connect fails the collection's first load", () => {
  for (const syncMode of ["eager", "on-demand"] as const) {
    it(`${syncMode}: preload() rejects with the cause, and a restart recovers once the server is reachable`, async () => {
      const r = await seededRoom(syncMode)
      const dial: Dial = { down: true }
      const t = transportFor(r, dial)
      const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode }))

      await expect(within(messages.preload())).rejects.toThrow("dial failed: server unreachable")
      expect(messages.status).toBe("error")

      dial.down = false
      await within(restart(messages))
      expect(messages.status).toBe("ready")
      const hi = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "hi")))
      await within(hi.preload())
      expect(hi.get("seed")).toMatchObject({ id: "seed", body: "hi" })
      t.close()
    })
  }

  // Before 1.0 an eager collection also heals with no call from the app: its
  // subscription keeps the transport retrying, and the snapshot that finally
  // arrives marks the collection ready. From 1.0 a failed sync run is over
  // (`markReady()` after `fail()` does nothing), so only the restart above
  // recovers it.
  it.runIf(!hasReload)("eager, before 1.0: the transport's own retry heals the collection", async () => {
    const r = await seededRoom("heal")
    const dial: Dial = { down: true }
    const t = transportFor(r, dial)
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    await expect(within(messages.preload())).rejects.toThrow("dial failed: server unreachable")

    dial.down = false
    await waitFor(() => messages.status === "ready")
    await within(messages.preload())
    expect(messages.get("seed")).toMatchObject({ id: "seed", body: "hi" })
    t.close()
  })
})
