import { CollectionImpl, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

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
  attempts?: number
  down: boolean
}
/** A real transport to the DO whose dial fails while `dial.down`. */
function transportFor(r: string, dial: Dial): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 5,
    open: async () => {
      dial.attempts = (dial.attempts ?? 0) + 1
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
describe("first load across a retry or intentional pause (ADR-0029)", () => {
  for (const syncMode of ["eager", "on-demand"] as const) {
    it.runIf(hasReload || syncMode === "on-demand")(`${syncMode} loads after a transient outage without restarting`, async () => {
      const r = await seededRoom(syncMode)
      const dial: Dial = { down: true }
      const t = transportFor(r, dial)
      const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: m => m.id, syncMode }))
      if (syncMode === "on-demand") {
        await within(messages.preload())
        expect(dial.attempts ?? 0).toBe(0)
      }
      const hi = createLiveQueryCollection(q => q.from({ m: messages }).where(({ m }) => eq(m.body, "hi")))
      const load = hi.preload()
      await waitFor(() => (dial.attempts ?? 0) >= 2)
      expect(hi.status).toBe("loading")
      dial.down = false
      await within(load)
      expect(hi.get("seed")).toMatchObject({ id: "seed", body: "hi" })
      await hi.cleanup()
      await messages.cleanup()
      t.close()
    })

    it(`${syncMode} survives close and remount during its first dial`, async () => {
      const r = await seededRoom("remount")
      const dial = { down: false, attempts: 0 }
      const t = transportFor(r, dial)
      const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: m => m.id, syncMode }))
      const hi = createLiveQueryCollection(q => q.from({ m: messages }))
      const load = hi.preload()
      expect(dial.attempts).toBe(1)
      t.close()
      await t.connect()
      await within(load)
      expect(hi.get("seed")).toMatchObject({ body: "hi" })
      await hi.cleanup()
      await messages.cleanup()
      t.close()
    })
  }

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
