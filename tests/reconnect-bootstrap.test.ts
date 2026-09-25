import { createCollection, createLiveQueryCollection, eq, gt } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: one shared cursor serves every sub on the socket, and it says nothing
// about rows a sub never received. A sub that drops before its first terminal
// (snap-end, or its own catch-up uptodate) must therefore restart from a
// snapshot, not resume from the cursor as a catch-up (ADR-0023 D7) — and what
// the partial first snapshot delivered must not outlive the restart.

const room = (tag: string): string => `rb-${tag}-${crypto.randomUUID()}`
const stubFor = (r: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(r))
async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
async function seed(r: string, bodies: Array<string>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    bodies.forEach((b, i) => s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${String(i).padStart(2, "0")}`, b))
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
async function serverExec(r: string, sql: string, ...args: Array<unknown>): Promise<void> {
  await runInDurableObject(stubFor(r), (instance, s) => {
    s.storage.sql.exec(sql, ...args)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}
const dropSocket = (r: string) =>
  runInDurableObject(stubFor(r), (_i, state) => {
    for (const sock of state.getWebSockets()) sock.close(1000, "drop")
  })

interface Gate {
  hold: boolean
  queue: Array<() => void>
}
/** A real transport whose inbound frames can be held (in order) while `hold`. */
function transportFor(r: string, gate: Gate): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${r}`,
    reconnectDelay: () => 0,
    open: async () => {
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
            if (gate.hold) gate.queue.push(() => fn(ev as never))
            else fn(ev as never)
          })
        },
        removeEventListener: () => {},
      } satisfies WebSocketLike
    },
  })
}

describe("reconnect before a sub's first snapshot", () => {
  it("a sub that never bootstrapped restarts from a snapshot, not a catch-up", async () => {
    // A drop after a new sub was sent but before its snap-end, with the cursor
    // already > 0 from another sub, used to resubscribe it with `since`: the DO
    // answered a catch-up (changed keys only), its rows never loaded and its
    // load hung.
    const r = room("f7")
    const gate: Gate = { hold: false, queue: [] }
    const t = transportFor(r, gate)
    await seed(r, ["01", "02", "03", "04", "05", "06"])
    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    const q1 = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => gt(m.body, "04")))
    await q1.preload()
    await waitFor(() => q1.size === 2)
    expect(BigInt(t.appliedCursor)).toBeGreaterThan(0n)

    gate.hold = true
    const q2 = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "01")))
    const loaded = q2.preload()
    await sleep(30) // q2's sub was sent and answered; the answer is lost in the drop
    gate.queue.length = 0 // the dead socket's frames never arrive
    gate.hold = false
    await dropSocket(r)
    await loaded
    await waitFor(() => q2.size === 1)
    expect(q1.size).toBe(2)
    t.close()
  })

  it("an eager sub restarted after a partial first snapshot carries no deleted row over", async () => {
    // Part of the first snapshot arrived (inserts in the still-open sync
    // transaction), then the socket dropped and the DO deleted one of those
    // rows. The restart's snapshot replaces the partial one; the eager
    // reconcile only sees SYNCED rows, so the partial one must be undone.
    const r = room("eager-partial")
    const gate: Gate = { hold: false, queue: [] }
    const t = transportFor(r, gate)
    await seed(r, ["a", "b", "c"])
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    await t.connect()
    gate.hold = true
    void messages.preload().catch(() => {})
    await sleep(30) // snap m00, snap m01, snap m02, snap-end are held
    gate.queue.splice(0).slice(0, 2).forEach((deliver) => deliver()) // m00, m01 arrive; the rest is lost
    await serverExec(r, "DELETE FROM messages WHERE id = ?", "m01")
    await sleep(10)
    gate.queue.length = 0 // its delta dies with the socket
    gate.hold = false
    await dropSocket(r)
    await waitFor(() => messages.size === 2)
    await sleep(50)
    expect(messages.get("m01")).toBeUndefined()
    expect([...messages.keys()].sort()).toEqual(["m00", "m02"])
    t.close()
  })
})

describe("an explicit catch-up that drops before its terminal", () => {
  it("resumes from its own since, not from a cursor other subs advanced", async () => {
    // SSR hydration subscribes with `since` = the dehydrated cursor. If the
    // socket drops before that catch-up's own terminal while another sub on the
    // transport advanced the shared cursor, resuming from the cursor would skip
    // the changes in between.
    const r = room("explicit-since")
    const gate: Gate = { hold: false, queue: [] }
    const t = transportFor(r, gate)
    await seed(r, ["a", "b"])
    const noop: SubHandler = { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} }
    let aReady!: () => void
    const aLoaded = new Promise<void>((res) => (aReady = res))
    await t.subscribe("A", "messages", { ...noop, onSnapEnd: () => aReady() })
    await aLoaded
    const since = t.appliedCursor // S0

    await serverExec(r, "UPDATE messages SET body = ? WHERE id = ?", "b2", "m01") // S1: the change to not lose
    await waitFor(() => BigInt(t.appliedCursor) > BigInt(since)) // A's boundary advanced the cursor

    const seen: Array<string> = []
    gate.hold = true
    await t.subscribe("B", "messages", { ...noop, onDelta: (_op, key) => seen.push(key as string) }, undefined, undefined, undefined, since)
    await sleep(30)
    gate.queue.length = 0 // B's catch-up is lost in the coming drop
    gate.hold = false
    await dropSocket(r)
    await waitFor(() => seen.includes("m01"))
    t.close()
  })
})

describe("a fetch page from an abandoned socket", () => {
  it("rejects its fetch instead of delivering rows the fresh socket may already have replaced", async () => {
    // A late hydration chunk regresses the cursor and forces a reconnect; the
    // fresh socket's replay may delete a row the old socket's in-flight page
    // still carries. Installing that page would resurrect the row for good —
    // no later delta mentions it — so the page settles its fetch as a failure.
    const r = room("stale-page")
    const gate: Gate = { hold: false, queue: [] }
    const t = transportFor(r, gate)
    await seed(r, ["a", "b"])
    const noop: SubHandler = { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} }
    let ready!: () => void
    const loaded = new Promise<void>((res) => (ready = res))
    await t.subscribe("A", "messages", { ...noop, onSnapEnd: () => ready() })
    await loaded
    expect(BigInt(t.appliedCursor)).toBeGreaterThan(1n)

    gate.hold = true
    const page = t.fetch({ t: "fetch", fetchId: "f-stale", collection: "messages", limit: 10 })
    const settled = page.then(
      () => "resolved",
      (e: Error) => `rejected: ${e.message}`,
    )
    await sleep(30) // the page is held on the old socket
    t.seedCursor("1") // regress: forced reconnect onto a fresh socket
    const held = gate.queue.splice(0)
    gate.hold = false
    held.forEach((deliver) => deliver()) // the old socket's page lands after the switch
    expect(await settled).toMatch(/^rejected: .*abandoned socket/)
    t.close()
  })
})
