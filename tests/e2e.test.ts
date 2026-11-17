import { createCollection } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"
import { settled } from "./settled.ts"

// WHY: the real proof of the stack — a genuine @tanstack/db createCollection,
// driven by our adapter + transport, against a real DO in workerd (not spy
// controls). Exercises sync (begin/write/commit/markReady) from server frames
// and the optimistic insert -> mut -> single-stream confirmation path through
// the actual TanStack runtime. Possible because createCollection runs in
// workerd (verified by probes).

function makeTransport(room: string): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
}

describe("end-to-end: createCollection + adapter + transport + DO", () => {
  it("syncs the initial snapshot and confirms an optimistic insert", async () => {
    const room = "e2e-basic"
    const t = makeTransport(room)
    await t.connect() // upgrade creates the schema

    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('seed','hi')")
    })

    const messages = createCollection(
      doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }),
    )

    await messages.preload() // starts sync -> subscribe -> snapshot -> markReady
    expect(messages.get("seed")).toMatchObject({ id: "seed", body: "hi" })

    const tx = messages.insert({ id: "a", body: "yo" })
    await settled(tx)
    expect(messages.get("a")).toMatchObject({ id: "a", body: "yo" })

    t.close()
  })

  it("reflects a second client's write via a live delta", async () => {
    const room = "e2e-two"
    const ta = makeTransport(room)
    const tb = makeTransport(room)
    await Promise.all([ta.connect(), tb.connect()])

    const a = createCollection(doCollectionOptions({ transport: ta, table: "messages", getKey: (r) => r.id }))
    const b = createCollection(doCollectionOptions({ transport: tb, table: "messages", getKey: (r) => r.id }))
    await Promise.all([a.preload(), b.preload()])

    await settled(a.insert({ id: "x", body: "from-a" }))

    // B receives x via a live delta (coalescer tick), with no local write.
    const start = Date.now()
    while (!b.get("x")) {
      if (Date.now() - start > 2000) throw new Error("B never saw A's write")
      await new Promise((r) => setTimeout(r, 5))
    }
    expect(b.get("x")).toMatchObject({ id: "x", body: "from-a" })

    ta.close()
    tb.close()
  })
})
