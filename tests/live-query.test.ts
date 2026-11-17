import { createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"
import { settled } from "./settled.ts"

// WHY: the entire reactive layer (filtering, joins, incremental view
// maintenance) is the client's job — the DO only stores + emits, never runs
// IVM (ADR-0001 D15). This pins that a TanStack DB live query composes over our
// DO-backed collection and updates incrementally as synced data changes: the DO
// streams every row, the client's IVM derives the filtered view.

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

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("client live query / IVM over a DO-backed collection (M8)", () => {
  it("derives a filtered view client-side and updates it incrementally", async () => {
    const t = makeTransport("lq")
    await t.connect()
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }))
    await messages.preload()

    // A live query filtering client-side — the DO sends every row; IVM filters.
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    await settled(messages.insert({ id: "a", body: "keep" }))
    await settled(messages.insert({ id: "b", body: "drop" }))

    await waitFor(() => kept.get("a") !== undefined)
    expect(kept.get("a")).toMatchObject({ id: "a", body: "keep" })
    expect(kept.get("b")).toBeUndefined() // filtered out client-side

    // Incremental update: edit 'b' into the view, 'a' out of it.
    await settled(
      messages.update("b", (d) => {
        d.body = "keep"
      }),
    )
    await settled(
      messages.update("a", (d) => {
        d.body = "drop"
      }),
    )

    await waitFor(() => kept.get("b") !== undefined && kept.get("a") === undefined)
    expect(kept.get("b")).toMatchObject({ id: "b", body: "keep" })

    t.close()
  })
})
