import { createCollection } from "@tanstack/db"
import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"
import { settled } from "./settled.ts"

// WHY: a session typically watches several tables of one DO at once. The design
// multiplexes all of them over a SINGLE WebSocket (transport demuxes by subId,
// server routes by collection). This pins that two collections sync
// independently over exactly one socket — one connection per DO, not per table.

describe("multi-collection multiplexing over one WS (M8)", () => {
  it("syncs two collections over a single shared socket", async () => {
    const room = "mux"
    const sockets: Array<WebSocketLike> = []
    const transport = new WebSocketTransport<TestApi>({
      url: `https://example.com/sync/${room}`,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        const like = ws as unknown as WebSocketLike
        sockets.push(like)
        return like
      },
    })

    const messages = createCollection(doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id }))
    const files = createCollection(doCollectionOptions({ transport, table: "files", getKey: (r) => r.id }))
    await Promise.all([messages.preload(), files.preload()])

    await settled(messages.insert({ id: "m1", body: "hi" }))
    await settled(files.insert({ id: "f1", name: "doc" }))

    expect(messages.get("m1")).toMatchObject({ id: "m1", body: "hi" })
    expect(files.get("f1")).toMatchObject({ id: "f1", name: "doc" })
    // The two collections shared one transport -> exactly one socket was opened.
    expect(sockets.length).toBe(1)

    transport.close()
  })
})
