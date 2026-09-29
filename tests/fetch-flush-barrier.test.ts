import { createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

const codec = createFrameCodec()

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

async function serverExec(room: string, sql: string): Promise<void> {
  await runInDurableObject(env.SLOW_DO.get(env.SLOW_DO.idFromName(room)), (instance, state) => {
    state.storage.sql.exec(sql)
    ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
  })
}

it("flushes a held row's buffered delta before a covered fetch settles", async () => {
  const room = `fetch-barrier-${crypto.randomUUID()}`
  const frames: Array<ServerFrame> = []
  const transport = new WebSocketTransport<TestApi>({
    url: `https://example.com/slow/${room}`,
    open: async () => {
      const response = await SELF.fetch(`https://example.com/slow/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = response.webSocket!
      ws.accept()
      ws.addEventListener("message", (event) => {
        frames.push(codec.decode((event as MessageEvent).data as ArrayBuffer) as ServerFrame)
      })
      return ws as unknown as WebSocketLike
    },
  })
  try {
    await serverExec(room, "INSERT INTO messages(id,body) VALUES('a','old')")
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (message) => message.id, syncMode: "on-demand" }),
    )
    const all = createLiveQueryCollection((query) => query.from({ message: messages }))
    await all.preload()
    await waitFor(() => messages.get("a")?.body === "old")

    await serverExec(room, "UPDATE messages SET body='new' WHERE id='a'")
    const mark = frames.length
    expect(frames.some((frame) => frame.t === "d")).toBe(false)
    const one = createLiveQueryCollection((query) => query.from({ message: messages }).where(({ message }) => eq(message.id, "a")))
    await one.preload()

    expect(messages.get("a")?.body).toBe("new")
    const after = frames.slice(mark)
    const page = after.findIndex((frame) => frame.t === "page")
    const delta = after.findIndex((frame) => frame.t === "d" && frame.key === "a")
    const boundary = after.findIndex((frame) => frame.t === "uptodate")
    expect(page).toBeGreaterThanOrEqual(0)
    expect(delta).toBeGreaterThanOrEqual(0)
    expect(delta).toBeLessThan(boundary)
    expect(boundary).toBeLessThan(page)
    expect(after[page]).toMatchObject({ rows: [{ id: "a", body: "new" }] })
  } finally {
    transport.close()
  }
})
