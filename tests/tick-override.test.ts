import { env, runInDurableObject, SELF } from "cloudflare:test"
import { expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

it("honours a subclass tickMs field without flushing at the constructor's default tick", async () => {
  const room = `tick-override-${crypto.randomUUID()}`
  const response = await SELF.fetch(`https://example.com/slow/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = response.webSocket!
  ws.accept()
  const frames: Array<ServerFrame> = []
  ws.addEventListener("message", (event) => {
    frames.push(codec.decode((event as MessageEvent).data as ArrayBuffer) as ServerFrame)
  })
  try {
    ws.send(codec.encode({ t: "sub", subId: "s", collection: "messages" }))
    await waitFor(() => frames.some((frame) => frame.t === "snap-end"))
    await runInDurableObject(env.SLOW_DO.get(env.SLOW_DO.idFromName(room)), (instance, state) => {
      state.storage.sql.exec("INSERT INTO messages(id,body) VALUES('x','v')")
      ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
    })
    await new Promise((resolve) => setTimeout(resolve, 1000))
    expect(frames.some((frame) => frame.t === "d")).toBe(false)

    ws.send(codec.encode({ t: "sub", subId: "barrier", collection: "files" }))
    await waitFor(() => frames.some((frame) => frame.t === "snap-end" && frame.sub === "barrier"))
    expect(frames.find((frame) => frame.t === "d")).toMatchObject({ key: "x", cols: { body: "v" } })
  } finally {
    ws.close()
  }
})
