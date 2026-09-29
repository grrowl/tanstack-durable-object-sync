import { env, runInDurableObject, SELF } from "cloudflare:test"
import { expect, it, vi } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()

async function waitFor(pred: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

it.each(["close", "error"])("a socket %s cancels the only subscribed socket's buffered 30-second tick", async (event) => {
  const room = `close-timer-${crypto.randomUUID()}`
  const stub = env.SLOW_DO.get(env.SLOW_DO.idFromName(room))
  const response = await SELF.fetch(`https://example.com/slow/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = response.webSocket!
  ws.accept()
  const frames: Array<ServerFrame> = []
  ws.addEventListener("message", (event) => {
    frames.push(codec.decode((event as MessageEvent).data as ArrayBuffer) as ServerFrame)
  })
  const cancel = vi.spyOn(globalThis, "clearTimeout")
  let pendingTimer: ReturnType<typeof setTimeout> | undefined
  try {
    ws.send(codec.encode({ t: "sub", subId: "s", collection: "messages" }))
    await waitFor(() => frames.some((frame) => frame.t === "snap-end"))
    pendingTimer = await runInDurableObject(stub, (instance, state) => {
      const schedule = vi.spyOn(globalThis, "setTimeout")
      try {
        state.storage.sql.exec("INSERT INTO messages(id,body) VALUES('x','v')")
        ;(instance as unknown as { drainAndBroadcast(): void }).drainAndBroadcast()
        const tick = schedule.mock.calls.findIndex(([, delay]) => delay === 30_000)
        expect(tick).toBeGreaterThanOrEqual(0)
        return schedule.mock.results[tick]!.value as ReturnType<typeof setTimeout>
      } finally {
        schedule.mockRestore()
      }
    })
    expect(cancel.mock.calls.some(([timer]) => timer === pendingTimer)).toBe(false)
    expect(frames.some((frame) => frame.t === "d")).toBe(false)

    if (event === "close") {
      ws.close()
    } else {
      await runInDurableObject(stub, (instance, state) => {
        const [serverWs] = state.getWebSockets()
        expect(serverWs).toBeDefined()
        ;(instance as unknown as { webSocketError(ws: WebSocket, error: unknown): void })
          .webSocketError(serverWs!, new Error("socket failed"))
      })
    }
    await waitFor(() => runInDurableObject(stub, (_instance, state) =>
      state.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM _sync_subs").one().count === 0,
    ))
    expect(cancel.mock.calls.some(([timer]) => timer === pendingTimer)).toBe(true)
    expect(frames.some((frame) => frame.t === "d")).toBe(false)
  } finally {
    cancel.mockRestore()
    ws.close()
    if (pendingTimer !== undefined) await runInDurableObject(stub, () => clearTimeout(pendingTimer))
  }
})
