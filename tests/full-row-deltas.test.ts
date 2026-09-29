import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ServerFrame } from "../src/wire/frames.ts"

// Issue #28 part (a), bugbash G28a. WHY: a live `d` for an update carries the
// WHOLE current row, however little changed — including nothing. That is a
// design property, not an accident:
//   - capture records only (key, op) and rows hydrate fresh at drain, so a
//     column an author adds later flows with no trigger change (ADR-0007;
//     schema-evolution.test.ts);
//   - the always-emit rule sends a matching row's current state to every
//     sub with no per-sub membership, and the client applies an update for a
//     key it lacks as an upsert — move-in (ADR-0002 C4). A column-only patch
//     would upsert a partial row there;
//   - live deltas are self-contained (ADR-0009's premise), so the broadcaster
//     may keep only the latest delta per key per tick, and a missed delta
//     heals at the key's next write.
// The cost is bandwidth: row size × write rate × subscribers. ADR-0018 D3
// warns above 1 MiB and points here. Column projection would change the delta
// contract and needs its own ADR; if it lands, this test is what it supersedes.

const codec = createFrameCodec()
const BIG = "x".repeat(300_000)

type Frame = { f: ServerFrame; bytes: number }

async function setup(room: string): Promise<{ ws: WebSocket; frames: Array<Frame>; stub: DurableObjectStub }> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
  await runInDurableObject(stub, (inst, s) => {
    s.storage.sql.exec("ALTER TABLE messages ADD COLUMN n INTEGER") // a small column beside the big one
    s.storage.sql.exec("INSERT INTO messages(id, body, n) VALUES ('big', ?, 0)", BIG)
    ;(inst as unknown as { sync: { drainAndBroadcast(): void } }).sync.drainAndBroadcast()
  })
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket!
  ws.accept()
  const frames: Array<Frame> = []
  ws.addEventListener("message", (e) => {
    frames.push({ f: codec.decode(e.data as ArrayBuffer) as ServerFrame, bytes: (e.data as ArrayBuffer).byteLength })
  })
  ws.send(codec.encode({ t: "sub", subId: "s1", collection: "messages" }))
  await waitFor(() => frames.some(({ f }) => f.t === "snap-end"))
  return { ws, frames, stub }
}

function serverWrite(stub: DurableObjectStub, stmt: string): Promise<void> {
  return runInDurableObject(stub, (inst) => {
    ;(inst as unknown as { sync: { runSyncedWrite(fn: (sql: SqlStorage) => void): void } }).sync.runSyncedWrite((sql) => {
      sql.exec(stmt)
    })
  })
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

const delta = (frames: Array<Frame>): Frame | undefined => frames.find(({ f }) => f.t === "d")

describe("#28 (a): an update's live delta is the full current row", () => {
  it("a narrow UPDATE of a small column re-sends the unchanged 300 KB column", async () => {
    const { ws, frames, stub } = await setup(`g28-narrow-${crypto.randomUUID()}`)
    await serverWrite(stub, "UPDATE messages SET n = 1 WHERE id = 'big'")
    await waitFor(() => delta(frames) !== undefined)
    ws.close()
    const d = delta(frames)!
    expect(d.f).toMatchObject({ t: "d", sub: "s1", key: "big", op: "update", cols: { id: "big", body: BIG, n: 1 } })
    expect(d.bytes).toBeGreaterThan(300_000)
  })

  it("an UPDATE that changes nothing still emits the full row", async () => {
    const { ws, frames, stub } = await setup(`g28-noop-${crypto.randomUUID()}`)
    await serverWrite(stub, "UPDATE messages SET body = body WHERE id = 'big'")
    await waitFor(() => delta(frames) !== undefined)
    ws.close()
    const d = delta(frames)!
    expect(d.f).toMatchObject({ t: "d", key: "big", op: "update", cols: { id: "big", body: BIG, n: 0 } })
    expect(d.bytes).toBeGreaterThan(300_000)
  })
})
