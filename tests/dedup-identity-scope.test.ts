import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// bugbash L3 — accepted limitation, deferred by ADR-0012 D4 ("Dedup identity
// binding deferred"). WHY: `_sync_seen_tx` is keyed by txId alone, so the
// receipt belongs to whichever socket presented the txId FIRST, whoever that
// was. Two halves, both `it.fails` until an identity-scoped dedup lands. They
// encode the scoping D4 sketches (a `dedupScope(user)` hook): another identity's
// txId is simply new in your scope, so your frame runs. A design that instead
// refuses cross-identity collisions would need to re-state them.
//   1. Read (the half D4 names): another identity presenting your txId gets
//      your stored receipt, including a command's result.
//   2. Pre-claim (wider than D4's text): another identity that uses a txId
//      FIRST makes your later write with it answer `committed` without running.
// Both need the txId; built-in clients mint CSPRNG UUIDs and no peer frame
// carries one, which is why D4 rates the risk low. Predictable app-supplied
// transaction ids, or txIds leaked through telemetry, raise it.

const codec = createFrameCodec()

async function openAs(room: string, user: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket", "x-user": user } })
  const ws = res.webSocket!
  ws.accept()
  return ws
}

function receipt(ws: WebSocket, f: ClientFrame & { txId: string }): Promise<ServerFrame> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no receipt for ${f.txId}`)), 2000)
    const onMsg = (e: MessageEvent): void => {
      const x = codec.decode(e.data as ArrayBuffer) as ServerFrame
      if ((x.t === "committed" || x.t === "rejected") && x.txId === f.txId) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(x)
      }
    }
    ws.addEventListener("message", onMsg)
    ws.send(codec.encode(f))
  })
}

const rowIds = (room: string): Promise<Array<string>> =>
  runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) =>
    Array.from(s.storage.sql.exec<{ id: string }>("SELECT id FROM messages ORDER BY id")).map((r) => r.id),
  )

describe("dedup is scoped to the identity that owns the txId (ADR-0012 D4, deferred)", () => {
  it("control: the SAME identity retrying a txId gets its stored result replayed", async () => {
    const room = `l3-control-${crypto.randomUUID()}`
    const alice = await openAs(room, "alice")
    const alice2 = await openAs(room, "alice") // a reconnect: a new socket, same identity
    await receipt(alice, { t: "call", txId: "tx-1", name: "echo", args: { secret: "first" } })
    const retry = await receipt(alice2, { t: "call", txId: "tx-1", name: "echo", args: { secret: "second" } })
    alice.close()
    alice2.close()
    expect(retry).toMatchObject({ t: "committed", result: { echoed: { secret: "first" } } })
  })

  // bugbash L3 (read half) — ADR-0012 D4.
  it.fails("another identity presenting your txId does not receive your stored result", async () => {
    const room = `l3-read-${crypto.randomUUID()}`
    const alice = await openAs(room, "alice")
    const bob = await openAs(room, "bob")
    await receipt(alice, { t: "call", txId: "tx-shared", name: "echo", args: { secret: "alice-only" } })
    const b = await receipt(bob, { t: "call", txId: "tx-shared", name: "echo", args: { secret: "bob" } })
    alice.close()
    bob.close()
    // today: bob gets {committed, result:{echoed:{secret:"alice-only"}}}
    expect(b).toMatchObject({ t: "committed", result: { echoed: { secret: "bob" } } })
  })

  // bugbash L3 (pre-claim half) — wider than ADR-0012 D4's text.
  it.fails("another identity using a txId first does not make your later write report committed unexecuted", async () => {
    const room = `l3-preclaim-${crypto.randomUUID()}`
    const alice = await openAs(room, "alice")
    const bob = await openAs(room, "bob")
    await receipt(bob, { t: "mut", txId: "tx-pre", collection: "messages", ops: [{ type: "insert", key: "bob-row", cols: { id: "bob-row", body: "b" } }] })
    const a = await receipt(alice, { t: "mut", txId: "tx-pre", collection: "messages", ops: [{ type: "insert", key: "alice-row", cols: { id: "alice-row", body: "a" } }] })
    const rows = await rowIds(room)
    alice.close()
    bob.close()
    // today: alice gets bob's `committed` receipt and rows are ["bob-row"].
    expect(a.t).toBe("committed")
    expect(rows).toEqual(["alice-row", "bob-row"])
  })
})
