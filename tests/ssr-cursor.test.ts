import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (ADR-0011 D3): SSR hydration hands a client rows it did not stream — so
// the FIRST sub must be able to resume from the dehydrated cursor (server
// catch-up, not a redundant snapshot), and the transport must be able to claim
// that position before/around live traffic:
//   - seedCursor before any advance: a drop in the bootstrap window otherwise
//     resubscribes from 0 → fresh snapshot over hydrated rows → a row deleted
//     server-side meanwhile is never removed (snapshots carry no tombstones).
//   - seedCursor AFTER live advance (a late streamed SSR chunk): upstream has
//     already applied the chunk's possibly-stale rows — we cannot veto. The
//     transport claims the SHORTER prefix (always safe) and resubscribes, so
//     the catch-up replay re-freshens exactly the clobbered window.

interface Rec {
  events: Array<[string, ...Array<unknown>]>
  handler: SubHandler
}
function recorder(): Rec {
  const events: Array<[string, ...Array<unknown>]> = []
  return {
    events,
    handler: {
      onSnap: (k, r) => events.push(["snap", k, r]),
      onSnapEnd: () => events.push(["snap-end"]),
      onDelta: (op, k, c) => events.push(["d", op, k, c]),
      onUptodate: () => events.push(["uptodate"]),
      onReset: () => events.push(["reset"]),
    },
  }
}

function makeTransport(room: string): WebSocketTransport {
  return new WebSocketTransport({
    url: `https://example.com/sync/${room}`,
    reconnectDelay: 20,
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

function stubFor(room: string): DurableObjectStub {
  return env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
}

async function snapshotCursor(room: string): Promise<string> {
  const stub = stubFor(room) as unknown as {
    readSyncSnapshot: (r: { collection: string }, request: Request) => Promise<{ rows: Array<unknown>; cursor: string }>
  }
  return (await stub.readSyncSnapshot({ collection: "messages" }, new Request("https://example.com/ssr", { headers: { "x-user": "anon" } }))).cursor
}

describe("transport cursor bootstrap (SSR hydration, ADR-0011 D3)", () => {
  it("a FIRST sub carrying `since` gets a catch-up, not a snapshot", async () => {
    const room = `ssr-since-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hydrated')")
    })
    const cursor = await snapshotCursor(room) // what dehydration exported
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('b','missed')")
    })

    const t = makeTransport(room)
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler, undefined, undefined, undefined, cursor)
    await waitFor(() => events.some((e) => e[0] === "uptodate"))

    // The hydrated row is NOT re-streamed; only the post-cursor change is.
    expect(events.some((e) => e[0] === "snap")).toBe(false)
    expect(events.some((e) => e[0] === "snap-end")).toBe(false)
    expect(events.some((e) => e[0] === "d" && e[2] === "b")).toBe(true)
    expect(events.some((e) => e[0] === "d" && e[2] === "a")).toBe(false)
    t.close()
  })

  it("seedCursor claims the dehydrated position before any advance", async () => {
    const room = `ssr-seed-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hydrated')")
    })
    const cursor = await snapshotCursor(room)

    const t = makeTransport(room)
    expect(t.appliedCursor).toBe("0")
    t.seedCursor(cursor)
    expect(t.appliedCursor).toBe(cursor)
    // A seed can never grow the claim without data.
    t.seedCursor(String(BigInt(cursor) + 100n))
    expect(t.appliedCursor).toBe(cursor)
    t.close()
  })

  it("a late seed (streamed chunk after live advance) regresses the claim and replays the window", async () => {
    const room = `ssr-late-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','v1')")
    })
    const chunkCursor = await snapshotCursor(room) // a chunk dehydrated NOW...

    const t = makeTransport(room)
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler, undefined, undefined, undefined, chunkCursor)
    await waitFor(() => events.some((e) => e[0] === "uptodate"))

    // ...but it arrives LATE: live sync has moved on past another write
    // (driven through a real mut — raw SQL never broadcasts, ADR-0006).
    const t2 = makeTransport(room)
    await t2.sendMut({
      t: "mut",
      txId: `tx-${crypto.randomUUID()}`,
      collection: "messages",
      ops: [{ type: "update", key: "a", cols: { body: "v2" } }],
    })
    await waitFor(() => events.some((e) => e[0] === "d" && e[2] === "a"))
    t2.close()
    const advanced = t.appliedCursor
    expect(BigInt(advanced)).toBeGreaterThan(BigInt(chunkCursor))
    const before = events.length

    // Upstream already applied the chunk's stale rows; the transport claims
    // the shorter prefix and resubscribes — the replayed catch-up delivers the
    // post-chunk window again (idempotent) and re-freshens clobbered rows.
    t.seedCursor(chunkCursor)
    expect(t.appliedCursor).toBe(chunkCursor)
    await waitFor(() => events.slice(before).some((e) => e[0] === "d" && e[2] === "a"))
    await waitFor(() => BigInt(t.appliedCursor) >= BigInt(advanced))
    expect(events.slice(before).some((e) => e[0] === "snap")).toBe(false) // replay, not re-snapshot
    t.close()
  })

  it("a stale pre-regress boundary cannot re-advance the claim; the fresh socket replays from the seed", async () => {
    // Fully fake sockets: a live regress rides a RECONNECT because the old
    // socket's already-queued boundary frames (full duplex) would otherwise
    // re-advance the cursor past the repair window — then a drop would resume
    // beyond it and the late chunk's clobbered rows would stay stale forever.
    const codec = createFrameCodec()
    interface Fake {
      ws: WebSocketLike
      sent: Array<ClientFrame>
      emit: (type: string, ev: { data?: unknown }) => void
      closeCalled: boolean
    }
    const makeFake = (): Fake => {
      const listeners = new Map<string, Array<(ev: { data?: unknown }) => void>>()
      const fake: Fake = {
        sent: [],
        closeCalled: false,
        emit: (type, ev) => {
          for (const l of listeners.get(type) ?? []) l(ev)
        },
        ws: {
          send: (data) => fake.sent.push(codec.decode(data as ArrayBuffer | string) as ClientFrame),
          close: () => {
            fake.closeCalled = true // close event delivery is the TEST's choice
          },
          addEventListener: (type, l) => {
            const arr = listeners.get(type) ?? []
            arr.push(l)
            listeners.set(type, arr)
          },
          removeEventListener: () => {},
        },
      }
      return fake
    }
    const fakes: Array<Fake> = []
    const t = new WebSocketTransport({
      url: "wss://fake",
      reconnectDelay: 1,
      open: () => {
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    const { handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    const server = (frame: ServerFrame, fake = fakes.at(-1)!): void =>
      fake.emit("message", { data: codec.encode(frame) })

    server({ t: "snap-end", sub: "s1", seq: "100" })
    expect(t.appliedCursor).toBe("100")

    // Late chunk → regress. The transport must abandon this socket.
    t.seedCursor("50")
    expect(t.appliedCursor).toBe("50")
    expect(fakes[0]!.closeCalled).toBe(true)

    // A boundary the server sent BEFORE the close (still queued client-side)
    // must not count: the claim holds at the seed.
    server({ t: "uptodate", seq: "101" }, fakes[0]!)
    expect(t.appliedCursor).toBe("50")

    // Now the close lands; the fresh socket resubscribes FROM the seed...
    fakes[0]!.emit("close", {})
    await waitFor(() => fakes.length === 2 && fakes[1]!.sent.some((f) => f.t === "sub"))
    const resub = fakes[1]!.sent.find((f) => f.t === "sub") as Extract<ClientFrame, { t: "sub" }>
    expect(resub.since).toBe("50")

    // ...and its frames own the cursor again.
    server({ t: "uptodate", seq: "102" })
    expect(t.appliedCursor).toBe("102")
    t.close()
  })

  it("an abandoned socket still settles mutation receipts, fails pages — and never advances the cursor", async () => {
    // codex finding: a regress-reconnect must not convert a COMMITTED mutation
    // into a timeout — receipts are not re-covered by any replay. Stream
    // frames from the stale socket stay dropped (the catch-up re-covers them).
    const codec = createFrameCodec()
    const fakes: Array<{
      ws: WebSocketLike
      sent: Array<ClientFrame>
      emit: (type: string, ev: { data?: unknown }) => void
    }> = []
    const makeFake = () => {
      const listeners = new Map<string, Array<(ev: { data?: unknown }) => void>>()
      const fake = {
        sent: [] as Array<ClientFrame>,
        emit: (type: string, ev: { data?: unknown }) => {
          for (const l of listeners.get(type) ?? []) l(ev)
        },
        ws: {
          send: (data: string | ArrayBuffer | ArrayBufferView) =>
            fake.sent.push(codec.decode(data as ArrayBuffer | string) as ClientFrame),
          close: () => {},
          addEventListener: (type: string, l: (ev: { data?: unknown }) => void) => {
            const arr = listeners.get(type) ?? []
            arr.push(l)
            listeners.set(type, arr)
          },
          removeEventListener: () => {},
        } as WebSocketLike,
      }
      fakes.push(fake)
      return fake
    }
    const t = new WebSocketTransport({ url: "wss://fake", reconnectDelay: 1, open: () => makeFake().ws })
    const { handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    const emit = (frame: ServerFrame, i = fakes.length - 1): void =>
      fakes[i]!.emit("message", { data: codec.encode(frame) })

    emit({ t: "snap-end", sub: "s1", seq: "100" })
    const mut = t.sendMut({ t: "mut", txId: "tx-1", collection: "messages", ops: [] })
    const page = t.fetch({ t: "fetch", fetchId: "f-1", collection: "messages" })
    // Both frames must be ON the old socket before the regress abandons it.
    await waitFor(() => fakes[0]!.sent.some((f) => f.t === "mut") && fakes[0]!.sent.some((f) => f.t === "fetch"))

    t.seedCursor("50") // regress → the socket that carries tx-1/f-1 is abandoned
    expect(t.appliedCursor).toBe("50")

    // The stale socket's receipts settle their waiters...
    emit({ t: "committed", txId: "tx-1", seq: "101" }, 0)
    emit({ t: "page", fetchId: "f-1", rows: [{ id: "x" }], seq: "101" }, 0)
    await expect(mut).resolves.toEqual({ result: undefined })
    // A page is a snapshot, not an outcome: the fresh socket's replay may have
    // deleted a row it carries, so it fails its fetch (ADR-0023 D8).
    await expect(page).rejects.toThrow(/abandoned socket/)
    // ...but never the cursor; and its stream frames are dropped outright.
    expect(t.appliedCursor).toBe("50")
    emit({ t: "uptodate", seq: "102" }, 0)
    expect(t.appliedCursor).toBe("50")
    t.close()
  })

  it("unsubscribing while a subscribe's connect is in flight sends no ghost sub", async () => {
    // codex finding: the suspended subscribe() body would send its `sub` after
    // the open completes even though the handler is gone — the server persists
    // a subscription nothing consumes (ADR-0019) until the socket drops.
    const codec = createFrameCodec()
    let openNow!: (ws: WebSocketLike) => void
    const sent: Array<ClientFrame> = []
    const ws: WebSocketLike = {
      send: (data) => sent.push(codec.decode(data as ArrayBuffer | string) as ClientFrame),
      close: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
    }
    const t = new WebSocketTransport({
      url: "wss://fake",
      reconnectDelay: 1,
      open: () => new Promise<WebSocketLike>((r) => (openNow = r)),
    })
    const { handler } = recorder()
    const sub = t.subscribe("s1", "messages", handler) // parks on the pending open
    t.unsubscribe("s1") // no socket yet: nothing to send the unsub on
    openNow(ws)
    await sub
    expect(sent.filter((f) => f.t === "sub")).toEqual([])
    t.close()
  })
})
