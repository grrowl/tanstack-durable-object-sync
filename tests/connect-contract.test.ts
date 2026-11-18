import { describe, expect, it } from "vitest"
import {
  type SubHandler,
  TransportClosedError,
  WebSocketTransport,
  type WebSocketLike,
} from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame } from "../src/wire/frames.ts"

// WHY (issues #37 / #38, ADR-0020): the connect/close boundary is the client's
// most race-prone seam. Two defects live in the same in-flight-open window:
//
//   #37 — connect() could RESOLVE having adopted no socket (the epoch-discard
//         `return`). Its caller (subscribe/sendMut/fetch) then sent on a null
//         socket and threw, floating an unhandled rejection and leaving the
//         collection silently empty. And `intentionallyClosed` latched forever:
//         a transport revived by a later connect() (connection pools do this)
//         lost auto-reconnect AND onClosed delivery.
//
//   #38 — close() could not abort a socket whose open() was still in flight:
//         `this.ws` was null, so `this.ws?.close()` was a no-op, and a handshake
//         that never resolved leaked a CONNECTING socket forever.
//
// The contract these pin: connect() never resolves disconnected (it re-dials
// under revival or rejects TransportClosedError), dialing clears the latch, and
// close() aborts an in-flight handshake via the AbortSignal handed to open().

const codec = createFrameCodec()

function noopHandler(): SubHandler {
  return { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} }
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

interface Fake {
  ws: WebSocketLike
  sent: Array<ClientFrame>
  emit: (type: string, ev: { data?: unknown; code?: number; reason?: string }) => void
}

function makeFake(): Fake {
  const listeners = new Map<string, Array<(ev: { data?: unknown }) => void>>()
  const fake: Fake = {
    sent: [],
    emit: (type, ev) => {
      for (const l of listeners.get(type) ?? []) l(ev)
    },
    ws: {
      send: (data) => fake.sent.push(codec.decode(data as ArrayBuffer | string) as ClientFrame),
      close: () => {},
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

describe("connect() contract — never resolves disconnected (#37)", () => {
  it("subscribe before the handshake completes still lands the sub frame once connected", async () => {
    const fake = makeFake()
    let release: (() => void) | null = null
    const t = new WebSocketTransport({
      url: "wss://x",
      open: () => new Promise<WebSocketLike>((res) => (release = () => res(fake.ws))),
    })

    const sub = t.subscribe("s1", "messages", noopHandler())
    await waitFor(() => release !== null)
    // Handshake still in flight: no frame has escaped.
    expect(fake.sent.length).toBe(0)

    release!() // the socket finally opens
    await sub
    // The frame LANDS on the socket that finally opened — not thrown into the void.
    expect(fake.sent.some((f) => f.t === "sub" && (f as { subId?: string }).subId === "s1")).toBe(true)
    t.close()
  })

  it("close() during the handshake rejects the in-flight connect() typed — never resolves disconnected", async () => {
    const fake = makeFake()
    let aborted = false
    let resolved = false
    const t = new WebSocketTransport({
      url: "wss://x",
      // A never-resolving open that HONOURS the abort signal (the documented
      // contract): close() must be able to tear it down.
      open: (signal) =>
        new Promise<WebSocketLike>((_res, rej) => {
          signal?.addEventListener(
            "abort",
            () => {
              aborted = true
              rej(new TransportClosedError())
            },
            { once: true },
          )
        }),
    })

    const c = t.connect()
    const outcome = c.then(
      () => {
        resolved = true
        return "resolved"
      },
      (e) => e,
    )
    t.close() // tears down the in-flight handshake
    const r = await outcome
    expect(aborted).toBe(true)
    expect(resolved).toBe(false) // it did NOT resolve disconnected…
    expect(r).toBeInstanceOf(TransportClosedError) // …it rejected typed
    void fake
  })

  it("close() during the first subscribe's handshake, then a fresh connect() revives and lands the sub", async () => {
    const fakes: Array<Fake> = []
    const releases: Array<(() => void) | undefined> = []
    const t = new WebSocketTransport({
      url: "wss://x",
      open: (signal) =>
        new Promise<WebSocketLike>((res, rej) => {
          const f = makeFake()
          fakes.push(f)
          const idx = fakes.length - 1
          releases[idx] = () => res(f.ws)
          signal?.addEventListener("abort", () => rej(new TransportClosedError()), { once: true })
        }),
    })

    const sub1 = t.subscribe("s1", "messages", noopHandler())
    const sub1Outcome = sub1.then(() => "ok", (e) => e) // capture the rejection — no unhandled
    await waitFor(() => fakes.length === 1)

    t.close() // Aborts the handshake. The subscription belongs to its owner.
    let settled = false
    void sub1Outcome.then(() => (settled = true))
    await new Promise((r) => setTimeout(r, 0))
    expect(settled).toBe(false)

    // Revive: a fresh subscribe re-dials. BOTH frames must land on the NEW socket.
    const sub2 = t.subscribe("s2", "messages", noopHandler())
    await waitFor(() => releases[1] !== undefined)
    releases[1]!()
    await sub2
    expect(await sub1Outcome).toBe("ok")
    const subs = fakes[1]!.sent.filter((f) => f.t === "sub").map((f) => (f as { subId?: string }).subId)
    expect(subs).toEqual(["s1", "s2"])
    t.close()
  })
})

describe("revived transport — dialing clears the intentional-close latch (#37)", () => {
  it("auto-reconnects on a 1006 drop after a connect() revives a closed transport", async () => {
    const fakes: Array<Fake> = []
    let opens = 0
    const t = new WebSocketTransport({
      url: "wss://x",
      reconnectDelay: () => 5,
      open: () => {
        opens++
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    expect(opens).toBe(1)

    t.close()
    await t.connect() // revive: dialing is intent, the latch must clear
    expect(opens).toBe(2)

    // An unexpected drop on the revived socket MUST auto-reconnect now — the
    // pre-fix latch silenced this forever.
    fakes[1]!.emit("close", { code: 1006 })
    await waitFor(() => opens >= 3)
    t.close()
  })

  it("delivers onClosed on a terminal 4xxx close after a connect() revives a closed transport", async () => {
    const fakes: Array<Fake> = []
    const closed: Array<[number | undefined, string | undefined]> = []
    const t = new WebSocketTransport({
      url: "wss://x",
      reconnectDelay: 5,
      onClosed: (code, reason) => closed.push([code, reason]),
      open: () => {
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())

    t.close()
    await t.connect() // revive: onClosed delivery must be restored too
    fakes[1]!.emit("close", { code: 4403, reason: "removed from this workspace" })
    await waitFor(() => closed.length === 1)
    expect(closed[0]).toEqual([4403, "removed from this workspace"])
    t.close()
  })
})

describe("close() aborts an in-flight handshake — no leaked CONNECTING socket (#38)", () => {
  it("close() during a never-resolving open() aborts (closes) the still-CONNECTING socket", async () => {
    let socketClosed = false
    let opened = false
    const t = new WebSocketTransport({
      url: "wss://x",
      open: (signal) =>
        new Promise<WebSocketLike>((_res, rej) => {
          opened = true
          const ws: WebSocketLike = {
            send: () => {},
            close: () => {
              socketClosed = true
            },
            addEventListener: () => {},
            removeEventListener: () => {},
          }
          // The default browser open() closes its socket on abort; a custom one
          // that honours the signal does the same. This never resolves — only
          // the abort can free the socket.
          signal?.addEventListener(
            "abort",
            () => {
              ws.close()
              rej(new TransportClosedError())
            },
            { once: true },
          )
        }),
    })

    const c = t.connect()
    const outcome = c.then(() => "ok", (e) => e)
    await waitFor(() => opened)
    t.close()
    expect(socketClosed).toBe(true) // the CONNECTING socket was aborted, not leaked
    expect(await outcome).toBeInstanceOf(TransportClosedError)
    t.close()
  })

  it("a custom open() that IGNORES the signal still has its late socket discarded and closed (backward-compat)", async () => {
    let socketClosed = false
    let release: (() => void) | null = null
    const t = new WebSocketTransport({
      url: "wss://x",
      // Legacy open contract: takes no signal, resolves whenever. close() cannot
      // abort it mid-flight, but the epoch-discard must still close the orphan
      // the moment open() resolves — no socket installed, no leak.
      open: () =>
        new Promise<WebSocketLike>((res) => {
          const ws: WebSocketLike = {
            send: () => {},
            close: () => {
              socketClosed = true
            },
            addEventListener: () => {},
            removeEventListener: () => {},
          }
          release = () => res(ws)
        }),
    })

    const c = t.connect()
    const outcome = c.then(() => "ok", (e) => e)
    await waitFor(() => release !== null)
    t.close() // cannot abort a signal-ignoring open…
    release!() // …but when it resolves, the orphan is closed and connect() rejects
    await waitFor(() => socketClosed)
    expect(await outcome).toBeInstanceOf(TransportClosedError)
    t.close()
  })
})

describe("read settlement across sockets (ADR-0029)", () => {
  it("a terminal dial or policy exception settles every unsent read with the original cause", async () => {
    for (const throws of [false, true]) {
      const dialError = new Error("offline")
      const policyError = new Error("policy failed")
      const t = new WebSocketTransport({
        url: "wss://x",
        open: () => { throw dialError },
        reconnectDelay: () => {
          if (throws) throw policyError
          return null
        },
      })
      const results = await Promise.allSettled([
        t.subscribe("s1", "messages", noopHandler()),
        t.fetch({ t: "fetch", fetchId: "page", collection: "messages" }),
      ])
      for (const result of results) {
        expect(result.status).toBe("rejected")
        if (result.status === "rejected") {
          expect(result.reason.name).toBe("TransportStoppedError")
          expect(result.reason.cause).toBe(throws ? policyError : dialError)
        }
      }
      t.unsubscribe("s1")
      t.close()
    }
  })

  it("failed writes during an armed retry leave its policy budget for the pending read", async () => {
    const fake = makeFake()
    const attempts: number[] = []
    let offline = true
    const t = new WebSocketTransport({
      url: "wss://x",
      open: () => {
        if (offline) throw new Error("offline")
        return fake.ws
      },
      reconnectDelay: attempt => {
        attempts.push(attempt)
        return attempt === 1 ? 60_000 : null
      },
    })
    const sub = t.subscribe("s1", "messages", noopHandler())
    await waitFor(() => attempts.length === 1)
    for (let i = 0; i < 3; i++) {
      await expect(t.sendMut({ t: "mut", collection: "messages", txId: String(i), ops: [] })).rejects.toThrow("offline")
    }
    expect(attempts).toEqual([1])
    offline = false
    await t.connect()
    await sub
    expect(fake.sent.map(f => f.t)).toEqual(["sub"])
    t.close()
  })

  it("a page replays after its watch, ignores a late answer, and rejects on a policy stop", async () => {
    const sockets: Fake[] = []
    const t = new WebSocketTransport({
      url: "wss://x", reconnectDelay: (_attempt, code) => code === 4403 ? null : 60_000,
      open: () => {
        const fake = makeFake()
        sockets.push(fake)
        return fake.ws
      },
    })
    await t.subscribe("watch", "messages", noopHandler())
    const frame = { t: "fetch", fetchId: "page", collection: "messages" } as const
    const page = t.fetch(frame)
    sockets[0]!.emit("close", { code: 1006 })
    await t.connect()
    expect(sockets[1]!.sent.map(f => f.t)).toEqual(["sub", "fetch"])
    const answer = (value: string) => ({ data: codec.encode({ t: "page", fetchId: "page", rows: [value], seq: "0" }) })
    sockets[0]!.emit("message", answer("stale"))
    sockets[1]!.emit("message", answer("fresh"))
    expect(await page).toEqual(["fresh"])

    const denied = t.fetch(frame)
    sockets[1]!.emit("close", { code: 4403, reason: "forbidden" })
    await expect(denied).rejects.toMatchObject({ name: "TransportStoppedError", closeCode: 4403, closeReason: "forbidden" })
    t.close()
  })

  it("a subscription send that throws abandons the socket and the next install finishes the batch once", async () => {
    const broken = makeFake()
    const working = makeFake()
    let opens = 0
    broken.ws.send = () => { throw new Error("send failed") }
    const t = new WebSocketTransport({
      url: "wss://x", open: () => (++opens === 1 ? broken : working).ws, reconnectDelay: () => 0,
    })
    await Promise.all(["a", "b"].map(id => t.subscribe(id, "messages", noopHandler())))
    expect(opens).toBe(2)
    expect(working.sent.map(f => f.t === "sub" && f.subId)).toEqual(["a", "b"])
    t.close()
  })
})
