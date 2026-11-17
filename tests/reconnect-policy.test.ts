import { env, runInDurableObject, SELF } from "cloudflare:test"
import { afterEach, describe, expect, it, vi } from "vitest"
import {
  defaultReconnectDelay,
  type SubHandler,
  WebSocketTransport,
  type WebSocketLike,
} from "../src/client/transport.ts"

// WHY (ADR-0016, issues #25/#26): the DO's client-readable auth-rejection
// pattern is accept-then-close with an application code (4xxx) — a failed HTTP
// handshake status is invisible to a browser WebSocket. A transport that
// blindly reconnects on ANY close turns that rejection into an infinite retry
// loop against the DO, and a fixed retry interval turns a DO outage into a
// thundering herd. So the reconnect delay is a POLICY function: the default
// treats 4000-4999 as terminal (surfaced via onClosed) and otherwise backs off
// exponentially with full jitter, resetting the attempt counter once a
// connection has proven healthy by staying open (amendment 2026-11-17).

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

async function openSocket(room: string): Promise<WebSocketLike> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws as unknown as WebSocketLike
}

function serverDrop(room: string, code: number, reason: string): Promise<unknown> {
  return runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, state) => {
    for (const sock of state.getWebSockets()) sock.close(code, reason)
  })
}

describe("defaultReconnectDelay policy", () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it("application close codes 4000-4999 are terminal (null); everything else retries", () => {
    const policy = defaultReconnectDelay(250)
    expect(policy(1, 4000)).toBeNull()
    expect(policy(1, 4403, "org access denied")).toBeNull()
    expect(policy(5, 4999)).toBeNull()
    expect(policy(1, 1000)).not.toBeNull() // normal closure: transient (hibernation, redeploy)
    expect(policy(1, 1006)).not.toBeNull() // abnormal drop: transient
    expect(policy(1)).not.toBeNull() // connect failure: no close frame at all
  })

  it("backs off exponentially from the base and caps at 30s", () => {
    vi.spyOn(Math, "random").mockReturnValue(1) // pin jitter at the ceiling
    const policy = defaultReconnectDelay(250)
    expect(policy(1)).toBe(250)
    expect(policy(2)).toBe(500)
    expect(policy(3)).toBe(1000)
    expect(policy(8)).toBe(30_000) // 250·2^7 = 32_000 → capped
    expect(policy(1000)).toBe(30_000) // huge attempt counts must not overflow past the cap
  })

  it("full jitter: the delay is uniform in [0, ceiling] — attempts desynchronize", () => {
    vi.spyOn(Math, "random").mockReturnValue(0)
    expect(defaultReconnectDelay(250)(3)).toBe(0)
    vi.spyOn(Math, "random").mockReturnValue(0.5)
    expect(defaultReconnectDelay(250)(3)).toBe(500)
  })

  it("a base above the default cap is not truncated below itself", () => {
    vi.spyOn(Math, "random").mockReturnValue(1)
    expect(defaultReconnectDelay(60_000)(1)).toBe(60_000)
  })
})

describe("transport reconnect policy (ADR-0016)", () => {
  it("a 4xxx application close is terminal: no reconnect; onClosed surfaces code+reason", async () => {
    const room = "rcpol-4403-terminal"
    let opens = 0
    const closed: Array<[number | undefined, string | undefined]> = []
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 5,
      onClosed: (code, reason) => closed.push([code, reason]),
      open: async () => {
        opens++
        return openSocket(room)
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    expect(opens).toBe(1)

    await serverDrop(room, 4403, "org access denied")
    await waitFor(() => closed.length === 1)
    // The app can distinguish this auth rejection from a transient drop.
    expect(closed[0]).toEqual([4403, "org access denied"])

    // Ample time for a (wrongly) scheduled reconnect to have fired.
    await new Promise((r) => setTimeout(r, 150))
    expect(opens).toBe(1)
  })

  it("a normal-closure drop (1000) still auto-reconnects; onClosed stays silent", async () => {
    const room = "rcpol-1000-transient"
    let opens = 0
    const closed: Array<[number | undefined, string | undefined]> = []
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 5,
      onClosed: (code, reason) => closed.push([code, reason]),
      open: async () => {
        opens++
        return openSocket(room)
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    await serverDrop(room, 1000, "drop")
    await waitFor(() => opens >= 2)
    expect(closed.length).toBe(0)
    t.close()
  })

  it("close() suppresses auto-reconnect until a later connect() REVIVES the transport (#37)", async () => {
    // Contract change (issue #37): close() suppresses auto-reconnect, but a
    // later connect() is an explicit statement of intent that CLEARS the latch.
    // A revived transport (connection pools do this) must auto-reconnect again —
    // the pre-fix latch never reset, silencing reconnect forever on any revived
    // transport. ADR-0016's `intentionallyClosed` still governs; only its
    // clearing point moves (from "never" to "on the next connect()").
    const room = "rcpol-close-revive"
    let opens = 0
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 5,
      open: async () => {
        opens++
        return openSocket(room)
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    expect(opens).toBe(1)

    // While closed, a drop must NOT reconnect (the latch is set).
    t.close()
    // Revive: dialing clears the latch, so a subsequent unexpected drop DOES
    // auto-reconnect on this same instance.
    await t.connect()
    expect(opens).toBe(2)
    await serverDrop(room, 1000, "drop")
    await waitFor(() => opens >= 3) // auto-reconnect restored by the revive
    t.close()
  })
})

// --- Fake-socket tests: deterministic attempt-counter semantics ------------

interface Fake {
  ws: WebSocketLike
  emit: (type: string, ev: object) => void
}

function makeFake(): Fake {
  const listeners = new Map<string, Array<(ev: object) => void>>()
  return {
    emit: (type, ev) => {
      for (const l of listeners.get(type) ?? []) l(ev)
    },
    ws: {
      send: () => {},
      close: () => {},
      addEventListener: (type, l) => {
        const arr = listeners.get(type) ?? []
        arr.push(l as (ev: object) => void)
        listeners.set(type, arr)
      },
      removeEventListener: () => {},
    },
  }
}

describe("reconnect attempt counter (drives the backoff)", () => {
  it("grows across failed opens and young closes; resets after a connection proves healthy", async () => {
    const attempts: Array<number> = []
    const fakes: Array<Fake> = []
    let failNext = 0
    const t = new WebSocketTransport({
      url: "wss://fake-backoff",
      // Deterministic policy: record the attempt number the transport passes,
      // retry almost immediately so the test never sleeps for real backoff.
      reconnectDelay: (attempt) => {
        attempts.push(attempt)
        return 1
      },
      open: async () => {
        if (failNext > 0) {
          failNext--
          throw new Error("server unreachable")
        }
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    expect(fakes.length).toBe(1)

    // Outage: the next two open() attempts fail, the third succeeds. The
    // policy must see a GROWING attempt number across the failures — this is
    // what makes the default backoff exponential rather than fixed-interval.
    failNext = 2
    fakes[0]!.emit("close", { code: 1006 })
    await waitFor(() => fakes.length === 2)
    expect(attempts).toEqual([1, 2, 3])

    // A successful open is not proof of health (ADR-0016 §3, amendment
    // 2026-11-17): a socket that closes soon after it opened keeps growing the
    // count, or a server that accepts and then closes holds every client at
    // attempt 1 forever.
    fakes[1]!.emit("close", { code: 1006 })
    await waitFor(() => fakes.length === 3)
    expect(attempts).toEqual([1, 2, 3, 4])

    // A drop after the connection stayed open 5 s starts over at attempt 1,
    // otherwise a long-lived client would pay maximum backoff for every later
    // blip. Shift the clock so the open socket looks 5 s old.
    const realNow = Date.now.bind(Date)
    const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + 5_000)
    try {
      fakes[2]!.emit("close", { code: 1006 })
      await waitFor(() => fakes.length === 4)
    } finally {
      clock.mockRestore()
    }
    expect(attempts).toEqual([1, 2, 3, 4, 1])
    t.close()
  })

  it("a custom policy returning null stops retrying; onClosed carries no code for a connect failure", async () => {
    const closed: Array<[number | undefined, string | undefined]> = []
    const fakes: Array<Fake> = []
    let opens = 0
    let failOpens = false
    const t = new WebSocketTransport({
      url: "wss://fake-stop",
      reconnectDelay: (attempt) => (attempt >= 2 ? null : 1),
      onClosed: (code, reason) => closed.push([code, reason]),
      open: async () => {
        opens++
        if (failOpens) throw new Error("server unreachable")
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    expect(opens).toBe(1)

    // Drop, then the reconnect attempt fails (attempt 1 → retry), and the
    // policy gives up on attempt 2. The connect-failure path has no close
    // frame, so onClosed reports (undefined, undefined) — distinguishable
    // from a server-sent application close.
    failOpens = true
    fakes[0]!.emit("close", { code: 1006, reason: "abnormal" })
    await waitFor(() => closed.length === 1)
    expect(closed[0]).toEqual([undefined, undefined])

    const opensAtStop = opens
    await new Promise((r) => setTimeout(r, 100))
    expect(opens).toBe(opensAtStop) // no further attempts after the policy said stop
    t.close()
  })

  it("a stale transient-drop timer cannot resurrect the transport after a terminal close", async () => {
    // Codex-review scenario: transient drop arms a delayed retry; a
    // demand-driven connect() beats the timer, and THAT socket is closed
    // terminally (4403). "Terminal means stop" must win — the earlier timer
    // must not fire a further attempt (or a duplicate onClosed) later.
    const closed: Array<[number | undefined, string | undefined]> = []
    const fakes: Array<Fake> = []
    let opens = 0
    const t = new WebSocketTransport({
      url: "wss://fake-stale-timer",
      reconnectDelay: (_attempt, code) => (code !== undefined && code >= 4000 && code <= 4999 ? null : 50),
      onClosed: (code, reason) => closed.push([code, reason]),
      open: async () => {
        opens++
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())

    fakes[0]!.emit("close", { code: 1006 }) // transient: retry timer armed for +50ms
    await t.connect() // demand-driven connect wins the race
    expect(opens).toBe(2)
    fakes[1]!.emit("close", { code: 4403, reason: "forbidden" }) // terminal
    await waitFor(() => closed.length === 1)

    await new Promise((r) => setTimeout(r, 150)) // well past the stale timer
    expect(opens).toBe(2) // the stale timer did not reconnect…
    expect(closed).toEqual([[4403, "forbidden"]]) // …and onClosed fired exactly once
    t.close()
  })

  it("close() during the reconnect window cancels the pending retry", async () => {
    const fakes: Array<Fake> = []
    let opens = 0
    const t = new WebSocketTransport({
      url: "wss://fake-close-cancels",
      reconnectDelay: () => 30,
      open: async () => {
        opens++
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    fakes[0]!.emit("close", { code: 1006 }) // retry armed for +30ms
    t.close() // the app shut the transport down before the timer fired

    await new Promise((r) => setTimeout(r, 100))
    expect(opens).toBe(1) // an intentional close() leaves nothing running
  })

  it("a stale socket's late close event cannot detach the live socket", async () => {
    // close() then an immediate connect(): the OLD socket's close event can be
    // delivered after the new socket is installed. It must be ignored —
    // otherwise it nulls the live connection and the next demand-driven
    // connect() opens a needless third socket.
    const fakes: Array<Fake> = []
    let opens = 0
    const t = new WebSocketTransport({
      url: "wss://fake-stale-close",
      reconnectDelay: () => 1,
      open: async () => {
        opens++
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    t.close() // old socket told to close; its event is not delivered yet
    await t.connect() // fresh socket on the same instance
    expect(opens).toBe(2)

    fakes[0]!.emit("close", { code: 1005 }) // the old socket's close finally lands
    await t.connect() // must be a no-op: the live socket is still attached
    expect(opens).toBe(2)
    t.close()
  })

  it("close() during an in-flight reconnect open() cannot resurrect a socket after teardown", async () => {
    // Post-PR codex adversarial pass: a transient drop arms the retry timer;
    // the timer fires and open() is SLOW (TLS / Worker cold start). The app
    // tears down with close() while that open() is still pending — close()
    // can cancel the timer but not the already-running connect() body. When
    // open() finally resolves, the socket must NOT be installed: otherwise a
    // live, message-handling, resubscribed socket outlives teardown and
    // "close() is permanent" is a lie. The orphan must be closed instead.
    const fakes: Array<Fake> = []
    let opens = 0
    let releaseOpen: ((ws: WebSocketLike) => void) | null = null
    const t = new WebSocketTransport({
      url: "wss://fake-teardown-race",
      reconnectDelay: () => 10,
      open: async () => {
        opens++
        const f = makeFake()
        fakes.push(f)
        if (opens === 1) return f.ws
        // The reconnect attempt: hold open() unresolved until the test says so.
        return new Promise<WebSocketLike>((resolve) => {
          releaseOpen = resolve
        })
      },
    })
    await t.subscribe("s1", "messages", noopHandler())

    fakes[0]!.emit("close", { code: 1006 }) // transient: retry armed for +10ms
    await waitFor(() => releaseOpen !== null) // timer fired; open() is in flight

    let orphanClosed = false
    let orphanFrames = 0
    const orphan = fakes[1]!
    orphan.ws.close = () => {
      orphanClosed = true
    }
    orphan.ws.send = () => {
      orphanFrames++
    }

    t.close() // teardown while the reconnect open() is still pending
    releaseOpen!(orphan.ws) // the slow open finally resolves, after teardown
    await waitFor(() => orphanClosed) // the orphan socket is closed, not installed
    expect(orphanFrames).toBe(0) // and never resubscribed to the server

    await new Promise((r) => setTimeout(r, 50))
    expect(opens).toBe(2) // nothing kept retrying after close()
  })
})
