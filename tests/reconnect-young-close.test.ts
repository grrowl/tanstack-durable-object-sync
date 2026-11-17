import { env, runInDurableObject, SELF } from "cloudflare:test"
import { afterEach, describe, expect, it, vi } from "vitest"
import {
  defaultReconnectDelay,
  type SubHandler,
  WebSocketTransport,
  type WebSocketLike,
} from "../src/client/transport.ts"

// WHY (ADR-0016 §3, amendment 2026-11-17): the reconnect attempt counter used
// to reset on every successful open. A server that accepts a socket and then
// closes it soon after (a handler that fails on every resubscribe, a DO that
// throws on its first frame after a deploy) therefore kept every client at
// attempt 1: a delay of 0 to 250 ms, forever, with every subscription resent
// each time. The backoff must grow across connections that die young, and
// reset only once a connection has proven healthy by staying open long enough.
// When auto-reconnect ends (a terminal stop, close(), or the last
// unsubscribe), the next run starts again at attempt 1.

/** How long a connection must stay open to count as healthy (ADR-0016 §3). */
const HEALTHY_OPEN_MS = 30_000

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

function noopHandler(): SubHandler {
  return { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} }
}

/** Make every later Date.now() read `ms` further on, so a socket opened before
 *  this call looks `ms` older when it closes. Call it BEFORE any waitFor whose
 *  start time must agree with the shifted clock. */
function ageOpenSocketsBy(ms: number): void {
  const realNow = Date.now.bind(Date)
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + ms)
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe("a server that accepts, answers, then closes every time (real DO)", () => {
  it("the attempt number grows across the loop, and dials in a fixed window stay bounded", async () => {
    const room = "rcyoung-loop"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    // The server answers the subscription (snapshot or catch-up), then closes
    // with a non-terminal 1011, the shape of a handler that fails on every
    // resubscribe. Each answer triggers the close, so every connection dies
    // young and the loop never ends on its own.
    const closeAfterAnswer = (): void => {
      void runInDurableObject(stub, (_i, state) => {
        for (const sock of state.getWebSockets()) sock.close(1011, "handler failed")
      })
    }
    const answered: SubHandler = {
      onSnap: () => {},
      onSnapEnd: closeAfterAnswer,
      onDelta: () => {},
      onUptodate: closeAfterAnswer,
      onReset: closeAfterAnswer,
    }
    const attempts: Array<number> = []
    let opens = 0
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      // Exponential but tiny, so the test runs in well under a second. Under
      // the old reset-on-open rule this is a fixed 2 ms delay, forever.
      reconnectDelay: (attempt) => {
        attempts.push(attempt)
        return 2 ** attempt
      },
      open: async () => {
        opens++
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        return ws as unknown as WebSocketLike
      },
    })
    await t.subscribe("s1", "messages", answered)

    // A fixed window. With growth the delays are 2, 4, 8, … 256 ms, so about
    // eight dials fit; with the counter stuck at 1 the client dials every few
    // milliseconds.
    await new Promise((r) => setTimeout(r, 500))
    const opensInWindow = opens
    const attemptsInWindow = [...attempts]
    t.close()

    expect(opensInWindow).toBeLessThanOrEqual(10)
    expect(attemptsInWindow.length).toBeGreaterThanOrEqual(3) // the loop really ran
    expect(attemptsInWindow).toEqual(attemptsInWindow.map((_a, i) => i + 1)) // 1, 2, 3, …
  })
})

// --- Fake-socket tests: deterministic counter semantics ----------------------

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

/** A transport over fake sockets whose policy records each attempt number. */
function fakeTransport(policy: (attempt: number, code?: number) => number | null) {
  const attempts: Array<number> = []
  const fakes: Array<Fake> = []
  const closed: Array<number | undefined> = []
  const t = new WebSocketTransport({
    url: "wss://fake-young-close",
    reconnectDelay: (attempt, code) => {
      attempts.push(attempt)
      return policy(attempt, code)
    },
    onClosed: (code) => closed.push(code),
    open: async () => {
      const f = makeFake()
      fakes.push(f)
      return f.ws
    },
  })
  /** Close the newest socket young and wait for its replacement. */
  const dropYoung = async (code = 1011): Promise<void> => {
    const n = fakes.length
    fakes[n - 1]!.emit("close", { code })
    await waitFor(() => fakes.length === n + 1)
  }
  return { t, attempts, fakes, closed, dropYoung }
}

describe("what proves a connection healthy (ADR-0016 §3)", () => {
  it("a connection that stayed open long enough, then drops once, retries at the first-attempt delay", async () => {
    // The default policy with jitter pinned at its ceiling, so the delay the
    // transport would wait is exactly readable: base · 2^(attempt − 1).
    vi.spyOn(Math, "random").mockReturnValue(1)
    const policy = defaultReconnectDelay(5)
    const delays: Array<number | null> = []
    const { t, fakes, dropYoung } = fakeTransport((attempt, code) => {
      const d = policy(attempt, code)
      delays.push(d)
      return d
    })
    await t.subscribe("s1", "messages", noopHandler())

    // Two young deaths build up the backoff: 5 ms, then 10 ms.
    await dropYoung()
    await dropYoung()
    expect(delays).toEqual([5, 10])

    // The third connection stays healthy, then one blip. It must retry at the
    // first-attempt delay again, not pay the accumulated backoff. Without the
    // reset a long-lived client would wait out the cap after any old outage.
    ageOpenSocketsBy(HEALTHY_OPEN_MS)
    fakes[2]!.emit("close", { code: 1006 })
    await waitFor(() => fakes.length === 4)
    expect(delays).toEqual([5, 10, 5])
    t.close()
  })

  it("a connection one moment short of the threshold does not reset the backoff", async () => {
    // Pins the threshold itself: a rule that resets a little earlier (say on
    // the first frame, or after a few seconds) lets a server that closes later
    // than that loop at the first-attempt delay.
    const { t, attempts, fakes, dropYoung } = fakeTransport(() => 1)
    await t.subscribe("s1", "messages", noopHandler())
    await dropYoung()
    ageOpenSocketsBy(HEALTHY_OPEN_MS - 1_000)
    fakes[1]!.emit("close", { code: 1011 })
    await waitFor(() => fakes.length === 3)
    expect(attempts).toEqual([1, 2])
    t.close()
  })
})

describe("the next run of auto-reconnect starts at attempt 1", () => {
  it("after a terminal stop, an app-driven connect() gets the policy's full budget again", async () => {
    // A policy that gives up on attempt 3. Young deaths reach it; onClosed
    // fires; the app reconnects (e.g. after re-auth). If the counter carried
    // over, the first young death of the new run would be attempt 4 and the
    // policy would give up at once.
    const { t, attempts, fakes, closed, dropYoung } = fakeTransport((attempt) => (attempt >= 3 ? null : 1))
    await t.subscribe("s1", "messages", noopHandler())
    await dropYoung()
    await dropYoung()
    fakes[2]!.emit("close", { code: 1011 })
    await waitFor(() => closed.length === 1)
    expect(attempts).toEqual([1, 2, 3])

    await t.connect()
    expect(fakes.length).toBe(4)
    await dropYoung()
    expect(attempts).toEqual([1, 2, 3, 1])
    t.close()
  })

  it("after close(), a revived transport (ADR-0020) starts from attempt 1", async () => {
    const { t, attempts, dropYoung } = fakeTransport(() => 1)
    await t.subscribe("s1", "messages", noopHandler())
    await dropYoung()
    await dropYoung()
    expect(attempts).toEqual([1, 2])

    t.close()
    await t.connect()
    await dropYoung()
    expect(attempts).toEqual([1, 2, 1])
    t.close()
  })

  it("after the last unsubscribe ends a pending reconnect, the next subscribe starts from attempt 1", async () => {
    const { t, attempts, fakes, dropYoung } = fakeTransport(() => 30)
    await t.subscribe("s1", "messages", noopHandler())
    await dropYoung()
    expect(attempts).toEqual([1])

    // Drop again, and unsubscribe before the 30 ms timer fires: ADR-0016 does
    // not reconnect for zero subscriptions, so this run of retries is over.
    fakes[1]!.emit("close", { code: 1011 })
    expect(attempts).toEqual([1, 2])
    t.unsubscribe("s1")

    await t.subscribe("s2", "messages", noopHandler())
    expect(fakes.length).toBe(3)
    await dropYoung()
    expect(attempts).toEqual([1, 2, 1])
    t.close()
  })
})
