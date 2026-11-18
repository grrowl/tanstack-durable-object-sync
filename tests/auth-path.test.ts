// Auth on the sync upgrade path — where each kind of rejection surfaces.
//
// WHY: auth has exactly two client-visible surfaces, and confusing them breaks
// real apps. (1) `parseAttachment` runs on the HTTP upgrade: a thrown Response
// is returned verbatim, any other throw is HTTP 401 — a browser WebSocket
// cannot read those statuses, so this surface is for trusted-header setups
// behind a Worker. (2) Client-readable rejection is accept-then-close with an
// application code (4403): a subclass fetch override can do async authz BEFORE
// super.fetch without touching sync socket bookkeeping, and the transport must
// treat that close as TERMINAL (no retry loop — issue #25, ADR-0016). Also
// pins that a bare SyncDurableObject claims the upgrade on ANY path, so
// Workers need no URL rewrite.

import { CollectionImpl, createCollection, createLiveQueryCollection } from "@tanstack/db"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import type { TestApi } from "./test-worker.ts"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { TransportStoppedError, WebSocketTransport, type SubHandler, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

const codec = createFrameCodec()

async function upgrade(path: string, headers: Record<string, string> = {}): Promise<Response> {
  return SELF.fetch(`https://example.com${path}`, {
    headers: { Upgrade: "websocket", ...headers },
  })
}

async function openWs(path: string, headers: Record<string, string> = {}): Promise<WebSocket> {
  const res = await upgrade(path, headers)
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 2000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      const f = codec.decode(e.data as ArrayBuffer) as ServerFrame
      out.push(f)
      if (done(f)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

/** Installs a fetch override in the same dispatch position a subclass's
 *  `async fetch()` would occupy (own property beats the mixin prototype):
 *  async authz that rejects non-alice upgrades with accept-then-close(4403)
 *  BEFORE super.fetch — the rejected socket never reaches ctx.acceptWebSocket. */
function installAuthzOverride(room: string): Promise<unknown> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
  return runInDurableObject(stub, (instance) => {
    const inst = instance as unknown as { fetch: (req: Request) => Promise<Response> }
    const superFetch = inst.fetch.bind(instance)
    inst.fetch = async (req: Request): Promise<Response> => {
      if (req.headers.get("Upgrade") === "websocket") {
        await new Promise((r) => setTimeout(r, 5)) // genuine async authz hop
        if (req.headers.get("x-user") !== "alice") {
          const pair = new WebSocketPair()
          pair[1].accept() // plain accept — NOT ctx.acceptWebSocket, no SYNC_TAG
          pair[1].close(4403, "org access denied")
          return new Response(null, { status: 101, webSocket: pair[0] })
        }
      }
      return superFetch(req)
    }
  })
}

describe("auth path: upgrade statuses and application close codes", () => {
  it("bare SyncDurableObject accepts the upgrade on ANY path — no URL rewrite needed", async () => {
    // The Worker forwards the ORIGINAL request; the DO sees /sync/sessions/abc123
    // (nothing like /_sync) and still upgrades + syncs.
    const ws = await openWs("/sync/sessions/abc123", { "x-user": "alice" })
    ws.send(codec.encode({ t: "sub", subId: "s1", collection: "messages" } satisfies ClientFrame))
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")
    expect(frames.at(-1)!.t).toBe("snap-end")
    ws.close()
  })

  it("parseAttachment throw -> HTTP status on the upgrade response, not a WS close code", async () => {
    const room = "authpath-parse-throw"
    // Prime the instance, then swap the auth hook via the public sync facade —
    // exactly what a subclass's parseAttachment override feeds through.
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (instance) => {
      ;(instance as unknown as { sync: { configure: (o: object) => void } }).sync.configure({
        parseAttachment: (req: Request) => {
          const u = req.headers.get("x-user")
          if (u === "response-throw") throw new Response("forbidden", { status: 403 })
          if (u === "error-throw") throw new Error("nope")
          return { userId: u ?? "anon" }
        },
      })
    })

    const resp403 = await upgrade(`/sync/${room}`, { "x-user": "response-throw" })
    expect(resp403.status).toBe(403)
    expect(resp403.webSocket).toBeNull()

    const resp401 = await upgrade(`/sync/${room}`, { "x-user": "error-throw" })
    expect(resp401.status).toBe(401)
    expect(await resp401.text()).toBe("unauthorized")
    expect(resp401.webSocket).toBeNull()

    // parseAttachment is upgrade-only: a plain GET never reaches it (426 first).
    const plain = await SELF.fetch(`https://example.com/sync/${room}`, {
      headers: { "x-user": "response-throw" },
    })
    expect(plain.status).toBe(426)
  })

  it("accept-then-close(4403) before super.fetch: client sees 4403; later authorized connect syncs", async () => {
    const room = "authpath-4403"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await installAuthzOverride(room)

    // Unauthorized connect: handshake succeeds, then the app close code arrives.
    const badRes = await upgrade(`/sync/${room}`, { "x-user": "intruder" })
    expect(badRes.status).toBe(101)
    const badWs = badRes.webSocket!
    const closed = new Promise<{ code: number; reason: string }>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("no close event")), 2000)
      badWs.addEventListener("close", (e: CloseEvent) => {
        clearTimeout(timer)
        resolve({ code: e.code, reason: e.reason })
      })
    })
    badWs.accept()
    const closeEvt = await closed
    expect(closeEvt.code).toBe(4403)
    expect(closeEvt.reason).toBe("org access denied")

    // Sync socket bookkeeping untouched: no hibernatable socket was registered.
    await runInDurableObject(stub, (_i, state) => {
      expect(state.getWebSockets().length).toBe(0)
    })

    // Authorized connect on the SAME instance still syncs end to end.
    const ws = await openWs(`/sync/${room}`, { "x-user": "alice" })
    ws.send(codec.encode({ t: "sub", subId: "s1", collection: "messages" } satisfies ClientFrame))
    await collectUntil(ws, (f) => f.t === "snap-end")

    const mut: ClientFrame = {
      t: "mut",
      txId: "tx-authpath-1",
      collection: "messages",
      ops: [{ type: "insert", key: "m1", cols: { id: "m1", body: "hello" } }],
    }
    ws.send(codec.encode(mut))
    const frames = await collectUntil(ws, (f) => f.t === "committed")
    expect(frames.some((f) => f.t === "d" && f.op === "insert" && f.key === "m1")).toBe(true)

    // Exactly one hibernatable (tagged) sync socket exists now.
    await runInDurableObject(stub, (_i, state) => {
      expect(state.getWebSockets().length).toBe(1)
      expect(state.getWebSockets()[0]!.deserializeAttachment()).toEqual({ userId: "alice" })
    })
    ws.close()
  })

  for (const refreshable of [false, true]) {
    it(`pending eager and subset loads follow the auth policy (refreshable=${refreshable})`, async () => {
      const room = `auth-first-load-${crypto.randomUUID()}`
      await installAuthzOverride(room)
      await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, state) => {
        state.storage.sql.exec("INSERT INTO messages(id,body) VALUES('seed','hello')")
      })
      const codes: number[] = []
      const users: string[] = []
      let user = "intruder"
      let refreshed: Promise<string> | undefined
      const t = new WebSocketTransport<TestApi>({
        url: "unused",
        open: async () => {
          if (refreshed) user = await refreshed
          users.push(user)
          const response = await upgrade(`/sync/${room}`, { "x-user": user })
          const ws = response.webSocket!
          ws.accept()
          return ws as unknown as WebSocketLike
        },
        reconnectDelay: (_attempt, code) => {
          codes.push(code!)
          if (!refreshable) return null
          refreshed = new Promise(resolve => setTimeout(() => resolve("alice"), 5))
          return 0
        },
      })
      const eager = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: m => m.id }))
      const source = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: m => m.id, syncMode: "on-demand" }))
      const query = createLiveQueryCollection(q => q.from({ m: source }))
      const loads = await Promise.allSettled([eager.preload(), query.preload()])
      expect(codes).toEqual([4403])
      expect(source.status).toBe("ready")
      if (refreshable) {
        expect(users).toEqual(["intruder", "alice"])
        expect(loads.map(l => l.status)).toEqual(["fulfilled", "fulfilled"])
        expect(query.get("seed")).toMatchObject({ body: "hello" })
      } else {
        expect(users).toEqual(["intruder"])
        for (const load of loads) {
          expect(load.status).toBe("rejected")
          if (load.status === "rejected") expect(load.reason).toMatchObject({
            name: "TransportStoppedError", closeCode: 4403, closeReason: "org access denied",
          })
        }
        expect(eager.status).toBe("error")
        user = "alice"
        await t.connect()
        if ("reload" in CollectionImpl.prototype) await (eager as unknown as { reload(): Promise<void> }).reload()
        else {
          await eager.cleanup()
          await eager.preload()
        }
        await query.cleanup()
        const next = createLiveQueryCollection(q => q.from({ m: source }))
        await next.preload()
        expect(next.get("seed")).toMatchObject({ body: "hello" })
        await next.cleanup()
      }
      expect(eager.get("seed")).toMatchObject({ body: "hello" })
      await query.cleanup()
      await source.cleanup()
      await eager.cleanup()
      t.close()
    })
  }

  it("an intentional pause keeps the same first load pending until the revived socket denies access", async () => {
    const room = `pause-denied-${crypto.randomUUID()}`
    await installAuthzOverride(room)
    let opens = 0
    const t = new WebSocketTransport<TestApi>({
      url: "unused",
      open: async signal => {
        if (++opens === 1) {
          return new Promise<WebSocketLike>((_resolve, reject) => {
            signal!.addEventListener("abort", () => reject(new Error("handshake aborted")), { once: true })
          })
        }
        return await openWs(`/sync/${room}`, { "x-user": "intruder" }) as unknown as WebSocketLike
      },
    })
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: m => m.id }))
    const load = messages.preload()
    const denied = expect(load).rejects.toMatchObject({ name: "TransportStoppedError", closeCode: 4403 })
    expect(opens).toBe(1)
    t.close()
    await Promise.resolve()
    expect(messages.status).toBe("loading")
    await t.connect()
    await denied
    expect(messages.status).toBe("error")
    expect(opens).toBe(2)
    await messages.cleanup()
    t.close()
  })

  it("retains ready rows on denial and clears the stop on a successful catch-up", async () => {
    const room = `ready-denied-${crypto.randomUUID()}`
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (_i, state) => {
      state.storage.sql.exec("INSERT INTO messages(id,body) VALUES('seed','before')")
    })
    let stop: TransportStoppedError | undefined
    const t = new WebSocketTransport<TestApi>({
      url: "unused",
      open: async () => await openWs(`/sync/${room}`) as unknown as WebSocketLike,
      onClosed: (_code, _reason, error) => { stop = error },
    })
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: m => m.id }))
    const health = () => (messages as unknown as { health?: { failureReason: unknown } }).health
    await messages.preload()
    await runInDurableObject(stub, (_i, state) => {
      for (const ws of state.getWebSockets()) ws.close(4403, "access revoked")
    })
    await waitFor(() => !!stop)
    expect(stop).toBeInstanceOf(TransportStoppedError)
    expect(messages.status).toBe("ready")
    expect(messages.get("seed")?.body).toBe("before")
    if ("reload" in CollectionImpl.prototype) {
      expect(health()?.failureReason).toMatchObject({ name: "TransportStoppedError", message: stop!.message })
    }
    await runInDurableObject(stub, (_i, state) => {
      state.storage.sql.exec("UPDATE messages SET body='after' WHERE id='seed'")
    })
    await t.connect()
    await waitFor(() => messages.get("seed")?.body === "after" && !health()?.failureReason)
    await messages.cleanup()
    t.close()
  })

  it("the transport treats the 4403 rejection as terminal: no retry loop, onClosed surfaces it", async () => {
    const room = "authpath-4403-transport"
    await installAuthzOverride(room)

    let opens = 0
    const closed: Array<[number | undefined, string | undefined]> = []
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 5,
      onClosed: (code, reason) => closed.push([code, reason]),
      open: async () => {
        opens++
        const res = await upgrade(`/sync/${room}`, { "x-user": "intruder" })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        return ws as unknown as WebSocketLike
      },
    })
    const handler: SubHandler = {
      onSnap: () => {},
      onSnapEnd: () => {},
      onDelta: () => {},
      onUptodate: () => {},
      onReset: () => {},
    }
    // The server closes immediately after accepting, so the sub frame may race
    // the close — a send on the dead socket rejecting is fine here.
    await t.subscribe("s1", "messages", handler).catch(() => {})

    await waitFor(() => closed.length === 1)
    // The app learns WHY (auth), instead of the transport looping forever
    // against the DO on a rejection that can never succeed.
    expect(closed[0]).toEqual([4403, "org access denied"])

    await new Promise((r) => setTimeout(r, 150))
    expect(opens).toBe(1)
  })
})
