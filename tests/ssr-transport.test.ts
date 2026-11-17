import { createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { SsrReadOnlyError, SsrSnapshotTransport, type SnapshotRead } from "../src/client/ssr-transport.ts"
import { settled } from "./settled.ts"

// WHY (ADR-0011 D2): server rendering must run the SAME collection adapter the
// browser runs — one code path, swapped at the transport seam — with one
// snapshot read per subscription and no socket. These pin: eager preload
// materializes the DO's rows; on-demand loadSubset works under a server-side
// live query preload (the upstream SSR fixture's flagship pattern); the
// render's cursor is the durable high-water mark; and any write during SSR
// fails loud as the design error it is.

interface Msg {
  id: string
  body: string
}

/** Structural schema Api the branded transport carries, so `doCollectionOptions`
 *  infers Row = Msg for `table: "messages"`. */
type Api = { collections: { messages: { __row?: Msg } } }

/** Exactly what an SSR worker passes: the DO stub's RPC, as a function. */
function makeRead(room: string): SnapshotRead {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room)) as unknown as {
    readSyncSnapshot: (r: Parameters<SnapshotRead>[0], request: Request) => ReturnType<SnapshotRead>
  }
  // The author closes over the claims-bearing Request; the transport's read
  // contract stays {collection, where, ...} only.
  return (req) => stub.readSyncSnapshot(req, new Request("https://example.com/ssr", { headers: { "x-user": "anon" } }))
}

async function seed(room: string, rows: Array<[string, string]>): Promise<void> {
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const [id, body] of rows) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", id, body)
  })
}

describe("SsrSnapshotTransport (server-side render path, ADR-0011 D2)", () => {
  it("eager: preload materializes the DO's rows and a resumable cursor, no socket", async () => {
    const room = `ssrt-eager-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "hi"],
      ["b", "yo"],
    ])

    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id }),
    )
    await messages.preload()

    expect(messages.size).toBe(2)
    expect(messages.get("a")).toMatchObject({ id: "a", body: "hi" })
    expect(BigInt(transport.appliedCursor)).toBeGreaterThan(0n) // dehydration exports this
  })

  it("on-demand: a server-side live query preload drives loadSubset through one read", async () => {
    const room = `ssrt-od-${crypto.randomUUID()}`
    await seed(room, [
      ["a", "keep"],
      ["b", "drop"],
    ])

    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" }),
    )
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    expect(kept.get("a")).toMatchObject({ id: "a", body: "keep" })
    expect(kept.get("b")).toBeUndefined()
    expect(BigInt(transport.appliedCursor)).toBeGreaterThan(0n)
  })

  it("rejects writes during SSR — read-only, fail loud", async () => {
    const room = `ssrt-ro-${crypto.randomUUID()}`
    await seed(room, [["a", "hi"]])

    const transport = new SsrSnapshotTransport<Api>({ read: makeRead(room) })
    const messages = createCollection(
      doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id }),
    )
    await messages.preload()

    const tx = messages.insert({ id: "x", body: "nope" })
    await expect(settled(tx)).rejects.toThrow(SsrReadOnlyError)
  })
})
