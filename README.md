# tanstack-durable-object-sync

> Real-time sync for your
> [Cloudflare Durable Object](https://developers.cloudflare.com/durable-objects/)
> using [TanStack DB](https://tanstack.com/db). Optimistic writes, live queries,
> and efficient reconnect catch-up. Your DO is the global source of truth, with
> zero extra infrastructure dependencies.

Your data already lives in a Durable Object, which is a strongly-consistent
little SQLite database running close to your users. This library adds
instantaneous sync to browsers in real time: subscribe to live queries, mutate
it optimistically, and real-time changes sync via DO to clients. Simple, secure,
and with low overhead.

It's built on [TanStack DB](https://tanstack.com/db) — the reactive-store
sibling of TanStack Query. If you've used `useQuery`, this will feel familiar:
`useLiveQuery` for reads, `collection.insert()` / `.update()` / `.delete()` for
writes. TanStack DB gives the client its reactive layer; this library handles
the sync to your DO. It's as at home streaming an LLM's output token-by-token as
it is doing ordinary CRUD.

It's a deliberately plain topology, each part doing what it does best. One
authoritative writer keeps the change log totally ordered and contiguous, so a
single cursor drives live deltas, reconnect catch-up, and write confirmation
alike — no second ack channel, no CRDT to merge, no Postgres to mirror. The
Durable Object holds authoritative state and assigns order; TanStack DB gives
the client its reactive layer (live queries, IVM, optimistic rollback); this
library carries the diffs between. You stop trading one good thing for another
— optimistic CRUD *and* a single source of truth, a simple transport *and* a
fully reactive client, at once.

---

## Why this exists

If you reach for sync on Cloudflare today, the good options each ask you to
give something up. CRDT engines — Cloudflare's own
[PartyKit](https://github.com/cloudflare/partykit) — are superb for
collaborative editing, but they're [Yjs](https://github.com/yjs/yjs)-shaped
(merge semantics, document baggage) with a thin authorization story.
[Zero](https://zero.rocicorp.dev/) is excellent for authz-filtered CRUD but
wants a Postgres to mirror into and doesn't run on a DO.
[LiveStore](https://dev.docs.livestore.dev/) *does* sync against DO SQLite, but
it's event-log-based: events are what's stored at rest, so a token-by-token LLM
stream [grows the log without bound](https://github.com/livestorejs/livestore/issues/136),
and there's no plain SQL or ORM underneath. So this library takes the third
path — the DO is the source of truth, its own SQLite is what's stored, and the
entire client-side reactive layer (live queries, incremental view maintenance,
optimistic rollback) comes from TanStack DB for free.

| | This library |
|---|---|
| **Source of truth** | The Durable Object's own SQLite. No Postgres, no external sync service. |
| **Transport** | One WebSocket per DO. Hibernation-native. |
| **Writes** | Bidirectional. Optimistic on the client; authoritative in the DO. |
| **Confirmation** | A position in the one stream the client already tails — no second ack channel. |
| **Reads** | Live queries via TanStack DB's client-side IVM. The DO never joins or aggregates. |
| **Consistency** | Server-authoritative. Single writer per DO. No CRDTs, no multi-DO transactions. |

---

## The model in 30 seconds

- **One DO instance = one sync scope** (a session, a workspace, a document —
  whatever you shard by). It owns a SQLite database with one or more
  collections.
- **Change-data-capture via triggers.** Every write to a collection table
  fires a trigger appending to a single per-DO change log. That log is the
  one ordered stream — the source of truth for live deltas, reconnect
  catch-up, *and* write confirmation.
- **One cursor.** The client tracks a single position (`appliedSeq`). A
  write is confirmed when that position passes the sequence the DO assigned
  the write — exactly Electric's `awaitTxId`, reduced to a `>=` comparison
  because a single writer produces a contiguous log.
- **Client-supplied keys.** The client mints the primary key (ULID / UUIDv7),
  so the optimistic row and the confirmed row are *the same row* — the write
  applies locally under the chosen id and the server confirms that same id, with
  no key reconciliation or id swap on commit.
- **Bounded retention.** The change log stays light: compaction keeps only the
  latest op per key, and changes are swept after a retention window (2 days by
  default, configurable). A client reconnecting from beyond that window gets a
  fresh snapshot instead of a delta. Bounded storage, no event-log explosion.

See [ADR-0001](./docs/adr/0001-sync-architecture.md) for the full rationale,
and [the build plan](./docs/adr/0001-sync-architecture.md#build-sequence) for
the milestone sequence.

---

## Quick start

Three files: the Durable Object that owns the data, the Worker that fronts it
and stamps trusted claims, and the browser. Here's the whole stack.

### 1. Define your Durable Object

```ts
import { defineSync, SyncDurableObject } from "tanstack-durable-object-sync"

interface Claims { userId: string }
interface Env { /* your bindings */ }
interface Message { id: string; author: string; content: string; created_at: number }

// defineSync binds identity (Claims) and binding-env (Env) once and returns
// three co-located helpers. They flow `user`/`env` into every handler ctx.
const sync = defineSync<Claims, Env>()

// The schema VALUE is both the DO registration and the client contract. The
// collection KEY ("messages") is the DB table name. `pk` must be a real column
// of Row — the sole TEXT, client-supplied key (ADR-0007).
export const chatSchema = sync.schema({
  collections: {
    messages: sync.collection<Message>({
      pk: "id",
      // The closed mutation trio { insert?; update?; delete? } — a 4th key is a
      // type error. op.cols is typed per op: full Row on insert, Partial on
      // update, absent on delete.
      mutations: {
        insert: {
          // authorize runs BEFORE the tx (async ok); throw to deny.
          // op.cols is typed Message here — no cast.
          authorize: ({ user, op }) => {
            if (op.cols.author !== user.userId) {
              throw new Error("author mismatch")
            }
          },
          // execute runs INSIDE transactionSync — synchronous only.
          execute: ({ op, sql }) => {
            const m = op.cols // Message
            sql.exec(
              "INSERT INTO messages(id, author, content, created_at) VALUES (?, ?, ?, ?)",
              m.id, m.author, m.content, m.created_at,
            )
          },
          // afterCommit (optional): fire-and-forget AFTER the commit + receipt —
          // the home for external side effects execute can't do (delete an R2
          // object, enqueue a job). Receives `env`; owns its own idempotency.
          // afterCommit: async ({ op, env }) => { await env.BUCKET.delete(op.key) },
        },
        delete: {
          execute: ({ op, sql }) => {
            // delete carries op.key only — no op.cols.
            sql.exec("DELETE FROM messages WHERE id = ?", op.key)
          },
        },
      },
    }),
  },
  // Commands are the escape hatch for writes that aren't a single typed row op.
  // Their own SQL still flows through the CDC triggers, and they can return a
  // result. Type-only Args is curried (call the factory twice); Result is
  // inferred from the return. If you have commands you MUST declare them inline
  // here so Args/Result inference flows into the Api type.
  commands: {
    clearRoom: sync.command()(({ sql }) => {
      const before = Array.from(sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
      sql.exec("DELETE FROM messages")
      return { deleted: before }
    }),
  },
})

// Export the schema type as the client contract.
export type Api = typeof chatSchema

export class SessionDO extends SyncDurableObject<Env, Claims> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    // You own your schema — migrate with anything (raw DDL, Drizzle, …), then
    // call registerSync to wire CDC. blockConcurrencyWhile runs it before the
    // first request.
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec(`CREATE TABLE IF NOT EXISTS messages (
        id         TEXT PRIMARY KEY,        -- client-supplied TEXT key (ULID/UUIDv7)
        author     TEXT NOT NULL,
        content    TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )`)

      // registerSync takes the schema VALUE — it compiles it, validates pk
      // affinity, and wires the CDC triggers.
      this.registerSync(chatSchema)
    })
  }

  // Read the Worker-forged claims header into the per-socket attachment.
  protected parseAttachment(req: Request): Claims {
    return JSON.parse(req.headers.get("x-claims") ?? "{}") as Claims
  }
}
```

> [!IMPORTANT]
> **Schema & migrations.** You own the table — create it with anything (raw
> `CREATE TABLE`, Drizzle, a versioned migrator), then call `registerSync` to
> wire CDC. The pk must have **TEXT affinity** (`TEXT`, `VARCHAR`, `CHAR`, …) so
> it stores the client-supplied id verbatim; an `INTEGER` key is rejected — it
> aliases rowid (server-assigned) and breaks optimistic id parity. Evolve
> freely: the CDC triggers capture only the row key, so `ALTER TABLE ADD COLUMN`
> flows to clients with no re-wiring, and re-running `registerSync` on the next
> deploy is idempotent (ADR-0007).

> [!NOTE]
> Server-side writes outside the client flow — an agent inserting a row, a
> webhook, a cron job, a bulk seed — go through `this.runSyncedWrite(sql => …)`:
> it applies your write and broadcasts it to connected clients (ADR-0006).

### 2. Route the upgrade from your Worker (the trust boundary)

This Worker fronts every `/sync/<sessionId>` WebSocket upgrade: match the path,
authenticate, then forge the claims header and hand off to the right DO.

```ts
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    // Only handle /sync/<sessionId> — the sessionId is the DO shard key.
    const match = new URL(req.url).pathname.match(/^\/sync\/(.+)$/)
    if (!match) return new Response("not found", { status: 404 })
    const sessionId = match[1]!

    // The trust boundary: authenticate here, then stamp claims the DO can trust.
    const claims = await verifyToken(req) // your auth
    if (!claims) return new Response("unauthorized", { status: 401 })
    const headers = new Headers(req.headers)
    headers.set("x-claims", JSON.stringify(claims)) // .set() overwrites any client-injected value

    const id = env.SESSION_DO.idFromName(sessionId)
    return env.SESSION_DO.get(id).fetch(new Request(req, { headers }))
  },
} satisfies ExportedHandler<Env>
```

### 3. Use it from the browser

```tsx
import { createCollection } from "@tanstack/db"
import { useLiveQuery } from "@tanstack/react-db"
import { doCollectionOptions, WebSocketTransport } from "tanstack-durable-object-sync/client"
import { ulid } from "ulid"
import type { Api } from "./session-do" // TYPE-ONLY — nothing server-side is bundled

const transport = new WebSocketTransport<Api>({ url: `wss://${host}/sync/${sessionId}` })

// Api-driven: the row type is inferred from the schema Api + table name, so
// there's no runtime schema value and no explicit Row generic. `m` is Message.
const messages = createCollection(
  doCollectionOptions<Api, "messages">({ transport, table: "messages", getKey: (m) => m.id }),
)

function ChatRoom({ userId }: { userId: string }) {
  const { data } = useLiveQuery((q) => q.from({ m: messages }).orderBy(({ m }) => m.created_at, "asc"))
  const send = (content: string) =>
    // Optimistic; resolves once the server confirms on the single stream.
    messages.insert({ id: ulid(), author: userId, content, created_at: Date.now() })
  // Commands run over the transport (not the collection). `call` is a typed
  // Proxy — the name autocompletes and the result is checked against the Api.
  const clear = () => transport.call.clearRoom() // Promise<{ deleted: number }>
  return <ChatView rows={data} onSend={send} onClear={clear} />
}
```

One `WebSocketTransport` per DO is shared by every collection on that DO
(multiplexed over the single socket). Pass `where` to
`doCollectionOptions` (eager mode) to sync only a matching subset. Commands go through the
same socket: `transport.call.<name>(args)` (typed sugar) or the low-level
`transport.sendCall("clearRoom", undefined)` — both mint the txId for you and
resolve with the command's result on `committed`.

#### Transport lifecycle & reconnect

The transport auto-reconnects on an unexpected drop and resubscribes every
collection from its single applied cursor (a windowed catch-up, not a
re-snapshot). Tune the pacing with `reconnectDelay` (a base-delay `number` for
the default jittered backoff, or a full `(attempt, closeCode?, closeReason?) =>
number | null` policy — return `null` to stop). Application close codes
(4000-4999) are terminal by default; wire `onClosed(code, reason)` to observe a
deliberate server close (e.g. an auth rejection) — see
[ADR-0016](./docs/adr/0016-reconnect-policy.md).

`close()` disposes the transport, and `connect()` **revives** it: a later
`connect()` clears the intentional-close latch, so a reused transport
auto-reconnects and delivers `onClosed` again. A `connect()` (and any
`subscribe`/`sendMut`/`fetch` awaiting it) never resolves disconnected — it
rejects `TransportClosedError` if the transport was closed and stays closed
([ADR-0020](./docs/adr/0020-connect-contract-abortable-open.md)).

The socket opener is injectable (`open`, for non-browser runtimes or tests). It
receives an optional `AbortSignal` that `close()` aborts while the handshake is
still in flight — honour it (close the socket, reject) so disposing a transport
before its socket finishes connecting never leaks a CONNECTING socket:

```ts
new WebSocketTransport<Api>({
  url,
  open: (signal) =>
    new Promise((resolve, reject) => {
      const ws = new WebSocket(url)
      signal?.addEventListener("abort", () => { ws.close(); reject(new Error("aborted")) }, { once: true })
      ws.addEventListener("open", () => resolve(ws))
      ws.addEventListener("error", () => reject(new Error("ws error")))
    }),
})
```

An opener that ignores the signal still works (the transport closes the socket
once `open()` resolves), but a handshake that never resolves then leaks.

### 4. SSR (experimental)

Built on TanStack DB's SSR support (`DbClient` `dehydrate()`/`hydrate()` and
the `exportSyncMeta`/`importSyncMeta`/`mergeSyncMeta` sync hooks, shipped in
`@tanstack/db` 0.8.0). Why/how trade-offs
live in [ADR-0011](./docs/adr/0011-ssr-dehydrate-hydrate.md).

On the worker, render through a **per-request** `DbClient` backed by one
snapshot read per subscription — no WebSocket from the render path:

```ts
// Route loader / server handler (per request!)
import { DbClient, collectionOptions } from "@tanstack/db"
import { doCollectionOptions, SsrSnapshotTransport } from "tanstack-durable-object-sync/client"

const stub = env.CHAT_DO.get(env.CHAT_DO.idFromName(sessionId))
// `request` is the incoming (claims-bearing) Request — the DO runs it through
// parseAttachment, the SAME auth gate as the WebSocket upgrade.
const transport = new SsrSnapshotTransport<Api>({
  read: (req) => stub.readSyncSnapshot(req, request),
})
const db = new DbClient()
const messages = db.collection(
  collectionOptions("messages", () =>
    doCollectionOptions<Api, "messages">({ transport, table: "messages", getKey: (m) => m.id }),
  ),
)
await messages.preload()
return { dbState: db.dehydrate() } // rows + our resume cursor (opaque syncMeta)
```

In the browser, hydrate before going live. The collection is ready
immediately with the dehydrated rows (stale-while-revalidate); the first sub
resumes from the dehydrated cursor, so the catch-up applies exactly what
changed while the HTML was in flight — updates *and* deletes:

```ts
const db = new DbClient()
db.hydrate(dbState) // or, in React: <DbProvider client={db}><HydrationBoundary state={dbState}>…</HydrationBoundary></DbProvider>
const messages = db.collection(
  collectionOptions("messages", () =>
    doCollectionOptions<Api, "messages">({ transport: wsTransport, table: "messages", getKey: (m) => m.id }),
  ),
)
```

Mutations during SSR throw (`SsrReadOnlyError`). `readSyncSnapshot` is callable
by any worker holding the DO binding, and its required `request` argument runs
through `parseAttachment` — **one auth gate for both the socket and the read
path**, so a tenant check in `parseAttachment` can't be bypassed by SSR.

---

## Examples

Each is a runnable Worker + browser client (`npm install && npm run dev`),
browser-verified.

- **[`examples/chat`](./examples/chat)** — eager sync of a room's messages;
  multi-tab live updates. The smallest end-to-end shape.
- **[`examples/on-demand`](./examples/on-demand)** — `syncMode: 'on-demand'`:
  categorised items where each panel loads only its subset (`loadSubset`/
  `unloadSubset`) and unopened categories are never synced.
- **[`examples/board`](./examples/board)** — the at-scale stress test: 5,000
  tasks on one DO with a bounded window, `useLiveInfiniteQuery` cursor
  scroll-back, and a mutable order key so voting bumps a task to the top
  (move-in). Its firehose makes the deferred bounded-window-under-churn
  limitation visible — `loaded` climbs past `window`.
- **[`examples/multi-do`](./examples/multi-do)** — two separate DOs (a room and
  an inbox) behind one Worker: one transport per DO, each typed by its own `Api`
  so `transport.call.*` is scoped to that DO's commands, and a cross-DO feed
  merged client-side (the DO never joins — ADR-0001).
- **[`examples/ssr`](./examples/ssr)** — TanStack Start on Cloudflare with
  `routerWithDbClient`: a loader-preloaded page (rows in the server HTML,
  WebSocket resumes from the dehydrated cursor) and a Suspense-streaming page
  (`useLiveSuspenseQuery`, result streamed into the document). ADR-0011.

> [!TIP]
> Using on-demand with `orderBy` + `limit`? Add a **range index** on the order
> column (`collection.createIndex((r) => r.field, { indexType: BTreeIndex })`) —
> without it the window can't page lazily: growing it re-requests the whole
> window from the start instead of fetching only the next page. See
> `examples/board`.

---

## Common patterns and recipes

Task-oriented guides in [`recipes/`](./recipes):

- **[Commands vs mutations](./recipes/commands-vs-mutations.md)** — when a write
  is a typed `insert`/`update`/`delete` and when it's a named command.
- **[End-to-end types](./recipes/end-to-end-types.md)** — share one schema type
  between server and client so the transport, commands, and collections are typed.
- **[On-demand and windows](./recipes/on-demand-and-windows.md)** — sync only the
  rows a query asks for, and grow a bounded window as the user scrolls.
- **[Server-originated writes](./recipes/server-originated-writes.md)** — write
  rows from the DO itself (webhooks, jobs, seeds) so clients still see them.
- **[Cohosting](./recipes/cohosting.md)** — add sync to a DO that already
  extends a framework base (`Agent`, `Think`), and how to compose with
  `@cloudflare/actors`.

---

## Cohosting: `Syncable` over a framework base

`SyncDurableObject` is the trivial application of a mixin, `Syncable(Base)`, that
adds the sync machinery to **any** Durable Object subclass. Use it when a DO
already extends a framework base — the Cloudflare Agents SDK `Agent`,
`@cloudflare/think`'s `Think` — and you want that same DO to also be a sync
source, instead of standing up a second DO and mirror-writing to it (ADR-0015).

```ts
import { Syncable } from "tanstack-durable-object-sync" // or ".../server/mixin"

// Curried: pin Env and your claims type, then apply over the runtime base.
class FeedAgent extends Syncable<Env, Claims>()(Agent<Env, State>) {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env) // host constructor first
    // Auth hook for the sync upgrade (same contract as parseAttachment):
    this.sync.configure({ parseAttachment: (req) => readClaims(req) })
    ctx.blockConcurrencyWhile(async () => {
      migrate(ctx.storage.sql) // you create the tables…
      this.sync.registerSync(feedSchema) // …then register (ADR-0007)
    })
  }
}
```

The sync API lives behind one facade, `this.sync` (`registerSync`,
`runSyncedWrite`, `drainAndBroadcast`, `parseAttachment`, `configure`,
`registry`), so the only public names the mixin adds to your class are `sync`,
`readSyncSnapshot` (the SSR read — public so DO RPC can reach it), and the four
WebSocket/`fetch` handlers. tddc's
sockets carry a reserved tag and a plain attachment, and it claims only the
`/_sync` path (configurable) — everything else is delegated to your host base, so
the two protocols never cross. No framework is added to tddc's dependency graph;
you supply `Base`.

Cohosting has a few conventions — reach `this.ctx.storage.sql` (not `this.sql`),
keep `__pk` out of your claims, opt in to two DO-global side effects, sync only
your own tables — and one caveat: a `@cloudflare/actors` `Actor` won't cohost
out of the box, since its `Sockets` helper takes over socket connections
completely. You can still compose the Actors features you do use — `Alarms`,
`Storage` — over a plain `DurableObject` base, as [Actors' own
examples](https://github.com/cloudflare/actors/tree/main/examples/durable-objects)
show. The conventions, what's verified and how, and the full
`@cloudflare/actors` story live in **[recipes/cohosting.md](./recipes/cohosting.md)**.

---

## Non-goals

- **Multi-DO transactions.** A transaction touches collections in one DO.
- **Server-assigned primary keys.** Optimism requires id parity.
- **Per-row read authorization.** Reads are gated at the WebSocket upgrade
  (per DO). Shard into more DOs for finer read isolation.
- **Server-side joins / IVM.** The DO stores and emits; the client composes.
- **An event log.** The change log is a *state-convergence* log, compacted to
  latest-op-per-key. It is not an audit trail.

---

## Acknowledgements

The design is indebted to, and learns directly from, the open-source work of
[ElectricSQL](https://github.com/electric-sql/electric) and
[TanStack DB](https://github.com/TanStack/db). This library is offered back to
that community under the MIT license.

## License

[MIT](./LICENSE) © Tom McKenzie
