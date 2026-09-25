# multi-do — tanstack-durable-object-sync example

The Cloudflare microservices story: **one app, two Durable Objects**, each its
own sync stream. `RoomDO` owns a chat room's `messages`; `InboxDO` owns a user's
`notifications`. They share no storage and no connection — the browser opens
**one transport per DO** and stitches the two collections back together
client-side.

The example imports the library from source (`../../src`), so it always tracks
the current code. A published consumer would `import` from
`tanstack-durable-object-sync` / `.../client` instead.

## Run

```sh
npm install --prefix ../..   # the repo root: the adapter is imported from ../../src
npm install
npm run dev                  # builds the client bundle, then `wrangler dev`
```

Open the printed URL (default http://localhost:8787). Post messages, hit “notify
me”, then `rooms.clearRoom()` / `inbox.markAllRead()` and watch the merged feed
update. Open a **second tab** to see the room sync live across clients.

- `npm run build:client` — bundle the React client to `public/client.js` (esbuild)
- `npm run watch:client` — rebuild on change (run alongside `wrangler dev`)

## Topology — one transport per DO

There is **no muxed connection**. The Worker routes straight into each DO:

```
/rooms/:room/sync   → env.ROOM_DO.get(idFromName(room)).fetch(req)
/inbox/:user/sync   → env.INBOX_DO.get(idFromName(user)).fetch(req)
everything else     → ASSETS (index.html + client.js)
```

A DO's sync stream is a single ordered WebSocket with one client cursor
(ADR-0002). That invariant is **per DO** — so two DOs means two transports, each
the ordered stream for exactly one DO:

```ts
const roomTransport  = new WebSocketTransport<RoomApi>({ url: ".../rooms/lobby/sync?user=…" })
const inboxTransport = new WebSocketTransport<InboxApi>({ url: ".../inbox/<me>/sync?user=…" })
```

Each transport is parameterized by **that DO's** `Api` (imported as a *type
only* — nothing server-side is bundled). So `roomTransport.call.*` exposes only
RoomDO's commands and `inboxTransport.call.*` only InboxDO's, fully typed.

## Why commands are keyed by DO

Command names are scoped to a DO, not global. `RoomDO` could name a command
`markAllRead` too, and it would be a *different* command on a *different* stream.
To keep that collision-safe on the client, the transports are exposed through a
`SyncProvider` **keyed by DO**, read with a `useSync` hook:

```ts
const rooms = useSync("rooms")   // WebSocketTransport<RoomApi>
const inbox = useSync("inbox")   // WebSocketTransport<InboxApi>

await rooms.call.clearRoom()     // RoomApi  — zero-arg command, returns { deleted }
await inbox.call.markAllRead()   // InboxApi — returns { marked }
```

`rooms.call.*` and `inbox.call.*` are two disjoint, independently-typed
namespaces. There is no global command table to collide in: the DO you reached
*is* the namespace.

## Cross-DO joins happen client-side

The DO never joins, aggregates, or runs IVM (ADR-0001) — reads are client-side.
A “cross-DO view” is therefore assembled in the browser: two `useLiveQuery`
hooks (one per DO collection) merged into a single sorted timeline in render.
Both inputs stay live, so the merged feed updates whenever **either** DO emits a
delta. There is no server-side join across DOs — there couldn't be; they're
separate objects on separate streams.

## Commands vs mutations

- **Mutations** are typed single-row writes on a collection and ride the
  optimistic path: posting a message (`messages.insert`), marking one
  notification read (`notifications.update` with `{ read: 1 }`).
- **Commands** are everything else — bulk ops or anything returning a value:
  `clearRoom` (deletes all, returns the count) and `markAllRead` (returns how
  many it flipped). Their own SQL still flows through the CDC triggers, so the
  bulk `DELETE`/`UPDATE` fans out to every connected tab as ordinary deltas.

## Shape

- `src/env.ts` — shared `Claims` + `Env` (both DO bindings); kept separate so the
  schemas never import the worker.
- `src/room-schema.ts` — RoomDO's `defineSync` schema (`messages` insert +
  `clearRoom` command); exports `RoomApi`.
- `src/inbox-schema.ts` — InboxDO's schema (`notifications` insert/update +
  `markAllRead` command); exports `InboxApi`.
- `src/worker.ts` — `RoomDO` + `InboxDO` (each `registerSync`s its schema) and
  the upgrade router.
- `src/client.tsx` — two typed transports behind a per-DO `SyncProvider`/
  `useSync`, two collections, and the merged cross-DO feed via `useLiveQuery`.
