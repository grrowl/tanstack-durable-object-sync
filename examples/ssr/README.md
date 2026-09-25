# ssr — tanstack-durable-object-sync example

Server-side rendering end to end (ADR-0011): a TanStack Start app on Cloudflare
Workers reads a `todos` collection from the sync DO **without a WebSocket**,
dehydrates it into the router payload, hydrates in the browser for an instant
first paint, then goes live over the socket and converges — catch-up from the
dehydrated cursor delivers whatever changed while the HTML was in flight.
Stale-while-revalidate, never a flash of empty.

Built on the **released** TanStack DB SSR API and the official
[`@tanstack/react-router-with-db`](https://www.npmjs.com/package/@tanstack/react-router-with-db)
Start adapter — `routerWithDbClient(router, dbClient)` handles DbProvider,
dehydrate/hydrate through the router, and Suspense query streaming. The only
app-specific part is this library's transport seam.

The example depends on the local package (`"tanstack-durable-object-sync":
"file:../.."`), so build it first:

## Run

```sh
npm install --prefix ../..     # the repo root's dependencies
npm run build --prefix ../..   # build the library's dist/ (file: dep)
npm install
npm run dev                    # vite dev with the Cloudflare plugin (runs in workerd)
```

Open the printed URL (default http://localhost:5173).

- `npm run build` — production build (client + worker)
- `npm run typecheck` — `tsc --noEmit`
- `npm run deploy` — build then `wrangler deploy`

## Pages

`/` is a plain landing page.

### `/live-query` — loader preload + `useLiveQuery`

The baseline SSR round trip. The route loader materializes the collection on
the request's DbClient and calls `preload()` — one snapshot read from the DO
via `readSyncSnapshot`. `routerWithDbClient` dehydrates the normalized rows
**plus the resume cursor** (our opaque `syncMeta`) into the router payload and
hydrates the browser client from it. Rows are in the raw HTML; the status line
flips `ssr → hydrated` on mount and `catching up → live` once the WebSocket
resumes from the dehydrated cursor and converges — updates *and* deletes made
while the HTML was in flight are applied. Adds and toggles are optimistic.
Open a second tab to watch them sync.

### `/live-suspense-query` — Suspense streaming + `useLiveSuspenseQuery`

No loader preload: the query is discovered mid-render. On the server the
component suspends while the snapshot transport reads the DO, and
`routerWithDbClient` streams the pending query result into the document — the
streamed shell shows the fallback, then the rows arrive, then the browser's
live WebSocket result replaces the snapshot once sync converges. The
"show only open" toggle changes the structured query IR (a new derived query
identity), which re-suspends until the new query computes.

## Shape

One worker serves everything (`src/server.ts`): WebSocket upgrades on `/sync/*`
go straight to the DO; every other request is the Start app via
`@tanstack/react-start/server-entry`.

- `src/todos-do.ts` — `TodosDO` (`todos` table + insert/update/delete
  mutations, `defineSync` object-schema API), seeded with three rows on first
  create. `readSyncSnapshot` comes with `SyncDurableObject`.
- `src/lib/todos.ts` — ONE shared collection descriptor:
  `collectionOptions("todos", (client) => doCollectionOptions({...}))`. The
  factory runs once per DbClient and pulls that environment's transport out of
  the client's dependency bag. The descriptor id matches the table name
  (`todos`) everywhere — that match routes dehydrated rows back into the
  collection on hydrate.
- `src/router.tsx` — the wiring (ADR-0011 D2 seam, released form). `getRouter()`
  runs per request on the server and once per tab in the browser; it builds a
  `DbClient` carrying the transport dependency — `SsrSnapshotTransport` over
  `stub.readSyncSnapshot(req, getRequest())` on the server (the incoming
  Request goes through `parseAttachment`, the SAME auth gate as the WS
  upgrade), `WebSocketTransport` to `/sync/main` in the browser — and hands
  both to `routerWithDbClient`. No manual DbProvider, HydrationBoundary, or
  serverFn: the adapter dehydrates the server client into the router stream
  and hydrates the browser client from it.
- `src/routes/live-query.tsx`, `src/routes/live-suspense-query.tsx` — the two
  consumers. Reads go through the descriptor in `from`; imperative writes
  through `useDbClient().collection(todosCollection)`.

The vite config dedupes `@tanstack/db`: the `file:` link would otherwise
resolve the library's peer import from the repo root's node_modules — a second
physical copy, which breaks the Symbol-branded `collectionOptions`.

The library's own `tests/ssr-*.test.ts` pin the dehydrate → hydrate → converge
contract; this example is the in-framework showcase.
