# chat — tanstack-durable-object-sync example

A minimal multi-client chat: a Cloudflare Worker + `SessionDO` (the sync DO) and
a React client using `useLiveQuery` over a DO-backed collection. Optimistic
sends, live cross-tab sync, reconnect — the whole stack end to end.

The example imports the library from source (`../../src`), so it always tracks
the current code. A published consumer would `import` from
`tanstack-durable-object-sync` / `.../client` instead.

## Run

```sh
npm install --prefix ../..   # the repo root: the adapter is imported from ../../src
npm install
npm run dev                  # builds the client bundle, then `wrangler dev`
```

Open the printed URL (default http://localhost:8787), then open a **second tab**
and watch messages sync live between them. Each tab gets a throwaway identity.

- `npm run build:client` — bundle the React client to `public/client.js` (esbuild)
- `npm run watch:client` — rebuild on change (run alongside `wrangler dev`)

## Commands vs mutations — the `clear room` button

Sending a message is a **mutation**: a typed `insert` on the `messages`
collection, so it rides TanStack DB's optimistic path (the message shows
instantly, then confirms on the stream).

"Clear the room" is not a single-row write, so it can't be an
insert/update/delete. It's a **command** (`sync.command` →
`transport.call.clearRoom()`): RPC that runs outside the optimistic path and returns a
result (here, the count it deleted). The thing worth seeing is that a command's
own SQL writes still flow through the CDC triggers — so the server-side `DELETE`
fans out to **every** connected tab as ordinary `delete` deltas, and the list
empties live for everyone. Open two tabs, fill one, hit `clear room`, and watch
both empty at once.

That's the rule of thumb: typed row writes are mutations; anything else (bulk
ops, RPC, async external work, operations that return a value) is a command.

## Shape

- `src/worker.ts` — `SessionDO` (one `messages` collection with an insert
  mutation authorized to the connected user, plus a `clearRoom` command) and the
  upgrade router (`/sync` → DO, everything else → static assets). The DO's
  `defineSync` schema is exported as `ChatApi` for the client to type-only import.
- `src/client.tsx` — one `WebSocketTransport<ChatApi>` + a `messages` collection
  via `doCollectionOptions` (Row inferred from `ChatApi`), rendered by
  `useLiveQuery`; `send` is an optimistic mutation, `clear room` calls the
  command over `transport.call.clearRoom()`.
