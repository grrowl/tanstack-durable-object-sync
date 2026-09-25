# on-demand — tanstack-durable-object-sync example

Demonstrates `syncMode: 'on-demand'`: the collection loads only the subsets your
live queries request. Items are categorised; each category is a panel that, when
opened, runs `loadSubset(where category = X)` — and releases it when closed.
Categories you never open are never synced.

## Run

```sh
npm install --prefix ../..   # the repo root: the adapter is imported from ../../src
npm install
npm run dev                  # builds the client, then `wrangler dev`
# optional: seed some rows
curl 'http://localhost:8787/seed?room=demo'
```

Open http://localhost:8787 — switch categories and watch each subset load on
demand. Add an item to a category that isn't open: it's persisted but won't
appear until you open that category (its subset loads it).

## Shape

- `src/worker.ts` — `ItemsDO` (one categorised `items` collection) + a
  `GET /seed` endpoint that inserts fixed rows.
- `src/client.tsx` — an on-demand collection; per-category `useLiveQuery` panels
  drive `loadSubset` / `unloadSubset` by mounting / unmounting.
