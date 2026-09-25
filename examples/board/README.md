# board (stress) — tanstack-durable-object-sync example

A high-volume "task board" on **one** Durable Object — the at-scale stress test
for on-demand windowed pagination. One board holds **5,000 tasks**, but a client
loads only a bounded window (top 50 by `updated_at`) and pages older ones in on
demand via the cursor `fetch`.

Tasks are ordered by a **mutable** key: voting (▲) or starring (★) sets
`updated_at = now`, which **bumps** a task to the top. That exercises
**move-in / move-out** — a bump to a task a client never loaded arrives as an
`update` delta for an absent key and upserts into the window (the always-emit
rule, ADR-0002 C4). Uses `useLiveInfiniteQuery`, whose `setWindow` drives the
real cursor `loadSubset`.

## Run

```sh
npm install --prefix ../..   # the repo root: the adapter is imported from ../../src
npm install
npm run dev                  # builds the client, then `wrangler dev`
```

Open http://localhost:8787 — the first load auto-seeds 5,000 tasks. Then:

- **Bounded load.** On join, `loaded` is only a little above `window` (the
  first page plus a peek-ahead) even though `total` is 5,000 — the whole board
  is *not* synced.
- **Scroll-back.** Scroll the list (or "load older") to page older tasks via the
  cursor; `window` and `loaded` grow a page at a time.
- **Bump / move-in.** ▲ or ★ a task — it jumps to the top. Open a second tab and
  start the **firehose**: it bumps random (mostly cold) tasks server-side, and
  the other tab sees them move into its window.

## Watch the deferred limitation, made visible

Under the firehose, **`loaded` climbs past `window`**: every cold task that gets
bumped upserts into the collection and is never evicted, while the visible window
stays 50. That growing gap *is* the deferred "bounded-window maintenance under
churn" limitation (ADR-0001) — the example surfaces it as a number rather than
hiding it. The window stays correct throughout; only the loaded set grows.

## Shape

- `src/worker.ts` — `BoardDO` (one `tasks` collection; insert/update/delete
  mutations) + endpoints: `GET /seed?n=` (tie-bursts: every 25 tasks share an
  `updated_at`, so scroll-back crosses real tie boundaries), `GET /bump?n=` (the
  server-side firehose — mutates random tasks and broadcasts), `GET /count`.
- `src/client.tsx` — an on-demand collection with a **range index**
  (`BTreeIndex`) on the order column so the window pages lazily; a
  `useLiveInfiniteQuery` window; vote / star / delete; firehose toggle; the
  window / loaded / total metrics.
