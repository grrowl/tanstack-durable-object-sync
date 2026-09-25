# Examples

## [chat](./chat)

A minimal multi-client chat — Worker + `SessionDO` + a React `useLiveQuery`
client. Showcases the whole stack end to end: optimistic **mutations**, live
cross-tab sync, reconnect, and a **command** (`clearRoom`) for the one action
that isn't a typed row write.

## [on-demand](./on-demand)

Categorised items where each category panel loads only when opened. Showcases
`syncMode: 'on-demand'` — the collection syncs only the subsets your live
queries request, via `loadSubset` / `unloadSubset` as panels mount and unmount.
Categories you never open are never synced.

## [board](./board)

A high-volume "task board": 5,000 tasks on **one** Durable Object. Showcases
windowed pagination at scale — a bounded window (top 50) with cursor `fetch` for
scroll-back; a **mutable** order key, so a bump arrives as **move-in /
move-out**; and **server-originated writes** (`runSyncedWrite`) via `/seed` and
`/bump`. It also surfaces the deferred bounded-window-under-churn limitation as a
live number.

## [multi-do](./multi-do)

Two **separate** Durable Objects (a room and an inbox) behind one Worker.
Showcases the multi-DO story: **one transport per DO**, a React
`SyncProvider` / `useSync` keyed by DO so each DO's typed `transport.call`
namespace stays disjoint (no command-name collisions), and a **cross-DO feed**
merged client-side (the DO never joins — ADR-0001).

## One `@tanstack/db`, checked in CI

Each example imports the adapter from this repo (`../../src`, or a
`file:../..` link for ssr), so the adapter's own `import "@tanstack/db"` would
resolve from the **repo root's** `node_modules`. The result is a second copy
next to the example's, which breaks `instanceof` and the Symbol-branded
collection options across the boundary. Every example therefore pins that
import to its own copy (ADR-0024):

- the client bundle: esbuild `--alias:@tanstack/db=@tanstack/db` (ssr: vite
  `resolve.dedupe`);
- the worker bundle: `alias` in `wrangler.jsonc` (ssr: the same `dedupe`);
- the types: `paths` in `tsconfig.json`.

A published consumer doesn't need any of this. The hazard comes from importing
the adapter from source: an installed adapter imports the peer from the app's
own install.

Scripts in every example, all run by `.github/workflows/examples.yml`:

- `npm run typecheck`: `tsc --noEmit`.
- `npm run build`: the client bundle and a `wrangler deploy --dry-run` worker
  bundle, written to `dist/` (ssr: `vite build`).
- `npm run check:single-copy`: run after `build` (ssr: `build -- --sourcemap`).
  It fails if a bundle holds more than one `@tanstack/db` or `@tanstack/db-ivm`,
  or a copy other than the example's own.
- `npm run smoke`: run after `build`. It boots the worker in workerd and probes
  it over HTTP and WebSocket (ssr: checks the server-rendered rows), so an error
  during module evaluation fails.
