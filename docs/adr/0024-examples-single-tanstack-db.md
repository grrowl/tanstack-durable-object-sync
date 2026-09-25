# 0024 — Examples bundle exactly one `@tanstack/db`, and CI proves it

**Status:** Accepted. Scope: `examples/**` and `.github/workflows/examples.yml`
only. Published consumers are unaffected: the hazard here comes from importing
the adapter from this repo's source. An installed adapter imports the
`@tanstack/db` peer from the app's own install, and npm places a satisfiable
peer once.

## Context

The examples import the adapter from this repo rather than from npm, so they
track the real code: the esbuild examples use `../../src`, and ssr uses a
`file:../..` link to `dist/`. The adapter's own `import … from "@tanstack/db"`
therefore resolves from the **repo root's** `node_modules`, while the app's
imports resolve from the example's. Measured on `main` before this ADR:

- **Client bundle** (chat): 46 modules from the example's db 0.6.5 plus 5 from
  the root's db 0.8.5. That is two copies, which breaks every `instanceof` and
  the Symbol-branded collection options across the boundary. The example only
  built because the adapter quietly used the root's newer copy. Forcing a
  single 0.6.5 copy fails the build, because 0.6.5 lacks
  `withCollectionConfigFactory`.
- **Worker bundle**: one copy, but it is the root's. The server-side evaluator
  (ADR-0013) ran a db version the example never pinned.
- **Types**: with the root and the example on different versions, `tsc` sees
  two `CollectionConfig`s (TS2769). The esbuild examples had no typecheck
  script, so this went unnoticed. ssr typechecked only because both sides
  happened to be 0.8.5.
- A dry-run bundle never executes the code, so a crash during module
  evaluation passes every build check. db 0.8.5 calls
  `crypto.getRandomValues()` at global scope, which workerd rejects.

## Decision

**D1: one resolver override per bundler, all pointing at the example's own
copy.**

- Client (esbuild CLI): `--alias:@tanstack/db=@tanstack/db`. esbuild resolves
  an alias's target from the working directory (the example), not from the
  importing file, and it still goes through the package `exports`.
- Worker (wrangler): `"alias": { "@tanstack/db":
  "./node_modules/@tanstack/db/dist/esm/index.js" }`. Wrangler implements alias
  with `require.resolve`. The bare package name therefore picks db's **CJS**
  build and drops `sideEffects: false` tree-shaking (138 KB → 1.2 MB). The ESM
  entry file keeps the worker at about 141 KB. The cost is depending on db's
  `dist` layout. If that file moves, `require.resolve` throws and the build
  fails loudly.
- ssr (vite): the existing `resolve.dedupe: ["@tanstack/db"]` covers the client
  and the workerd environment.
- Types: `"paths": { "@tanstack/db": ["./node_modules/@tanstack/db"] }` in every
  example's `tsconfig.json`, ssr included.

**D2: a single-copy check that fails.** `examples/check-single-copy.mjs`
checks each runtime separately (browser and worker). For esbuild and wrangler
it reads the metafile: the files that contribute bytes to the output, so ESM
and CJS count as distinct instances. For vite, which has no metafile, it reads
source maps. These identify the install root, and they show the build format
only when the map names `dist/<format>`, as vite's do here. It fails when:

- `@tanstack/db` is absent (a vacuous pass);
- there is more than one copy of `@tanstack/db` or `@tanstack/db-ivm`;
- the copy is not the example's own top-level install. That includes a nested
  install and the root's, and it implies the example's installed version.

**D3: a boot smoke in workerd.** `examples/smoke-boot.mjs` runs `wrangler dev`
(ssr: `vite preview` of the built worker) and probes it over HTTP and
WebSocket. For ssr it asserts that the rows are in the server-rendered HTML. A
module-evaluation crash then fails CI. Pinning ssr back to db 0.8.5 turns it
red with workerd's "Disallowed operation called within global scope".

**D4: CI.** `.github/workflows/examples.yml` runs one job per example:

1. root `npm ci` and build, then the example's `npm ci`;
2. `typecheck`;
3. `build`;
4. `check:single-copy`;
5. `smoke`.

It is separate from `ci.yml`.

## Rejected

- **esbuild `nodePaths`**: it is only a fallback, and the root's
  `node_modules` is found first.
- **`file:../..` for the esbuild examples**: the symlink's real path still
  resolves the peer from the root, so this also needs `--preserve-symlinks`. It
  also moves the examples from `src` to `dist` and needs a root build first.
  More churn for the same result.
- **Leaving the worker on the root copy**: that is one copy, but not the pinned
  version. The server evaluator would drift from the client's.
- **Wrangler alias to the bare package name**: the CJS build and no
  tree-shaking (D1).
- **Committing a two-browser Playwright run to CI**: too heavy for this gate.
  Two-client sync is checked locally before a release. CI keeps the boot smoke.
