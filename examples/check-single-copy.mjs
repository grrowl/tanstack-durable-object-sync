#!/usr/bin/env node
// Fails if an example's bundle carries more than one copy of @tanstack/db (or
// @tanstack/db-ivm), or any copy other than the example's own top-level install
// (so, by construction, the version the example installs). Two copies break the
// Symbol-branded collectionOptions and every instanceof across the boundary; a
// foreign copy runs a db version the example never asked for (ADR-0024).
//
//   node ../check-single-copy.mjs <example-dir> <runtime>...
//
// Each <runtime> (relative to the example) is ONE bundle graph, checked on its
// own:
//   - an esbuild metafile (`*.json`, from esbuild `--metafile` or wrangler
//     `--metafile`): the files that contribute bytes to an output, relative to
//     the example dir — so ESM and CJS builds of one install show up as the two
//     module instances they are;
//   - a directory of source maps (vite, which has no metafile). They identify
//     the install; the build format only when the maps name `dist/<format>`
//     (a map that chains back to a package's own `src/` hides it).

import { existsSync, readdirSync, readFileSync, realpathSync, statSync } from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

const PACKAGES = ["@tanstack/db", "@tanstack/db-ivm"]

const [exampleArg, ...runtimeArgs] = process.argv.slice(2)
if (!exampleArg || runtimeArgs.length === 0) {
  console.error("usage: check-single-copy.mjs <example-dir> <runtime>...")
  process.exit(2)
}
const exampleDir = realpathSync(exampleArg)

function readJson(file) {
  return JSON.parse(readFileSync(file, "utf8"))
}

// Non-file ids (virtual modules, `\0`-prefixed plugin ids, `wrangler:`-style
// namespaces) can't be a package copy and are skipped.
const isFileId = (s) => !s.startsWith("\0") && !/^[a-z][a-z0-9+.-]*:/i.test(s)
const real = (p) => (existsSync(p) ? realpathSync(p) : p)

// Output-level inputs with bytes: a module esbuild loaded but tree-shook to
// nothing is not a runtime instance.
function metafileInputs(file) {
  return Object.values(readJson(file).outputs ?? {})
    .flatMap((o) => Object.entries(o.inputs ?? {}))
    .filter(([p, i]) => i.bytesInOutput > 0 && isFileId(p))
    .map(([p]) => real(path.resolve(exampleDir, p)))
}

function sourceMapInputs(dir) {
  const maps = (function walk(d) {
    return readdirSync(d, { withFileTypes: true }).flatMap((e) => {
      const p = path.join(d, e.name)
      if (e.isDirectory()) return e.name === "node_modules" ? [] : walk(p)
      return e.name.endsWith(".map") ? [p] : []
    })
  })(dir)
  return maps.flatMap((m) => {
    const map = readJson(m)
    const base = path.resolve(path.dirname(m), map.sourceRoot ?? "")
    return (map.sources ?? []).flatMap((s) => {
      if (s.startsWith("file://")) return [real(fileURLToPath(s))]
      return isFileId(s) ? [real(path.resolve(base, s))] : []
    })
  })
}

// A copy is an install root, split by build format when the path shows it
// (`dist/esm` vs `dist/cjs`).
function copyOf(file, pkg) {
  const marker = `${path.sep}node_modules${path.sep}${pkg.replace("/", path.sep)}${path.sep}`
  const i = file.lastIndexOf(marker)
  if (i < 0) return null
  const root = file.slice(0, i + marker.length - 1)
  const [top, sub] = file.slice(i + marker.length).split(path.sep)
  const format = top === "dist" && sub && !sub.includes(".") ? `dist/${sub}` : top
  return { root, id: `${path.relative(exampleDir, root)} [${format}]` }
}

let failed = false
const fail = (msg) => {
  failed = true
  console.error(`FAIL ${msg}`)
}

for (const rt of runtimeArgs) {
  const target = path.resolve(exampleDir, rt)
  if (!existsSync(target)) {
    fail(`${rt}: not found — build first`)
    continue
  }
  const inputs = statSync(target).isDirectory() ? sourceMapInputs(target) : metafileInputs(target)
  if (inputs.length === 0) {
    fail(`${rt}: no bundle inputs found`)
    continue
  }
  for (const pkg of PACKAGES) {
    const copies = new Map() // id -> root
    for (const f of inputs) {
      const c = copyOf(f, pkg)
      if (c) copies.set(c.id, c.root)
    }
    const list = [...copies.keys()].map((id) => `\n    ${id}`).join("")
    if (copies.size === 0) {
      // db-ivm can legitimately tree-shake away; db itself is always used.
      if (pkg === "@tanstack/db") fail(`${rt}: ${pkg} is not in the bundle`)
      continue
    }
    if (copies.size > 1) {
      fail(`${rt}: ${copies.size} copies of ${pkg}${list}`)
      continue
    }
    const [[id, root]] = copies
    const own = path.join(exampleDir, "node_modules", pkg)
    const version = readJson(path.join(root, "package.json")).version
    if (root !== real(own)) {
      const want = existsSync(own) ? readJson(path.join(own, "package.json")).version : "none"
      fail(`${rt}: ${pkg} ${version} bundled, not the example's own install (${want})${list}`)
      continue
    }
    console.log(`ok   ${rt}: one ${pkg} ${version} — ${id}`)
  }
}

process.exit(failed ? 1 : 0)
