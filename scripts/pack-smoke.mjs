#!/usr/bin/env node
// Pack smoke test (ADR-0022): the package as npm consumers receive it.
//
//   node scripts/pack-smoke.mjs [--db <spec>]... [--keep]
//
// Builds and packs the package, checks the tarball ships everything its
// manifest and source maps reference, then for each @tanstack/db spec (default:
// the declared floor and the locked version) installs the tarball into a fresh
// consumer (scripts/pack-smoke/) and:
//   - type-checks it with skipLibCheck OFF — a Worker program (bundler and
//     nodenext resolution) and a DOM-only browser program;
//   - imports `./client` under Node's strict ESM resolution;
//   - bundles the consumer Worker with wrangler and boots it in real workerd,
//     rendering an SSR snapshot from a Durable Object. The vitest pool cannot
//     see module-scope restrictions (it evaluates inside a request); this can.
//
// The consumer installs @cloudflare/workers-types itself — the documented setup
// (README, examples). Our server .d.ts files import it without declaring it.

import { execFileSync } from "node:child_process"
import { cpSync, existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import { dirname, join, relative, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { dbFloor } from "./db-floor.mjs"

const root = fileURLToPath(new URL("..", import.meta.url))
const fixture = join(root, "scripts/pack-smoke")
const require = createRequire(join(root, "package.json"))
const installedVersion = (name) => require(`${name}/package.json`).version

const args = process.argv.slice(2)
const specs = []
let keep = false
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--db" && args[i + 1]) specs.push(args[++i])
  else if (args[i] === "--keep") keep = true
  else fail(`unknown argument ${JSON.stringify(args[i])}; usage: pack-smoke.mjs [--db <spec>]... [--keep]`)
}
if (specs.length === 0) specs.push(dbFloor(), installedVersion("@tanstack/db"))

const tsc = require.resolve("typescript/bin/tsc")
const wranglerBin = require.resolve("wrangler/bin/wrangler.js")
// Miniflare through wrangler: wrangler's own pinned dependency, not a hoisted
// transitive one.
const { Miniflare } = createRequire(require.resolve("wrangler/package.json"))("miniflare")
const workersTypes = installedVersion("@cloudflare/workers-types")

/** Known diagnostics in @tanstack/db's OWN declarations: its local-storage
 *  types assume DOM, absent from a Worker program. Shown, not failed on. Any
 *  other diagnostic fails — upstream's included: the root typecheck skips lib
 *  checks, so this is the only place an upstream declaration break shows. */
const KNOWN_UPSTREAM = [
  /^node_modules\/@tanstack\/db\/dist\/esm\/local-storage\.d\.ts\(\d+,\d+\): error TS(2304|2552): Cannot find name '(Storage|StorageEvent)'/,
]

const work = mkdtempSync(join(tmpdir(), "tddc-pack-smoke-"))
try {
  step("build", () => run("npm", ["run", "build"], root))
  run("npm", ["pack", "--pack-destination", work], root)
  const tarball = join(work, readdirSync(work).find((f) => f.endsWith(".tgz")))
  const pkgDir = join(work, "package")
  run("tar", ["xzf", tarball, "-C", work], work)
  step("tarball ships everything it references", () => checkTarball(pkgDir))

  for (const spec of [...new Set(specs)]) {
    const dir = join(work, `consumer-${spec.replace(/[^\w.-]/g, "_")}`)
    for (const f of ["src", "tsconfig.worker.json", "tsconfig.browser.json", "wrangler.json"]) {
      cpSync(join(fixture, f), join(dir, f), { recursive: true })
    }
    writeFileSync(join(dir, "package.json"), JSON.stringify({ name: "tddc-smoke-consumer", private: true, type: "module" }))
    const tag = `[@tanstack/db ${spec}]`
    step(`${tag} install`, () => {
      run("npm", ["install", "--no-audit", "--no-fund", "--no-package-lock", tarball,
        `@tanstack/db@${spec}`, `@cloudflare/workers-types@${workersTypes}`], dir)
      const got = JSON.parse(readFileSync(join(dir, "node_modules/@tanstack/db/package.json"), "utf8")).version
      if (/^\d+\.\d+\.\d+$/.test(spec) && got !== spec) throw new Error(`installed @tanstack/db ${got}, wanted ${spec}`)
      console.log(`  resolved @tanstack/db ${got}`)
    })
    step(`${tag} types: worker program (bundler)`, () => typecheck(dir, ["-p", "tsconfig.worker.json"]))
    step(`${tag} types: worker program (nodenext)`, () =>
      typecheck(dir, ["-p", "tsconfig.worker.json", "--module", "nodenext", "--moduleResolution", "nodenext"]))
    step(`${tag} types: browser program`, () => typecheck(dir, ["-p", "tsconfig.browser.json"]))
    step(`${tag} runtime: node import of ./client`, () =>
      run("node", ["--input-type=module", "-e",
        `const m = await import("tanstack-durable-object-sync/client")
         if (typeof m.doCollectionOptions !== "function") throw new Error("doCollectionOptions missing")`], dir))
    await stepAsync(`${tag} runtime: boot in workerd + SSR render`, () => bootWorker(dir))
  }
  console.log(`\npack smoke: OK (${[...new Set(specs)].join(", ")})`)
} catch (e) {
  console.error(`\npack smoke: FAILED\n${e.message}`)
  process.exitCode = 1
} finally {
  if (keep) console.log(`kept ${work}`)
  else rmSync(work, { recursive: true, force: true })
}

/** Every path the manifest points at (main/types/every exports condition) and
 *  every `sources` entry of every shipped source map must be in the tarball. */
function checkTarball(pkgDir) {
  const pkg = JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8"))
  const targets = [pkg.main, pkg.types]
  const walk = (v) => (typeof v === "string" ? targets.push(v) : v && Object.values(v).forEach(walk))
  walk(pkg.exports)
  const missing = targets.filter((t) => t && !existsSync(join(pkgDir, t))).map((t) => `manifest → ${t}`)
  const maps = listFiles(pkgDir).filter((f) => f.endsWith(".map"))
  if (maps.length === 0) throw new Error("no source maps in the tarball")
  for (const map of maps) {
    const { sources = [], sourceRoot = "" } = JSON.parse(readFileSync(map, "utf8"))
    for (const s of sources) {
      const target = resolve(dirname(map), sourceRoot, s)
      if (!target.startsWith(pkgDir + "/") || !existsSync(target)) missing.push(`${relative(pkgDir, map)} → ${s}`)
    }
  }
  if (missing.length) throw new Error(`tarball is missing referenced files:\n    ${missing.join("\n    ")}`)
  console.log(`  ${targets.length} manifest targets, ${maps.length} source maps: all resolve`)
}

/** tsc with skipLibCheck OFF: our shipped declarations and the consumer code. */
function typecheck(dir, argv) {
  let out = ""
  try {
    run("node", [tsc, "--pretty", "false", ...argv], dir)
  } catch (e) {
    out = e.message
  }
  if (!out) return
  const diags = out.split("\n").filter((l) => /: error TS\d+/.test(l))
  const known = diags.filter((l) => KNOWN_UPSTREAM.some((re) => re.test(l)))
  if (diags.length === 0 || known.length !== diags.length) throw new Error(out)
  console.log(`  ignored ${known.length} known diagnostic(s) in @tanstack/db's own declarations:`)
  for (const l of known) console.log(`    ${l}`)
}

async function bootWorker(dir) {
  const config = JSON.parse(readFileSync(join(dir, "wrangler.json"), "utf8"))
  run("node", [wranglerBin, "deploy", "--dry-run", "--outdir", "out"], dir, { WRANGLER_SEND_METRICS: "false" })
  const mf = new Miniflare({
    modules: true,
    modulesRoot: join(dir, "out"),
    scriptPath: join(dir, "out/worker.js"),
    compatibilityDate: config.compatibility_date,
    durableObjects: Object.fromEntries(
      config.durable_objects.bindings.map((b) => [b.name, { className: b.class_name, useSQLite: true }]),
    ),
  })
  try {
    // A hang (e.g. a preload() that never settles) fails, rather than eating
    // the CI job's timeout.
    const res = await Promise.race([
      mf.dispatchFetch("http://smoke.test/"),
      new Promise((_, reject) => setTimeout(() => reject(new Error("worker did not respond within 60s")), 60_000).unref()),
    ])
    const body = await res.text()
    if (res.status !== 200) throw new Error(`worker responded ${res.status}: ${body}`)
    const { state, mixedRows, mixinEntriesAgree } = JSON.parse(body)
    const [messages] = state.collections
    const rows = messages.rows.map((r) => [r.key, r.value?.content])
    if (JSON.stringify(rows) !== '[["m1","hello"]]') throw new Error(`dehydrated rows ${JSON.stringify(messages.rows)}; wanted the seeded m1`)
    if (!(Number(messages.syncMeta?.cursor) > 0)) throw new Error(`dehydrated cursor ${JSON.stringify(messages.syncMeta)}; wanted > 0`)
    if (mixedRows !== 0) throw new Error(`mixin DO snapshot had ${mixedRows} rows; wanted 0`)
    if (mixinEntriesAgree !== true) throw new Error("`.` and `./server/mixin` exported different Syncable instances")
    console.log(`  SSR dehydrated 1 row at cursor ${messages.syncMeta.cursor}`)
  } finally {
    await mf.dispose()
  }
}

function listFiles(d) {
  return readdirSync(d).flatMap((f) => (statSync(join(d, f)).isDirectory() ? listFiles(join(d, f)) : [join(d, f)]))
}

function run(cmd, argv, cwd, env = {}) {
  try {
    return execFileSync(cmd, argv, {
      cwd,
      encoding: "utf8",
      env: { ...process.env, ...env },
      stdio: "pipe",
      timeout: 300_000,
    })
  } catch (e) {
    throw new Error(`${cmd} ${argv.join(" ")} failed (exit ${e.status}):\n${e.stdout ?? ""}${e.stderr ?? ""}`)
  }
}

function step(label, fn) {
  console.log(`▸ ${label}`)
  fn()
}

async function stepAsync(label, fn) {
  console.log(`▸ ${label}`)
  await fn()
}

function fail(msg) {
  console.error(`\npack smoke: FAILED\n${msg}`)
  process.exit(1)
}
