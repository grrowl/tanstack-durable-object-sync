#!/usr/bin/env node
// Boot smoke: run an example's worker in workerd and prove its modules
// evaluate and serve. A dry-run bundle never executes the code, so a crash at
// module evaluation (e.g. `crypto.getRandomValues()` at global scope — db 0.8.5
// in a Worker) only shows up here (ADR-0024).
//
//   node ../smoke-boot.mjs <port> "<server command>" <probe>...
//
// Probes, run in order once the server answers:
//   "GET /path"            expect 200
//   "GET /path >>marker"   expect 200 and the body to contain `marker`
//   "WS /path"             expect the WebSocket upgrade to open

import { spawn } from "node:child_process"

const [port, command, ...probes] = process.argv.slice(2)
if (!port || !command || probes.length === 0) {
  console.error('usage: smoke-boot.mjs <port> "<server command>" <probe>...')
  process.exit(2)
}
const origin = `http://127.0.0.1:${port}`
const BOOT_MS = 90_000
const PROBE_MS = 15_000

// Something already answering on the port would satisfy every probe.
const busy = await fetch(origin, { signal: AbortSignal.timeout(2_000) }).then(
  () => true,
  () => false,
)
if (busy) {
  console.error(`FAIL port ${port} is already in use — stop that server first`)
  process.exit(1)
}

let log = ""
const server = spawn(command, { shell: true, detached: true, env: { ...process.env, FORCE_COLOR: "0" } })
server.stdout.on("data", (d) => (log += d))
server.stderr.on("data", (d) => (log += d))
let exited = false
server.on("exit", () => (exited = true))

const stop = () => {
  try {
    process.kill(-server.pid, "SIGTERM") // the whole group: npx -> wrangler -> workerd
  } catch {}
}
process.on("exit", stop)
for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) process.on(sig, () => process.exit(1))
process.on("unhandledRejection", (err) => die(String(err)))

const die = (msg) => {
  console.error(`FAIL ${msg}\n--- server output ---\n${log}`)
  stop()
  process.exit(1)
}

async function waitForServer() {
  const deadline = Date.now() + BOOT_MS
  while (Date.now() < deadline) {
    if (exited) die(`server exited before answering on ${origin}`)
    try {
      await fetch(origin, { signal: AbortSignal.timeout(2_000) })
      return
    } catch {
      await new Promise((r) => setTimeout(r, 500))
    }
  }
  die(`server did not answer on ${origin} within ${BOOT_MS / 1000}s`)
}

async function probeHttp(path, marker) {
  const res = await fetch(origin + path, { signal: AbortSignal.timeout(PROBE_MS) })
  const body = await res.text()
  if (res.status !== 200) die(`GET ${path} -> ${res.status}\n${body.slice(0, 2_000)}`)
  if (marker && !body.includes(marker)) die(`GET ${path}: body lacks ${JSON.stringify(marker)}\n${body.slice(0, 2_000)}`)
}

function probeWs(path) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}${path}`)
    const timer = setTimeout(() => die(`WS ${path}: no open within ${PROBE_MS / 1000}s`), PROBE_MS)
    ws.onopen = () => {
      clearTimeout(timer)
      ws.close()
      resolve()
    }
    ws.onerror = () => die(`WS ${path}: upgrade failed`)
  })
}

await waitForServer()
for (const probe of probes) {
  const [, kind, path, marker] = probe.match(/^(GET|WS) (\S+)(?: >>(.+))?$/) ?? die(`bad probe: ${probe}`)
  try {
    if (kind === "GET") await probeHttp(path, marker)
    else await probeWs(path)
  } catch (err) {
    die(`${probe}: ${err}`)
  }
  console.log(`ok   ${probe}`)
}
stop()
process.exit(0)
