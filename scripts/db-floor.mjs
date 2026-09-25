#!/usr/bin/env node
// Print the @tanstack/db floor: the X.Y.Z of the declared `>=X.Y.Z` peer range.
//
// CI's floor leg and the pack smoke test install exactly this version, so the
// tested floor is derived from — and cannot drift from — the declared one
// (ADR-0022). Any other range shape fails loud rather than guessing.

import { readFileSync } from "node:fs"
import { pathToFileURL } from "node:url"

export function dbFloor() {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"))
  const range = pkg.peerDependencies?.["@tanstack/db"]
  const m = /^>=(\d+\.\d+\.\d+)$/.exec(range ?? "")
  if (!m) {
    throw new Error(
      `peerDependencies["@tanstack/db"] is ${JSON.stringify(range)}; expected ">=X.Y.Z" (ADR-0022)`,
    )
  }
  return m[1]
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    console.log(dbFloor())
  } catch (e) {
    console.error(e.message)
    process.exit(1)
  }
}
