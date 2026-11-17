#!/usr/bin/env node
// Print the @tanstack/db floor: the X.Y.Z of the declared `>=X.Y.Z` peer range.
//
// CI's floor leg and the pack smoke test install exactly this version, so the
// tested floor is derived from — and cannot drift from — the declared one
// (ADR-0022). Any other range shape fails loud rather than guessing.
//
// The range may carry one more clause that admits a prerelease line above the
// floor, e.g. `>=0.8.6 || >=1.0.0-rc.1` (ADR-0028). In semver `>=X.Y.Z` alone
// matches no prerelease, so without that clause npm refuses to install us next
// to a release candidate. The floor is still the first clause, so the second
// one must name a version above it.

import { readFileSync } from "node:fs"
import { pathToFileURL } from "node:url"

// A semver prerelease identifier: a number without leading zeros, or anything
// with a letter or a hyphen in it.
const PRERELEASE_ID = "(?:0|[1-9]\\d*|\\d*[A-Za-z-][0-9A-Za-z-]*)"
const RANGE = new RegExp(
  `^>=(\\d+\\.\\d+\\.\\d+)(?: \\|\\| >=(\\d+\\.\\d+\\.\\d+)-${PRERELEASE_ID}(?:\\.${PRERELEASE_ID})*)?$`,
)
const above = (a, b) => a.localeCompare(b, undefined, { numeric: true }) > 0

export function dbFloor() {
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"))
  const range = pkg.peerDependencies?.["@tanstack/db"]
  const m = RANGE.exec(range ?? "")
  if (!m || (m[2] && !above(m[2], m[1]))) {
    throw new Error(
      `peerDependencies["@tanstack/db"] is ${JSON.stringify(range)}; expected ">=X.Y.Z", optionally followed by " || >=A.B.C-prerelease" with A.B.C above X.Y.Z (ADR-0022, ADR-0028)`,
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
