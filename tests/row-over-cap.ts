import { encodeWhere } from "../src/server/subscriptions.ts"

// The SQLite-limit backstop for a sub's write to `_sync_subs` (ADR-0012
// amendment of 2026-11-17, "a refusal is always answered") is reached when the
// predicate's stored form fits SQLite's value cap but the row that holds it
// does not. A long sub id used to push the row over; since ids are limited to
// 256 bytes (amendment "ids and names have a length limit"), the predicate
// itself must sit just under the cap, so that even a short id tips the row over.

/** The cap on one value (and one row) in the workerd under test: see
 *  MAX_VALUE_BYTES in src/server/sql-compiler.ts. */
const CAP = 2_200_000

const fn = (name: string, ...args: Array<unknown>): unknown => ({ type: "func", name, args })
const inDates = (n: number): unknown =>
  fn("in", { type: "ref", path: ["id"] }, { type: "val", value: Array.from({ length: n }, (_, i) => new Date(1_700_000_000_000 + i)) })
const storedBytes = (where: unknown): number => new TextEncoder().encode(encodeWhere(where)!).length

/** An `in` list of Dates whose stored form is the longest under the cap: it
 *  passes the server's check of the stored form, and no row can hold it beside
 *  the other columns of `_sync_subs`. */
export function whereJustUnderStoredCap(): unknown {
  let lo = 0
  let hi = 60_000
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2)
    if (storedBytes(inDates(mid)) < CAP) lo = mid
    else hi = mid - 1
  }
  const where = inDates(lo)
  const bytes = storedBytes(where)
  if (bytes >= CAP || bytes < CAP - 100) throw new Error(`stored form is ${bytes} bytes, not just under ${CAP}`)
  return where
}
